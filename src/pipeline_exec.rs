use crate::atomic_write::{
    ensure_staging_dir, sweep_stale_inprogress, write_jsonl_atomic, write_zst_atomic,
};
use crate::config::ETLOptions;
use crate::date::YearMonth;
use crate::dedupe::{
    build_runs_sorted_with_key_stats, merge_runs_sorted_with_key_stats, DedupeCfg,
};
use crate::filters::{
    bounds_tuple, matches_full, matches_minimal, resolve_target_subs_from, within_bounds,
    ym_from_epoch, DateBounds,
};
use crate::key_extractor::KeyExtractor;
use crate::kv_shard::ShardedKVWriter;
use crate::paths::{
    discover_sources_checked, log_missing_month_warnings, plan_files_checked, FileJob, FileKind,
};
use crate::pipeline::{RedditETL, ScanPlan};
use crate::progress::{make_progress_bar_labeled, total_compressed_size};
use crate::progress_manifest::{ManifestAccumulator, MonthEntry};
use crate::query::QuerySpec;
use crate::shard::{ShardedWriter, UsernameStream};
use crate::stitch::{concat_tsvs, stitch_tmp_parts, stitch_tmp_parts_to_json_array};
use crate::streaming::{
    claim_record_or_stop, is_record_limit_reached, process_file_for_usernames_with_skip,
    stream_job_with_partial_policy, RecordLimit, StreamJobResult, WhitelistMatchTracker,
};
use crate::util::{
    create_dir_all_with_backoff, create_with_backoff, open_with_backoff, read_dir_with_backoff,
    remove_dir_all_with_backoff, remove_with_backoff, with_thread_pool,
};
use crate::zstd_jsonl::{
    for_each_line_with_opts_status, malformed_json_error, parse_minimal, LineStreamOpts,
    MinimalRecord, PartialReadPolicy,
};
use anyhow::{anyhow, Context, Result};
use indicatif::ProgressBar;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::fs;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Export format for partitioned corpus-style outputs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExportFormat {
    Jsonl,
    Zst,
}

/// Options for [`ScanPlan::extract_to_csv`] and [`ScanPlan::extract_to_tsv`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TabularExportOptions {
    /// Emit a header row containing the requested field names.
    pub header: bool,
}

impl Default for TabularExportOptions {
    fn default() -> Self {
        Self { header: true }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TabularFormat {
    Csv,
    Tsv,
}

impl TabularFormat {
    fn label(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Tsv => "tsv",
        }
    }

    fn tmp_dir_name(self) -> &'static str {
        match self {
            Self::Csv => "extract_csv_q_tmp",
            Self::Tsv => "extract_tsv_q_tmp",
        }
    }

    fn row_suffix(self) -> &'static str {
        match self {
            Self::Csv => ".csvpart",
            Self::Tsv => ".tsvpart",
        }
    }
}

/// Summary returned by [`ScanPlan::dedupe_keys_to_lines_with_stats`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DedupeKeySummary {
    /// Records that matched the scan query before dedupe key extraction.
    pub matched_records: u64,
    /// Distinct keys written to the output.
    pub unique_keys: u64,
    /// Matched records for which [`KeyExtractor::key_from_line`] returned
    /// `Ok(None)` (missing, null, or otherwise non-extractable key). These
    /// records are omitted from the dedupe output.
    pub key_extractions_failed: u64,
}

impl DedupeKeySummary {
    /// Fraction of matched records omitted because no dedupe key was found.
    pub fn key_drop_rate(&self) -> f64 {
        if self.matched_records == 0 {
            0.0
        } else {
            self.key_extractions_failed as f64 / self.matched_records as f64
        }
    }
}

/// Finalization choice for extract operations (internal).
enum Finalize {
    Jsonl,
    JsonArray { pretty: bool },
}

const FILE_PREFIX_RC: &str = "part_RC";
const FILE_PREFIX_RS: &str = "part_RS";
const DEDUPE_KEY_DROP_WARN_RATE: f64 = 0.01;
static EXTRACT_SCRATCH_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct PartialScanError {
    path: PathBuf,
    written: u64,
}

impl fmt::Display for PartialScanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "zstd decode error while streaming {}; {} record(s) were decoded before the file was skipped",
            self.path.display(),
            self.written
        )
    }
}

impl std::error::Error for PartialScanError {}

fn complete_stream_job(job: &FileJob, result: StreamJobResult) -> Result<u64> {
    if result.complete {
        Ok(result.written)
    } else {
        Err(PartialScanError {
            path: job.path.clone(),
            written: result.written,
        }
        .into())
    }
}

fn record_limit_from(limit: Option<u64>) -> Option<Arc<RecordLimit>> {
    limit.map(|n| Arc::new(RecordLimit::new(n)))
}

fn record_limit_from_with_claimed(limit: Option<u64>, claimed: u64) -> Option<Arc<RecordLimit>> {
    limit.map(|n| Arc::new(RecordLimit::new_with_claimed(n, claimed)))
}

fn committed_line_count(months: &HashMap<String, MonthEntry>) -> u64 {
    months.values().map(|entry| entry.lines).sum()
}

fn is_partial_scan_error(e: &anyhow::Error) -> bool {
    e.chain()
        .any(|cause| cause.downcast_ref::<PartialScanError>().is_some())
}

fn cleanup_scratch_dir(path: &Path, label: &str) {
    if let Err(e) = remove_dir_all_with_backoff(path, 8, 50) {
        tracing::warn!(path=%path.display(), error=%e, %label, "failed to remove scratch dir");
    }
}

fn dedupe_key_label(key: &KeyExtractor) -> String {
    match key {
        KeyExtractor::JsonPointer(ptr) => format!("json:{ptr}"),
        KeyExtractor::AuthorLowerFast => "author".to_string(),
        KeyExtractor::SubredditLowerFast => "subreddit".to_string(),
        KeyExtractor::ByValue(_) => "<custom>".to_string(),
    }
}

fn warn_dedupe_key_drops(key: &KeyExtractor, summary: &DedupeKeySummary) {
    if summary.matched_records == 0 || summary.key_extractions_failed == 0 {
        return;
    }
    let drop_rate = summary.key_drop_rate();
    if drop_rate > DEDUPE_KEY_DROP_WARN_RATE {
        let key_spec = dedupe_key_label(key);
        tracing::warn!(
            key = %key_spec,
            matched_records = summary.matched_records,
            key_extractions_failed = summary.key_extractions_failed,
            drop_rate,
            "dedupe dropped {} matching record(s) without an extractable key ({:.2}% of matches); use --strict-key to fail instead",
            summary.key_extractions_failed,
            drop_rate * 100.0,
        );
    }
}

fn dedupe_cfg_from_options(opts: &ETLOptions) -> DedupeCfg {
    crate::config::warn_if_inflight_pair_pathological(opts.inflight_bytes, opts.inflight_groups);
    let mut cfg = DedupeCfg::from(opts);
    if cfg.inflight_bytes > 0 {
        let per_flush_mb = (cfg.inflight_bytes / 2 / (1024 * 1024)).max(1);
        cfg.min_buf_mb = cfg.min_buf_mb.min(per_flush_mb);
        cfg.max_buf_mb = cfg.max_buf_mb.min(per_flush_mb.max(cfg.min_buf_mb));
    }
    cfg
}

fn warn_dedupe_zero_keys(key: &KeyExtractor, input_records: u64) {
    match key {
        KeyExtractor::JsonPointer(ptr) => {
            let key_spec = format!("json:{ptr}");
            tracing::warn!(
                key = %key_spec,
                input_records,
                "--key {key_spec} matched nothing; check the pointer; the value may not be a scalar"
            );
        }
        KeyExtractor::AuthorLowerFast => tracing::warn!(
            key = "author",
            input_records,
            "--key author extracted zero keys from matching input records"
        ),
        KeyExtractor::SubredditLowerFast => tracing::warn!(
            key = "subreddit",
            input_records,
            "--key subreddit extracted zero keys from matching input records"
        ),
        KeyExtractor::ByValue(_) => tracing::warn!(
            key = "<custom>",
            input_records,
            "custom dedupe key extractor extracted zero keys from matching input records"
        ),
    }
}

// -------- Original operations (back-compat) --------

impl RedditETL {
    #[deprecated(note = "use RedditETL::scan().usernames()")]
    pub fn usernames(self) -> Result<UsernameStream> {
        let subreddit = self
            .opts
            .subreddit
            .clone()
            .ok_or_else(|| anyhow!("subreddit is required"))?;
        let parallelism = self.opts.parallelism;

        with_thread_pool(parallelism, || {
            let work_dir = self.ensure_work_dir()?;
            let files = plan_pipeline_files(&self)?;
            tracing::info!("Planned {} files for processing.", files.len());

            let shard_writer =
                ShardedWriter::create(&work_dir, "usernames", self.opts.shard_count)?;
            let scratch_root = shard_writer.scratch_root().to_path_buf();
            let result = (|| -> Result<UsernameStream> {
                let read_buf = self.opts.read_buffer_bytes;

                let total_bytes = total_compressed_size(&files);
                let pb = if self.opts.progress {
                    Some(make_progress_bar_labeled(
                        total_bytes,
                        self.opts.progress_label.as_deref(),
                    ))
                } else {
                    None
                };

                // Per-run counter of files skipped by the decoder. Surfaced to
                // observers via tracing at the end of the run. Per-file detail is
                // already logged by `warn_decode_skip`; this aggregate makes the
                // total visible without parsing log output.
                let skip_count = std::sync::Arc::new(AtomicU64::new(0));
                let skip_count_inner = skip_count.clone();

                crate::concurrency::for_each_file_limited(
                    &files,
                    self.opts.file_concurrency,
                    |job| {
                        let skip_count_per_call = skip_count_inner.clone();
                        process_file_for_usernames_with_skip(
                            job,
                            read_buf,
                            &subreddit,
                            &shard_writer,
                            pb.clone(),
                            self.opts.allow_partial,
                            Some(&self.opts.partial_read_reporter),
                            move |_path, _err| {
                                skip_count_per_call.fetch_add(1, Ordering::Relaxed);
                            },
                        )
                        .with_context(|| format!("processing {}", job.path.display()))
                    },
                )?;

                let skipped = skip_count.load(Ordering::Relaxed);
                if skipped > 0 {
                    tracing::warn!(
                        skipped_files = skipped,
                        "usernames: {} input file(s) skipped due to zstd decode errors; see prior warnings for paths",
                        skipped
                    );
                }

                let (deduped_files, scratch_root) = shard_writer.dedup_with_scratch("usernames")?;
                UsernameStream::from_deduped_files_with_cleanup(deduped_files, vec![scratch_root])
            })();
            if result.is_err() {
                cleanup_scratch_dir(&scratch_root, "usernames");
            }
            result
        })
    }
}

// -------- ScanPlan execution methods --------

impl ScanPlan {
    pub fn usernames(self) -> Result<UsernameStream> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        let parallelism = plan.etl.opts.parallelism;
        with_thread_pool(parallelism, || {
            let work_dir = plan.etl.ensure_work_dir()?;
            let shard_writer =
                ShardedWriter::create(&work_dir, "usernames_q", plan.etl.opts.shard_count)?;
            let scratch_root = shard_writer.scratch_root().to_path_buf();

            let result = (|| -> Result<UsernameStream> {
                scan_records(
                    &plan.etl,
                    &plan.query,
                    /*show_progress=*/ true,
                    plan.limit,
                    |min, _kind, _line| {
                        if let Some(a) = min.author.as_deref() {
                            let a = a.trim();
                            if a.is_empty() {
                                return Ok(());
                            }
                            shard_writer.write(a)?;
                        }
                        Ok(())
                    },
                )?;

                let (deduped, scratch_root) = shard_writer.dedup_with_scratch("usernames_q")?;
                UsernameStream::from_deduped_files_with_cleanup(deduped, vec![scratch_root])
            })();
            if result.is_err() {
                cleanup_scratch_dir(&scratch_root, "usernames_q");
            }
            result
        })
    }

    /// JS-like lambda: stream **deduped** usernames and invoke a callback for each one.
    /// Example:
    ///   etl.scan().subreddit("programming").for_each_username(|name| authors.push(name.to_string()))?;
    pub fn for_each_username<F>(self, mut f: F) -> Result<()>
    where
        F: FnMut(&str),
    {
        let mut it = self.usernames()?;
        while let Some(u) = it.next() {
            f(&u);
        }
        Ok(())
    }

    /// Fallible variant of [`ScanPlan::for_each_username`]. Callback errors and
    /// stream open/read errors are returned to the caller instead of being
    /// ignored or logged-and-skipped.
    pub fn try_for_each_username<F>(self, mut f: F) -> Result<()>
    where
        F: FnMut(&str) -> Result<()>,
    {
        let mut it = self.usernames()?;
        while let Some(u) = it.try_next() {
            let u = u?;
            f(&u)?;
        }
        Ok(())
    }

    pub fn extract_to_jsonl(self, out_path: &Path) -> Result<()> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        let targets = resolve_target_subs_from(&plan.etl.opts.subreddit, &plan.query.subreddits);
        extract_common(
            &plan.etl,
            &plan.query,
            targets.as_ref(),
            "extract_jsonl_q_tmp",
            out_path,
            Finalize::Jsonl,
            plan.limit,
        )
    }

    /// Write the distinct keys from matching records as one key per line.
    ///
    /// The scan/filter pass first spools matching raw JSONL records into the
    /// configured work directory, then reuses the public sorted-run dedupe
    /// engine (`build_runs_sorted` + `merge_runs_sorted`) with the supplied
    /// [`KeyExtractor`]. Built-in `author` and `subreddit` extractors keep the
    /// `MinimalRecord` fast path; `json:/pointer` extractors parse full JSON
    /// only for key extraction. Matching records whose key extractor returns
    /// `Ok(None)` are omitted from the output; use
    /// [`ScanPlan::dedupe_keys_to_lines_with_stats`] to inspect that count.
    pub fn dedupe_keys_to_lines(self, key: &KeyExtractor, out_path: &Path) -> Result<u64> {
        Ok(self
            .dedupe_keys_to_lines_with_stats(key, out_path)?
            .unique_keys)
    }

    /// Like [`ScanPlan::dedupe_keys_to_lines`], but returns matched-record,
    /// unique-key, and missing-key counts for diagnostics.
    pub fn dedupe_keys_to_lines_with_stats(
        self,
        key: &KeyExtractor,
        out_path: &Path,
    ) -> Result<DedupeKeySummary> {
        let plan = self.build()?;
        let parallelism = plan.etl.opts.parallelism;
        with_thread_pool(parallelism, || {
            let work_dir = plan.etl.ensure_work_dir()?;
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let tmp_dir = work_dir.join(format!("dedupe_{}_{}", std::process::id(), unique));
            create_dir_all_with_backoff(&tmp_dir, 16, 50)
                .with_context(|| format!("creating dedupe work dir {}", tmp_dir.display()))?;

            let result = (|| -> Result<DedupeKeySummary> {
                let input = tmp_dir.join("matched.ndjson");
                let f = create_with_backoff(&input, 16, 50)
                    .with_context(|| format!("create {}", input.display()))?;
                let writer = Mutex::new(BufWriter::with_capacity(
                    plan.etl.opts.write_buffer_bytes,
                    f,
                ));
                let matched_records = AtomicU64::new(0);

                scan_records(
                    &plan.etl,
                    &plan.query,
                    /*show_progress=*/ true,
                    plan.limit,
                    |_min, _kind, line| {
                        matched_records.fetch_add(1, Ordering::Relaxed);
                        let mut w = writer
                            .lock()
                            .map_err(|_| anyhow!("dedupe writer lock poisoned"))?;
                        w.write_all(line.as_bytes())?;
                        w.write_all(b"\n")?;
                        Ok(())
                    },
                )?;
                writer
                    .into_inner()
                    .map_err(|_| anyhow!("dedupe writer lock poisoned"))?
                    .flush()?;

                let key_extractions_failed = AtomicU64::new(0);
                let cfg = dedupe_cfg_from_options(&plan.etl.opts);

                let runs_dir = tmp_dir.join("runs");
                let runs = build_runs_sorted_with_key_stats(
                    &input,
                    &runs_dir,
                    key,
                    &cfg,
                    Some(&key_extractions_failed),
                )?;
                let matched_total = matched_records.load(Ordering::Relaxed);
                let failed_after_build = key_extractions_failed.load(Ordering::Relaxed);
                if plan.etl.opts.strict_key && failed_after_build > 0 {
                    let summary = DedupeKeySummary {
                        matched_records: matched_total,
                        unique_keys: 0,
                        key_extractions_failed: failed_after_build,
                    };
                    if matched_total > 0 && runs.is_empty() {
                        warn_dedupe_zero_keys(key, matched_total);
                    }
                    warn_dedupe_key_drops(key, &summary);
                    anyhow::bail!(
                        "--strict-key: {} of {} matching record(s) did not contain extractable key {}; aborting dedupe output",
                        failed_after_build,
                        matched_total,
                        dedupe_key_label(key),
                    );
                }

                let mut unique_count = 0_u64;
                merge_runs_sorted_with_key_stats(
                    &runs,
                    out_path,
                    key,
                    &cfg,
                    |current_key, _group, w| {
                        w.write_all(current_key.as_bytes())?;
                        w.write_all(b"\n")?;
                        unique_count += 1;
                        Ok(())
                    },
                    Some(&key_extractions_failed),
                )?;

                let key_extractions_failed = key_extractions_failed.load(Ordering::Relaxed);
                let summary = DedupeKeySummary {
                    matched_records: matched_total,
                    unique_keys: unique_count,
                    key_extractions_failed,
                };
                if matched_total > 0 && unique_count == 0 {
                    warn_dedupe_zero_keys(key, matched_total);
                }
                warn_dedupe_key_drops(key, &summary);

                Ok(summary)
            })();

            let _ = remove_dir_all_with_backoff(&tmp_dir, 8, 50);
            result
        })
    }

    pub fn extract_to_json(self, out_path: &Path, pretty: bool) -> Result<()> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        let targets = resolve_target_subs_from(&plan.etl.opts.subreddit, &plan.query.subreddits);
        extract_common(
            &plan.etl,
            &plan.query,
            targets.as_ref(),
            "extract_json_q_tmp",
            out_path,
            Finalize::JsonArray { pretty },
            plan.limit,
        )
    }

    /// Export matching records as CSV. `fields` is the fixed top-level schema;
    /// missing fields render as empty cells.
    pub fn extract_to_csv<I, S>(
        self,
        out_path: &Path,
        fields: I,
        opts: TabularExportOptions,
    ) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.extract_to_tabular(out_path, fields, opts, TabularFormat::Csv)
    }

    /// Export matching records as TSV. `fields` is the fixed top-level schema;
    /// missing fields render as empty cells. Values containing a literal tab
    /// are rejected because TSV has no standard escaping convention.
    pub fn extract_to_tsv<I, S>(
        self,
        out_path: &Path,
        fields: I,
        opts: TabularExportOptions,
    ) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.extract_to_tabular(out_path, fields, opts, TabularFormat::Tsv)
    }

    fn extract_to_tabular<I, S>(
        self,
        out_path: &Path,
        fields: I,
        opts: TabularExportOptions,
        format: TabularFormat,
    ) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let fields = normalize_tabular_fields(fields)?;
        let mut scan = self;
        scan.etl.opts.whitelist_fields = Some(fields.clone());
        let plan = scan.build()?;
        log_pseudo_user_filter(&plan.query);
        let targets = resolve_target_subs_from(&plan.etl.opts.subreddit, &plan.query.subreddits);
        extract_tabular_common(
            &plan.etl,
            &plan.query,
            targets.as_ref(),
            out_path,
            &fields,
            opts,
            format,
            plan.limit,
        )
    }

    /// Spool per-month outputs for later parent attachment + aggregation.
    /// Writes **separate** files per source to avoid clobbering:
    ///   - comments → `part_RC_YYYY-MM.jsonl`
    ///   - submissions → `part_RS_YYYY-MM.jsonl`
    ///
    /// Each month is staged as a unique `<out_dir>/_staging/<file>.*.inprogress`
    /// then atomically renamed onto the final path so a crashed run never
    /// publishes a partial file. On entry, leftover `*.inprogress` files from
    /// a prior crashed run are swept only when their owner PID is no longer live.
    ///
    /// Returns `(vector_of_paths, total_records_written)`.
    pub fn extract_spool_monthly(self, out_dir: &Path) -> Result<(Vec<PathBuf>, u64)> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        validate_export_whitelist(&plan.etl)?;
        let parallelism = plan.etl.opts.parallelism;

        with_thread_pool(parallelism, || {
            create_dir_all_with_backoff(out_dir, 16, 50)
                .with_context(|| format!("creating spool dir {}", out_dir.display()))?;
            let staging_dir = ensure_staging_dir(out_dir)?;
            sweep_stale_inprogress(out_dir, true)?;

            let targets =
                resolve_target_subs_from(&plan.etl.opts.subreddit, &plan.query.subreddits);
            let files = plan_pipeline_files(&plan.etl)?;
            warn_if_unfiltered_undated_query(&plan.etl, &plan.query, &files);

            let resume = plan.etl.opts.resume;
            let resume_fingerprint =
                build_resume_fingerprint(&plan.etl, &plan.query, "spool", plan.limit)?;
            // Resume manifest: load on entry, validate each entry against the
            // current query/config fingerprint and the file actually on disk,
            // then snapshot surviving keys before any worker mutates the accumulator.
            let (initial_months, completed_keys) = if resume {
                load_and_validate_manifest(out_dir, &resume_fingerprint)?
            } else {
                (HashMap::new(), HashSet::new())
            };

            let total_bytes = total_compressed_size(&files);
            let pb = if plan.etl.opts.progress {
                Some(make_progress_bar_labeled(
                    total_bytes,
                    plan.etl.opts.progress_label.as_deref(),
                ))
            } else {
                None
            };

            let total_written = AtomicU64::new(0);
            let parts = Mutex::new(Vec::<PathBuf>::new());

            // Pre-seed `parts` with already-completed months so the returned
            // list reflects the full set of published outputs (resumed + new).
            if resume {
                let mut guard = parts.lock().unwrap();
                for key in &completed_keys {
                    guard.push(out_dir.join(format!("part_{}.jsonl", key)));
                }
            }

            let resumed_lines = committed_line_count(&initial_months);

            // Persist the (pruned) manifest before any work, so a crash mid-
            // prune cannot resurrect entries we already know are stale.
            let accumulator = if resume {
                crate::progress_manifest::save(
                    out_dir,
                    &initial_months,
                    Some(&resume_fingerprint),
                )?;
                Some(ManifestAccumulator::new(
                    out_dir,
                    initial_months,
                    Some(resume_fingerprint.clone()),
                ))
            } else {
                None
            };

            let whitelist = plan.etl.opts.whitelist_fields.clone();
            let whitelist_tracker = whitelist
                .as_ref()
                .map(|_| Arc::new(WhitelistMatchTracker::new(plan.etl.opts.strict_whitelist)));
            let record_limit = record_limit_from_with_claimed(plan.limit, resumed_lines);
            let targets_ref = targets.as_ref();
            let bounds = bounds_tuple(plan.etl.opts.start, plan.etl.opts.end);
            let read_buf = plan.etl.opts.read_buffer_bytes;
            let write_buf = plan.etl.opts.write_buffer_bytes;
            let human_ts = plan.etl.opts.human_readable_timestamps;

            crate::concurrency::for_each_file_limited(
                &files,
                plan.etl.opts.file_concurrency,
                |job| -> Result<()> {
                    let ctx = MonthJobCtx {
                        out_dir,
                        staging_dir: &staging_dir,
                        targets: targets_ref,
                        query: &plan.query,
                        whitelist: &whitelist,
                        pb: pb.as_ref(),
                        bounds,
                        read_buf,
                        write_buf,
                        human_ts,
                        whitelist_tracker: whitelist_tracker.as_deref(),
                        record_limit: record_limit.as_deref(),
                        resume,
                        completed_keys: &completed_keys,
                        allow_partial: plan.etl.opts.allow_partial,
                        partial_reporter: Some(&plan.etl.opts.partial_read_reporter),
                    };
                    let outcome = process_month(job, &ctx)?;

                    if let Some(month) = outcome {
                        total_written.fetch_add(month.lines, Ordering::Relaxed);
                        parts.lock().unwrap().push(month.out_path.clone());
                        if let Some(acc) = &accumulator {
                            commit_entry_to_manifest(acc, month).context(
                                "failed to durably update resume progress manifest after publishing spool output",
                            )?;
                        }
                    }
                    Ok(())
                },
            )?;

            if let Some(pb) = pb {
                pb.finish_with_message("done");
            }
            ensure_resume_manifest_durable(accumulator.as_ref(), "spool")?;

            let mut list = parts.into_inner().unwrap();
            list.sort();
            list.dedup();
            Ok((list, total_written.load(Ordering::Relaxed)))
        })
    }

    pub fn count_by_month(self) -> Result<BTreeMap<YearMonth, u64>> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        let parallelism = plan.etl.opts.parallelism;
        with_thread_pool(parallelism, || {
            let total = Mutex::new(BTreeMap::<YearMonth, u64>::new());
            scan_records(
                &plan.etl,
                &plan.query,
                /*show_progress=*/ false,
                plan.limit,
                |min, _kind, _line| {
                    if let Some(ts) = min.created_utc {
                        let ym = ym_from_epoch(ts);
                        *total.lock().unwrap().entry(ym).or_insert(0) += 1;
                    }
                    Ok(())
                },
            )?;
            Ok(total.into_inner().unwrap())
        })
    }

    pub fn author_counts_to_tsv(self, out_path: &Path) -> Result<()> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        let parallelism = plan.etl.opts.parallelism;
        with_thread_pool(parallelism, || {
            let work_dir = plan.etl.ensure_work_dir()?;
            let kv =
                ShardedKVWriter::create(&work_dir, "author_counts", plan.etl.opts.shard_count)?;
            let scratch_root = kv.scratch_root().to_path_buf();

            let result = (|| -> Result<()> {
                scan_records(
                    &plan.etl,
                    &plan.query,
                    /*show_progress=*/ false,
                    plan.limit,
                    |min, _kind, _line| {
                        if let Some(a) = min.author.as_deref() {
                            let a = a.trim();
                            if a.is_empty() {
                                return Ok(());
                            }
                            kv.write_kv(a, 1)?;
                        }
                        Ok(())
                    },
                )?;

                let (shards, _scratch_root) = kv.reduce_sum_with_scratch("author_counts")?;
                concat_tsvs(&shards, out_path, plan.etl.opts.write_buffer_bytes)?;
                Ok(())
            })();
            cleanup_scratch_dir(&scratch_root, "author_counts");
            result
        })
    }

    pub fn build_first_seen_index_to_tsv(self, out_path: &Path) -> Result<()> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        let parallelism = plan.etl.opts.parallelism;
        with_thread_pool(parallelism, || {
            let work_dir = plan.etl.ensure_work_dir()?;
            let kv = ShardedKVWriter::create(&work_dir, "first_seen", plan.etl.opts.shard_count)?;
            let scratch_root = kv.scratch_root().to_path_buf();

            let result = (|| -> Result<()> {
                scan_records(
                    &plan.etl,
                    &plan.query,
                    /*show_progress=*/ false,
                    plan.limit,
                    |min, _kind, _line| {
                        if let (Some(a), Some(ts)) = (min.author.as_deref(), min.created_utc) {
                            let a = a.trim();
                            if a.is_empty() {
                                return Ok(());
                            }
                            kv.write_kv(a, ts)?;
                        }
                        Ok(())
                    },
                )?;

                let (shards, _scratch_root) = kv.reduce_min_with_scratch("first_seen")?;
                concat_tsvs(&shards, out_path, plan.etl.opts.write_buffer_bytes)?;
                Ok(())
            })();
            cleanup_scratch_dir(&scratch_root, "first_seen");
            result
        })
    }

    /// Export corpus back to partitioned JSONL or ZST by month/kinds with query filters.
    /// This lives on ScanPlan (advanced query mode).
    ///
    /// Each output is staged as a unique
    /// `<out_base_dir>/_staging/<file>.*.inprogress`, finalized (zstd frame
    /// closed with checksum, or buffer flushed) and atomically renamed onto its
    /// final path. Stale `*.inprogress` from a crashed prior run are swept on
    /// entry only when their owner PID is no longer live.
    pub fn export_partitioned(self, out_base_dir: &Path, format: ExportFormat) -> Result<()> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        validate_export_whitelist(&plan.etl)?;
        let targets = resolve_target_subs_from(&plan.etl.opts.subreddit, &plan.query.subreddits);
        let parallelism = plan.etl.opts.parallelism;

        with_thread_pool(parallelism, || {
            let files = plan_pipeline_files(&plan.etl)?;
            warn_if_unfiltered_undated_query(&plan.etl, &plan.query, &files);

            let comments_dir = out_base_dir.join("comments");
            let submissions_dir = out_base_dir.join("submissions");
            create_dir_all_with_backoff(&comments_dir, 16, 50).with_context(|| {
                format!("creating comments export dir {}", comments_dir.display())
            })?;
            create_dir_all_with_backoff(&submissions_dir, 16, 50).with_context(|| {
                format!(
                    "creating submissions export dir {}",
                    submissions_dir.display()
                )
            })?;
            let staging_dir = ensure_staging_dir(out_base_dir)?;
            sweep_stale_inprogress(out_base_dir, true)?;

            let resume = plan.etl.opts.resume;
            let resume_fingerprint = build_resume_fingerprint(
                &plan.etl,
                &plan.query,
                partitioned_resume_operation(format),
                plan.limit,
            )?;
            let (initial_months, completed_keys) = if resume {
                load_and_validate_partitioned_manifest(out_base_dir, &resume_fingerprint, format)?
            } else {
                (HashMap::new(), HashSet::new())
            };
            let resumed_lines = committed_line_count(&initial_months);
            let accumulator = if resume {
                crate::progress_manifest::save(
                    out_base_dir,
                    &initial_months,
                    Some(&resume_fingerprint),
                )?;
                Some(ManifestAccumulator::new(
                    out_base_dir,
                    initial_months,
                    Some(resume_fingerprint.clone()),
                ))
            } else {
                None
            };

            let whitelist = plan.etl.opts.whitelist_fields.clone();
            let whitelist_tracker = whitelist
                .as_ref()
                .map(|_| Arc::new(WhitelistMatchTracker::new(plan.etl.opts.strict_whitelist)));
            let record_limit = record_limit_from_with_claimed(plan.limit, resumed_lines);
            let targets_ref = targets.as_ref();
            let bounds = bounds_tuple(plan.etl.opts.start, plan.etl.opts.end);
            let total_bytes = total_compressed_size(&files);
            let pb = if plan.etl.opts.progress {
                Some(make_progress_bar_labeled(
                    total_bytes,
                    plan.etl.opts.progress_label.as_deref(),
                ))
            } else {
                None
            };
            let read_buf = plan.etl.opts.read_buffer_bytes;
            let write_buf = plan.etl.opts.write_buffer_bytes;
            let human_ts = plan.etl.opts.human_readable_timestamps;
            let zst_level = plan.etl.opts.zst_level;

            crate::concurrency::for_each_file_limited(
                &files,
                plan.etl.opts.file_concurrency,
                |job| -> Result<()> {
                    let key = export_part_key(job);
                    let out_path = partitioned_output_path(out_base_dir, job, format);

                    if record_limit
                        .as_ref()
                        .is_some_and(|limit| limit.is_exhausted())
                    {
                        return Ok(());
                    }

                    if resume && completed_keys.contains(&key) {
                        if let Some(pb) = &pb {
                            let sz = fs::metadata(&job.path).map(|m| m.len()).unwrap_or(0);
                            pb.inc(sz);
                        }
                        return Ok(());
                    }

                    let written_result = match format {
                        ExportFormat::Jsonl => {
                            write_jsonl_atomic(&staging_dir, &out_path, write_buf, |w| {
                                let result = stream_job_with_partial_policy(
                                    job,
                                    w,
                                    targets_ref,
                                    &plan.query,
                                    &whitelist,
                                    pb.clone(),
                                    bounds,
                                    read_buf,
                                    human_ts,
                                    whitelist_tracker.as_deref(),
                                    plan.etl.opts.allow_partial,
                                    Some(&plan.etl.opts.partial_read_reporter),
                                    record_limit.as_deref(),
                                )?;
                                complete_stream_job(job, result)
                            })
                        }
                        ExportFormat::Zst => {
                            write_zst_atomic(&staging_dir, &out_path, zst_level, write_buf, |w| {
                                let result = stream_job_with_partial_policy(
                                    job,
                                    w,
                                    targets_ref,
                                    &plan.query,
                                    &whitelist,
                                    pb.clone(),
                                    bounds,
                                    read_buf,
                                    human_ts,
                                    whitelist_tracker.as_deref(),
                                    plan.etl.opts.allow_partial,
                                    Some(&plan.etl.opts.partial_read_reporter),
                                    record_limit.as_deref(),
                                )?;
                                complete_stream_job(job, result)
                            })
                        }
                    };

                    let written = match written_result {
                        Ok(n) => n,
                        Err(e) if plan.etl.opts.allow_partial && is_partial_scan_error(&e) => {
                            tracing::warn!(path=%job.path.display(), error=%e, "Skipping partitioned export month after zstd decode error; staged output was discarded");
                            return Ok(());
                        }
                        Err(e) => return Err(e),
                    };

                    if written == 0 {
                        let _ = remove_with_backoff(&out_path, 8, 50);
                    }
                    if let Some(acc) = &accumulator {
                        let size = if written == 0 {
                            0
                        } else {
                            fs::metadata(&out_path).map(|m| m.len()).unwrap_or(0)
                        };
                        acc.commit(
                            key,
                            MonthEntry {
                                size,
                                lines: written,
                                sha256: None,
                            },
                        )
                        .context(
                            "failed to durably update partitioned export progress manifest after publishing partition",
                        )?;
                    }
                    Ok(())
                },
            )?;

            if let Some(pb) = pb {
                pb.finish_with_message("done");
            }
            ensure_resume_manifest_durable(accumulator.as_ref(), "partitioned export")?;
            Ok(())
        })
    }
}

// ----------------- extract_spool_monthly helpers -----------------

fn log_pseudo_user_filter(query: &QuerySpec) {
    if query.filter_pseudo_users {
        tracing::info!(
            "Excluding pseudo-users ([deleted], [removed], empty). Pass --include-deleted to keep them."
        );
    }
}

/// Result of a single month's atomic publish — used by the orchestration
/// shell in `extract_spool_monthly` to update both the returned `parts` list
/// and the on-disk progress manifest.
struct MonthResult {
    out_path: PathBuf,
    key: String,
    lines: u64,
}

fn plan_pipeline_files(etl: &RedditETL) -> Result<Vec<FileJob>> {
    if let Some(err) = etl.opts.build_error.clone() {
        return Err(err.into());
    }
    let discovered = discover_sources_checked(
        &etl.opts.comments_dir,
        &etl.opts.submissions_dir,
        etl.opts.sources,
    )?;
    let jobs = plan_files_checked(
        &discovered,
        &etl.opts.comments_dir,
        &etl.opts.submissions_dir,
        etl.opts.sources,
        etl.opts.start,
        etl.opts.end,
    )?;
    log_missing_month_warnings(&discovered, etl.opts.sources, etl.opts.start, etl.opts.end);
    Ok(jobs)
}

fn warn_if_unfiltered_undated_query(etl: &RedditETL, query: &QuerySpec, files: &[FileJob]) {
    if etl.opts.start.is_some()
        || etl.opts.end.is_some()
        || etl.opts.subreddit.is_some()
        || query.has_selective_filters()
    {
        return;
    }

    let file_count = files.len();
    let compressed_bytes = total_compressed_size(files);
    tracing::warn!(
        files = file_count,
        compressed_bytes = compressed_bytes,
        "running an unfiltered, undated query over the full corpus (files={file_count}, compressed_bytes={compressed_bytes}); pass --subreddit and/or --start/--end to narrow the scope"
    );
}

fn validate_export_whitelist(etl: &RedditETL) -> Result<()> {
    if matches!(&etl.opts.whitelist_fields, Some(fields) if fields.is_empty()) {
        anyhow::bail!("--whitelist must include at least one non-empty field");
    }
    Ok(())
}

fn opt_ym_string(ym: Option<YearMonth>) -> Option<String> {
    ym.map(|ym| ym.to_string())
}

fn stable_fnv1a_hex(bytes: &[u8]) -> String {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001B3;
    let mut hash = FNV_OFFSET;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("fnv1a64:{hash:016x}")
}

fn absolutize_for_fingerprint(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()
            .context("resolve current directory for path fingerprint")?
            .join(path))
    }
}

fn canonicalize_or_absolutize(path: &Path) -> Result<PathBuf> {
    match fs::canonicalize(path) {
        Ok(path) => Ok(path),
        Err(_) => absolutize_for_fingerprint(path),
    }
}

fn path_for_fingerprint(path: &Path) -> Result<String> {
    let path = if path.is_dir() {
        canonicalize_or_absolutize(path)?
    } else if let Some(file_name) = path.file_name() {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        canonicalize_or_absolutize(parent)?.join(file_name)
    } else {
        canonicalize_or_absolutize(path)?
    };
    Ok(path.to_string_lossy().into_owned())
}

fn stable_hash_component(bytes: &[u8]) -> String {
    stable_fnv1a_hex(bytes).replace(':', "_")
}

fn extract_scratch_dir(
    work_dir: &Path,
    tmp_dir_name: &str,
    out_path: &Path,
    resume: bool,
    resume_fingerprint: Option<&str>,
) -> Result<PathBuf> {
    let name = if resume {
        let fingerprint = resume_fingerprint
            .ok_or_else(|| anyhow!("missing resume fingerprint for extract scratch directory"))?;
        let input = serde_json::json!({
            "resume_fingerprint": fingerprint,
            "out_path": path_for_fingerprint(out_path)?,
        });
        let bytes = serde_json::to_vec(&input).context("serialize extract scratch fingerprint")?;
        format!("{tmp_dir_name}_{}", stable_hash_component(&bytes))
    } else {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let seq = EXTRACT_SCRATCH_COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{tmp_dir_name}_{}_{}_{}", std::process::id(), unique, seq)
    };
    Ok(work_dir.join(name))
}

/// Fingerprint every query/config knob that can change the bytes written by a
/// resumable extract/spool operation. If it changes between `--resume` runs,
/// stale month parts are discarded instead of being stitched into the new run.
fn build_resume_fingerprint(
    etl: &RedditETL,
    query: &QuerySpec,
    operation: &str,
    limit: Option<u64>,
) -> Result<String> {
    let zst_level = (operation == "partitioned-zst").then_some(etl.opts.zst_level);
    let input = serde_json::json!({
        "operation": operation,
        "corpus_paths": {
            "base_dir": path_for_fingerprint(&etl.opts.base_dir)?,
            "comments_dir": path_for_fingerprint(&etl.opts.comments_dir)?,
            "submissions_dir": path_for_fingerprint(&etl.opts.submissions_dir)?,
        },
        "sources": format!("{:?}", etl.opts.sources),
        "start": opt_ym_string(etl.opts.start),
        "end": opt_ym_string(etl.opts.end),
        "legacy_subreddit": etl.opts.subreddit.as_ref(),
        "whitelist_fields": etl.opts.whitelist_fields.as_ref(),
        "strict_whitelist": etl.opts.strict_whitelist,
        "human_readable_timestamps": etl.opts.human_readable_timestamps,
        "zst_level": zst_level,
        "limit": limit,
        "query": {
            "subreddits": query.subreddits.as_ref(),
            "authors_in": query.authors_in.as_ref(),
            "authors_out": query.authors_out.as_ref(),
            "exclude_common_bots": query.exclude_common_bots,
            "author_regex": query.author_regex.as_ref().map(|re| re.as_str()),
            "author_regex_pattern": query.author_regex_pattern.as_ref(),
            "min_score": query.min_score,
            "max_score": query.max_score,
            "keywords_any": query.keywords_any.as_ref(),
            "domains_in": query.domains_in.as_ref(),
            "contains_url": query.contains_url,
            "json_predicates": query.json_predicates_fingerprint(),
            "filter_pseudo_users": query.filter_pseudo_users,
        }
    });
    let bytes = serde_json::to_vec(&input).context("serialize resume fingerprint input")?;
    Ok(stable_fnv1a_hex(&bytes))
}

fn remove_matching_files(dir: &Path, mut should_remove: impl FnMut(&str) -> bool) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    let entries = read_dir_with_backoff(dir, 16, 50)
        .with_context(|| format!("read_dir {}", dir.display()))?;
    for entry in entries {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if should_remove(name) {
            if let Err(e) = remove_with_backoff(&path, 8, 50) {
                tracing::warn!(path=%path.display(), error=%e, "failed to remove stale resume part");
            }
        }
    }
    Ok(())
}

fn clear_spool_resume_parts(out_dir: &Path) -> Result<()> {
    remove_matching_files(out_dir, |name| {
        (name.starts_with("part_RC_") || name.starts_with("part_RS_")) && name.ends_with(".jsonl")
    })
}

fn clear_extract_resume_parts(tmp_dir: &Path) -> Result<()> {
    remove_matching_files(tmp_dir, |name| {
        name.starts_with(".part_") && name.ends_with(".jsonl")
    })
}

struct MonthJobCtx<'a> {
    out_dir: &'a Path,
    staging_dir: &'a Path,
    targets: Option<&'a Vec<String>>,
    query: &'a QuerySpec,
    whitelist: &'a Option<Vec<String>>,
    pb: Option<&'a ProgressBar>,
    bounds: Option<DateBounds>,
    read_buf: usize,
    write_buf: usize,
    human_ts: bool,
    whitelist_tracker: Option<&'a WhitelistMatchTracker>,
    record_limit: Option<&'a RecordLimit>,
    resume: bool,
    completed_keys: &'a HashSet<String>,
    allow_partial: bool,
    partial_reporter: Option<&'a crate::config::PartialReadReporter>,
}

/// Load `_progress.json`, validate it against the current fingerprint and each
/// entry against the on-disk file size, drop mismatches with a warning, and
/// return the surviving entries plus a snapshot of completed keys.
fn load_and_validate_manifest(
    out_dir: &Path,
    fingerprint: &str,
) -> Result<(HashMap<String, MonthEntry>, HashSet<String>)> {
    let manifest = crate::progress_manifest::load(out_dir);
    if manifest.fingerprint.as_deref() != Some(fingerprint) && !manifest.months.is_empty() {
        tracing::warn!(
            path=%crate::progress_manifest::manifest_path(out_dir).display(),
            stored=?manifest.fingerprint,
            current=%fingerprint,
            "resume manifest fingerprint does not match current query/config; discarding spool parts"
        );
        clear_spool_resume_parts(out_dir)?;
        return Ok((HashMap::new(), HashSet::new()));
    }
    let mut keep: HashMap<String, MonthEntry> = HashMap::new();
    for (k, v) in manifest.months {
        let final_name = format!("part_{}.jsonl", k);
        let final_path = out_dir.join(&final_name);
        match fs::metadata(&final_path) {
            Ok(meta) if meta.len() == v.size => {
                keep.insert(k, v);
            }
            _ => {
                tracing::info!(key=%k, "dropping stale progress manifest entry; month will be re-run");
            }
        }
    }
    let completed_keys: HashSet<String> = keep.keys().cloned().collect();
    Ok((keep, completed_keys))
}

/// Per-month closure body: skip if the month is already published (resume
/// fast-path), otherwise atomically write the spool output via
/// `write_jsonl_atomic`. Returns `Ok(None)` on resume-skip or a tolerated zstd
/// decode/partial-scan skip (already logged); `Ok(Some(MonthResult))` on a
/// successful publish whose entry the caller should commit to the manifest.
/// Non-decode output, malformed-JSON, and publish failures are fatal.
fn process_month(job: &FileJob, ctx: &MonthJobCtx<'_>) -> Result<Option<MonthResult>> {
    let (file_prefix, key_prefix) = match job.kind {
        FileKind::Comment => (FILE_PREFIX_RC, "RC"),
        FileKind::Submission => (FILE_PREFIX_RS, "RS"),
    };
    let out_path = ctx
        .out_dir
        .join(format!("{}_{}.jsonl", file_prefix, job.ym));
    let key = crate::progress_manifest::month_key(key_prefix, job.ym);

    if ctx.record_limit.is_some_and(|limit| limit.is_exhausted()) {
        return Ok(None);
    }

    // Resume fast-path: skip the month entirely if it's already in the
    // manifest (and the on-disk file matched at load time). Still bump the
    // progress bar by the input file's compressed size so the user sees the
    // work accounted for.
    if ctx.resume && ctx.completed_keys.contains(&key) {
        if let Some(pb) = ctx.pb {
            let sz = fs::metadata(&job.path).map(|m| m.len()).unwrap_or(0);
            pb.inc(sz);
        }
        return Ok(None);
    }

    let n = match write_jsonl_atomic(ctx.staging_dir, &out_path, ctx.write_buf, |w| {
        let result = stream_job_with_partial_policy(
            job,
            w,
            ctx.targets,
            ctx.query,
            ctx.whitelist,
            ctx.pb.cloned(),
            ctx.bounds,
            ctx.read_buf,
            ctx.human_ts,
            ctx.whitelist_tracker,
            ctx.allow_partial,
            ctx.partial_reporter,
            ctx.record_limit,
        )?;
        complete_stream_job(job, result)
    }) {
        Ok(n) => n,
        Err(e) if ctx.allow_partial && is_partial_scan_error(&e) => {
            tracing::warn!(path=%job.path.display(), output=%out_path.display(), error=%e, "Skipping month after zstd decode error; staged spool output was discarded and resume will retry it");
            return Ok(None);
        }
        Err(e) => return Err(e),
    };

    Ok(Some(MonthResult {
        out_path,
        key,
        lines: n,
    }))
}

/// Atomically commit a month's manifest entry after a successful publish.
/// `write_jsonl_atomic` only returns Ok once the rename landed, so the size
/// stat is taken from the published path. Resume-enabled callers fail the run
/// immediately if this commit fails: the data file may remain published, but
/// returning success would incorrectly claim a durable checkpoint.
fn commit_entry_to_manifest(acc: &ManifestAccumulator, result: MonthResult) -> Result<()> {
    let size = fs::metadata(&result.out_path).map(|m| m.len()).unwrap_or(0);
    let entry = MonthEntry {
        size,
        lines: result.lines,
        sha256: None,
    };
    acc.commit(result.key, entry)
}

fn ensure_resume_manifest_durable(
    accumulator: Option<&ManifestAccumulator>,
    operation: &str,
) -> Result<()> {
    if let Some(error) = accumulator.and_then(ManifestAccumulator::last_save_error) {
        anyhow::bail!(
            "{operation} resume progress manifest is not durable; earlier save failed: {error}"
        );
    }
    Ok(())
}

fn export_part_key(job: &FileJob) -> String {
    let prefix = match job.kind {
        FileKind::Comment => "RC",
        FileKind::Submission => "RS",
    };
    crate::progress_manifest::month_key(prefix, job.ym)
}

fn partitioned_resume_operation(format: ExportFormat) -> &'static str {
    match format {
        ExportFormat::Jsonl => "partitioned-jsonl",
        ExportFormat::Zst => "partitioned-zst",
    }
}

fn partitioned_ext(format: ExportFormat) -> &'static str {
    match format {
        ExportFormat::Jsonl => "jsonl",
        ExportFormat::Zst => "zst",
    }
}

fn partitioned_output_path(out_base_dir: &Path, job: &FileJob, format: ExportFormat) -> PathBuf {
    let (dir, prefix) = match job.kind {
        FileKind::Comment => ("comments", "RC"),
        FileKind::Submission => ("submissions", "RS"),
    };
    out_base_dir
        .join(dir)
        .join(format!("{}_{}.{}", prefix, job.ym, partitioned_ext(format)))
}

fn partitioned_output_path_for_key(
    out_base_dir: &Path,
    key: &str,
    format: ExportFormat,
) -> Option<PathBuf> {
    let dir = if key.starts_with("RC_") {
        "comments"
    } else if key.starts_with("RS_") {
        "submissions"
    } else {
        return None;
    };
    Some(
        out_base_dir
            .join(dir)
            .join(format!("{}.{}", key, partitioned_ext(format))),
    )
}

fn clear_partitioned_resume_outputs(out_base_dir: &Path, format: ExportFormat) -> Result<()> {
    let suffix = format!(".{}", partitioned_ext(format));
    remove_matching_files(&out_base_dir.join("comments"), |name| {
        name.starts_with("RC_") && name.ends_with(&suffix)
    })?;
    remove_matching_files(&out_base_dir.join("submissions"), |name| {
        name.starts_with("RS_") && name.ends_with(&suffix)
    })?;
    Ok(())
}

fn validate_partitioned_resume_output(
    path: &Path,
    format: ExportFormat,
    expected: &MonthEntry,
) -> Result<()> {
    if expected.lines == 0 && !path.exists() {
        return Ok(());
    }
    let meta = fs::metadata(path)
        .with_context(|| format!("stat partitioned output {}", path.display()))?;
    if meta.len() != expected.size {
        anyhow::bail!(
            "partitioned output size mismatch for {}: expected {}, got {}",
            path.display(),
            expected.size,
            meta.len()
        );
    }
    match format {
        ExportFormat::Jsonl => {
            let actual = validate_jsonl_part(path)?;
            if actual.lines != expected.lines {
                anyhow::bail!(
                    "partitioned JSONL line-count mismatch for {}: expected {}, got {}",
                    path.display(),
                    expected.lines,
                    actual.lines
                );
            }
        }
        ExportFormat::Zst => {
            crate::zstd_jsonl::validate_zst_full(path)
                .with_context(|| format!("validating partitioned zst {}", path.display()))?;
        }
    }
    Ok(())
}

fn load_and_validate_partitioned_manifest(
    out_base_dir: &Path,
    fingerprint: &str,
    format: ExportFormat,
) -> Result<(HashMap<String, MonthEntry>, HashSet<String>)> {
    let manifest = crate::progress_manifest::load(out_base_dir);
    if manifest.fingerprint.as_deref() != Some(fingerprint) && !manifest.months.is_empty() {
        tracing::warn!(
            path=%crate::progress_manifest::manifest_path(out_base_dir).display(),
            stored=?manifest.fingerprint,
            current=%fingerprint,
            "partitioned resume manifest fingerprint does not match current query/config; discarding partitioned outputs"
        );
        clear_partitioned_resume_outputs(out_base_dir, format)?;
        return Ok((HashMap::new(), HashSet::new()));
    }

    let mut keep: HashMap<String, MonthEntry> = HashMap::new();
    for (key, entry) in manifest.months {
        let Some(path) = partitioned_output_path_for_key(out_base_dir, &key, format) else {
            tracing::info!(key=%key, "dropping unrecognized partitioned progress entry");
            continue;
        };
        match validate_partitioned_resume_output(&path, format, &entry) {
            Ok(()) => {
                keep.insert(key, entry);
            }
            Err(e) => {
                tracing::info!(key=%key, path=%path.display(), error=%e, "dropping stale partitioned progress entry; month will be re-run");
            }
        }
    }
    let completed_keys: HashSet<String> = keep.keys().cloned().collect();
    Ok((keep, completed_keys))
}

fn export_part_path(tmp_dir: &Path, key: &str) -> PathBuf {
    tmp_dir.join(format!(".part_{}.jsonl", key))
}

fn load_and_validate_export_manifest(
    tmp_dir: &Path,
    fingerprint: &str,
) -> Result<(HashMap<String, MonthEntry>, HashSet<String>)> {
    let manifest = crate::progress_manifest::load(tmp_dir);
    if manifest.fingerprint.as_deref() != Some(fingerprint) {
        tracing::warn!(
            path=%crate::progress_manifest::manifest_path(tmp_dir).display(),
            stored=?manifest.fingerprint,
            current=%fingerprint,
            "extract resume manifest fingerprint does not match current query/config; discarding cached parts"
        );
        clear_extract_resume_parts(tmp_dir)?;
        return Ok((HashMap::new(), HashSet::new()));
    }
    let mut keep: HashMap<String, MonthEntry> = HashMap::new();
    for (k, v) in manifest.months {
        let part_path = export_part_path(tmp_dir, &k);
        match validate_jsonl_part(&part_path) {
            Ok(entry) if entry.size == v.size => {
                keep.insert(k, entry);
            }
            _ => {
                tracing::info!(key=%k, "dropping stale extract progress entry; month will be re-run")
            }
        }
    }
    let completed_keys: HashSet<String> = keep.keys().cloned().collect();
    Ok((keep, completed_keys))
}

fn validate_jsonl_part(path: &Path) -> Result<MonthEntry> {
    let file = open_with_backoff(path, 16, 50)
        .with_context(|| format!("opening extract resume part {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut buf = String::new();
    let mut lines = 0_u64;
    loop {
        let n = crate::ndjson::read_line_capped(
            &mut reader,
            &mut buf,
            crate::ndjson::DEFAULT_MAX_LINE_BYTES,
            path,
        )?;
        if n == 0 {
            break;
        }
        if buf.is_empty() {
            continue;
        }
        let _: serde_json::Value = serde_json::from_str(&buf)
            .with_context(|| format!("validating JSONL part {}", path.display()))?;
        lines += 1;
    }
    let size = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    Ok(MonthEntry {
        size,
        lines,
        sha256: None,
    })
}

fn normalize_tabular_fields<I, S>(fields: I) -> Result<Vec<String>>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let fields: Vec<String> = fields
        .into_iter()
        .filter_map(|field| {
            let field = field.into();
            let field = field.trim();
            (!field.is_empty()).then(|| field.to_string())
        })
        .collect();
    if fields.is_empty() {
        anyhow::bail!("CSV/TSV export requires at least one whitelisted field");
    }
    Ok(fields)
}

fn tabular_part_path(tmp_dir: &Path, key: &str, format: TabularFormat) -> PathBuf {
    tmp_dir.join(format!(".part_{}{}", key, format.row_suffix()))
}

fn tabular_part_paths(tmp_dir: &Path, format: TabularFormat) -> Result<Vec<PathBuf>> {
    let suffix = format.row_suffix();
    let mut paths = Vec::new();
    for entry in read_dir_with_backoff(tmp_dir, 16, 50)
        .with_context(|| format!("read_dir {}", tmp_dir.display()))?
    {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if name.starts_with(".part_") && name.ends_with(suffix) {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

fn write_csv_cell<W: Write + ?Sized>(out: &mut W, cell: &str) -> Result<()> {
    let quote = cell
        .as_bytes()
        .iter()
        .any(|b| matches!(b, b',' | b'"' | b'\n' | b'\r'));
    if !quote {
        out.write_all(cell.as_bytes())?;
        return Ok(());
    }
    out.write_all(b"\"")?;
    for b in cell.as_bytes() {
        if *b == b'"' {
            out.write_all(b"\"\"")?;
        } else {
            out.write_all(&[*b])?;
        }
    }
    out.write_all(b"\"")?;
    Ok(())
}

fn value_to_tabular_cell(value: Option<&Value>) -> Result<String> {
    let Some(value) = value else {
        return Ok(String::new());
    };
    match value {
        Value::Null => Ok(String::new()),
        Value::String(s) => Ok(s.clone()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Array(_) | Value::Object(_) => Ok(serde_json::to_string(value)?),
    }
}

fn write_tabular_row<W: Write + ?Sized>(
    out: &mut W,
    fields: &[String],
    cells: &[String],
    format: TabularFormat,
) -> Result<()> {
    match format {
        TabularFormat::Csv => {
            for (i, cell) in cells.iter().enumerate() {
                if i > 0 {
                    out.write_all(b",")?;
                }
                write_csv_cell(out, cell)?;
            }
            out.write_all(b"\r\n")?;
        }
        TabularFormat::Tsv => {
            for (field, cell) in fields.iter().zip(cells.iter()) {
                if cell.contains('\t') {
                    tracing::warn!(
                        field,
                        "refusing to emit TSV value containing a literal tab; use --format csv for robust escaping"
                    );
                    anyhow::bail!(
                        "TSV export cannot represent a literal tab in field {field:?}; use --format csv"
                    );
                }
            }
            for (i, cell) in cells.iter().enumerate() {
                if i > 0 {
                    out.write_all(b"\t")?;
                }
                out.write_all(cell.as_bytes())?;
            }
            out.write_all(b"\n")?;
        }
    }
    Ok(())
}

fn tabular_cells_from_value(value: &Value, fields: &[String]) -> Result<(Vec<String>, bool)> {
    let map = value.as_object();
    let mut matched_any = false;
    let mut cells = Vec::with_capacity(fields.len());
    for field in fields {
        let field_value = map.and_then(|map| map.get(field));
        if field_value.is_some() {
            matched_any = true;
        }
        cells.push(value_to_tabular_cell(field_value)?);
    }
    Ok((cells, matched_any))
}

fn write_tabular_header<W: Write + ?Sized>(
    out: &mut W,
    fields: &[String],
    format: TabularFormat,
) -> Result<()> {
    write_tabular_row(out, fields, fields, format)
}

fn stitch_tabular_parts(
    tmp_dir: &Path,
    out_path: &Path,
    fields: &[String],
    opts: TabularExportOptions,
    format: TabularFormat,
    write_buf: usize,
) -> Result<()> {
    let parts = tabular_part_paths(tmp_dir, format)?;
    let parent = out_path.parent().unwrap_or_else(|| Path::new("."));
    let staging_dir = ensure_staging_dir(parent)?;
    write_jsonl_atomic(&staging_dir, out_path, write_buf, |out| {
        if opts.header {
            write_tabular_header(out, fields, format)?;
        }
        for path in &parts {
            let mut r = BufReader::new(open_with_backoff(path, 16, 50)?);
            std::io::copy(&mut r, out)?;
        }
        Ok(())
    })
}

#[allow(clippy::too_many_arguments)]
fn stream_tabular_job<W: Write + ?Sized>(
    job: &FileJob,
    writer: &mut W,
    targets: Option<&Vec<String>>,
    query: &QuerySpec,
    fields: &[String],
    format: TabularFormat,
    pb: Option<ProgressBar>,
    bounds: Option<DateBounds>,
    read_buf_bytes: usize,
    whitelist_tracker: Option<&WhitelistMatchTracker>,
    allow_partial: bool,
    partial_reporter: Option<&crate::config::PartialReadReporter>,
    record_limit: Option<&RecordLimit>,
) -> Result<StreamJobResult> {
    let mut written = 0_u64;
    let mut line_number = 0_u64;
    let mut on_line = |line: &str| -> Result<()> {
        line_number += 1;
        let min = match parse_minimal(line) {
            Ok(min) => min,
            Err(_) => match serde_json::from_str::<Value>(line) {
                Ok(_) => return Ok(()),
                Err(e) => return Err(malformed_json_error(&job.path, line_number, e)),
            },
        };
        if !matches_minimal(&min, targets, query) || !within_bounds(&min, bounds) {
            return Ok(());
        }
        if query.requires_full_parse() {
            let val: Value = serde_json::from_str(line)
                .map_err(|e| malformed_json_error(&job.path, line_number, e))?;
            if !matches_full(&val, job.kind, query) {
                return Ok(());
            }
        }
        claim_record_or_stop(record_limit)?;
        let val: Value = serde_json::from_str(line)
            .map_err(|e| malformed_json_error(&job.path, line_number, e))?;
        let (cells, matched_any) = tabular_cells_from_value(&val, fields)?;
        write_tabular_row(writer, fields, &cells, format)?;
        written += 1;
        if let Some(tracker) = whitelist_tracker {
            tracker.observe(crate::streaming::WhitelistEmission {
                emitted_empty_projection: !matched_any,
                used_slow_path: false,
            })?;
        }
        Ok(())
    };

    let partial_read_policy = if allow_partial {
        PartialReadPolicy::AllowPartial
    } else {
        PartialReadPolicy::Strict
    };
    let mut progress_cb = pb.map(|pb| move |delta| pb.inc(delta));
    let mut skip_cb = |path: &Path, err: &anyhow::Error| {
        if let Some(reporter) = partial_reporter {
            reporter.record(path, err);
        }
    };
    let stream_result = for_each_line_with_opts_status(
        &job.path,
        LineStreamOpts {
            read_buf_bytes: Some(read_buf_bytes),
            progress: progress_cb.as_mut().map(|cb| cb as &mut dyn FnMut(u64)),
            on_skip: allow_partial.then_some(&mut skip_cb as &mut dyn FnMut(&Path, &anyhow::Error)),
            partial_read_policy,
            ..Default::default()
        },
        |s| on_line(s),
    );
    let complete = match stream_result {
        Ok(complete) => complete,
        Err(e) if is_record_limit_reached(&e) => true,
        Err(e) => return Err(e),
    };
    if let Some(tracker) = whitelist_tracker {
        tracker.finalize()?;
    }
    Ok(StreamJobResult { written, complete })
}

#[allow(clippy::too_many_arguments)]
fn extract_tabular_common(
    etl: &RedditETL,
    query: &QuerySpec,
    targets: Option<&Vec<String>>,
    out_path: &Path,
    fields: &[String],
    opts: TabularExportOptions,
    format: TabularFormat,
    limit: Option<u64>,
) -> Result<()> {
    let parallelism = etl.opts.parallelism;
    with_thread_pool(parallelism, || {
        let files = plan_pipeline_files(etl)?;
        warn_if_unfiltered_undated_query(etl, query, &files);

        let work_dir = etl.ensure_work_dir()?;
        let tmp_dir = extract_scratch_dir(&work_dir, format.tmp_dir_name(), out_path, false, None)?;
        create_dir_all_with_backoff(&tmp_dir, 16, 50).with_context(|| {
            format!(
                "creating {} extract work dir {}",
                format.label(),
                tmp_dir.display()
            )
        })?;
        let staging_dir = ensure_staging_dir(&tmp_dir)?;
        sweep_stale_inprogress(&tmp_dir, true)?;

        let whitelist_tracker = Some(Arc::new(WhitelistMatchTracker::new(
            etl.opts.strict_whitelist,
        )));
        let record_limit = record_limit_from(limit);
        let total_bytes = total_compressed_size(&files);
        let pb = if etl.opts.progress {
            Some(make_progress_bar_labeled(
                total_bytes,
                etl.opts.progress_label.as_deref(),
            ))
        } else {
            None
        };

        let bounds = bounds_tuple(etl.opts.start, etl.opts.end);
        let read_buf = etl.opts.read_buffer_bytes;
        let write_buf = etl.opts.write_buffer_bytes;

        crate::concurrency::for_each_file_limited(
            &files,
            etl.opts.file_concurrency,
            |job| -> Result<()> {
                if record_limit
                    .as_ref()
                    .is_some_and(|limit| limit.is_exhausted())
                {
                    return Ok(());
                }
                let key = export_part_key(job);
                let tmp_file = tabular_part_path(&tmp_dir, &key, format);
                let lines = match write_jsonl_atomic(&staging_dir, &tmp_file, write_buf, |w| {
                    let result = stream_tabular_job(
                        job,
                        w,
                        targets,
                        query,
                        fields,
                        format,
                        pb.clone(),
                        bounds,
                        read_buf,
                        whitelist_tracker.as_deref(),
                        etl.opts.allow_partial,
                        Some(&etl.opts.partial_read_reporter),
                        record_limit.as_deref(),
                    )?;
                    complete_stream_job(job, result)
                }) {
                    Ok(lines) => lines,
                    Err(e) if etl.opts.allow_partial && is_partial_scan_error(&e) => {
                        tracing::warn!(path=%job.path.display(), part=%tmp_file.display(), error=%e, "Skipping tabular export month after zstd decode error; staged part was discarded");
                        return Ok(());
                    }
                    Err(e) => return Err(e),
                };
                if lines == 0 {
                    let _ = remove_with_backoff(&tmp_file, 8, 50);
                }
                Ok(())
            },
        )?;

        if let Some(pb) = pb {
            pb.finish_with_message("done");
        }
        stitch_tabular_parts(&tmp_dir, out_path, fields, opts, format, write_buf)?;
        if let Err(e) = remove_dir_all_with_backoff(&tmp_dir, 8, 50) {
            tracing::warn!(path=%tmp_dir.display(), error=%e, "failed to remove tabular extract scratch dir");
        }
        Ok(())
    })
}

/// Shared extraction logic used by:
///  - `ScanPlan::extract_to_jsonl`
///  - `ScanPlan::extract_to_json`
/// Keeps progress/date-bounds/streaming consistent and DRY.
fn extract_common(
    etl: &RedditETL,
    query: &QuerySpec,
    targets: Option<&Vec<String>>,
    tmp_dir_name: &str,
    out_path: &Path,
    finalize: Finalize,
    limit: Option<u64>,
) -> Result<()> {
    validate_export_whitelist(etl)?;
    let parallelism = etl.opts.parallelism;
    with_thread_pool(parallelism, || {
        let files = plan_pipeline_files(etl)?;
        warn_if_unfiltered_undated_query(etl, query, &files);

        let work_dir = etl.ensure_work_dir()?;
        let resume = etl.opts.resume;
        let resume_fingerprint = if resume {
            Some(build_resume_fingerprint(etl, query, "extract", limit)?)
        } else {
            None
        };
        let tmp_dir = extract_scratch_dir(
            &work_dir,
            tmp_dir_name,
            out_path,
            resume,
            resume_fingerprint.as_deref(),
        )?;
        create_dir_all_with_backoff(&tmp_dir, 16, 50)
            .with_context(|| format!("creating extract work dir {}", tmp_dir.display()))?;
        let staging_dir = ensure_staging_dir(&tmp_dir)?;
        sweep_stale_inprogress(&tmp_dir, true)?;

        let (initial_months, completed_keys) =
            if let Some(fingerprint) = resume_fingerprint.as_deref() {
                load_and_validate_export_manifest(&tmp_dir, fingerprint)?
            } else {
                (HashMap::new(), HashSet::new())
            };
        let resumed_lines = committed_line_count(&initial_months);
        let accumulator = if let Some(fingerprint) = resume_fingerprint.as_ref() {
            crate::progress_manifest::save(&tmp_dir, &initial_months, Some(fingerprint))?;
            Some(ManifestAccumulator::new(
                &tmp_dir,
                initial_months,
                Some(fingerprint.clone()),
            ))
        } else {
            None
        };

        let whitelist = etl.opts.whitelist_fields.clone();
        let whitelist_tracker = whitelist
            .as_ref()
            .map(|_| Arc::new(WhitelistMatchTracker::new(etl.opts.strict_whitelist)));
        let record_limit = record_limit_from_with_claimed(limit, resumed_lines);

        let total_bytes = total_compressed_size(&files);
        let pb = if etl.opts.progress {
            Some(make_progress_bar_labeled(
                total_bytes,
                etl.opts.progress_label.as_deref(),
            ))
        } else {
            None
        };

        let targets_ref = targets;
        let bounds = bounds_tuple(etl.opts.start, etl.opts.end);
        let read_buf = etl.opts.read_buffer_bytes;
        let write_buf = etl.opts.write_buffer_bytes;
        let human_ts = etl.opts.human_readable_timestamps;

        crate::concurrency::for_each_file_limited(
            &files,
            etl.opts.file_concurrency,
            |job| -> Result<()> {
                let key = export_part_key(job);
                let tmp_file = export_part_path(&tmp_dir, &key);

                if record_limit
                    .as_ref()
                    .is_some_and(|limit| limit.is_exhausted())
                {
                    return Ok(());
                }

                if resume && completed_keys.contains(&key) {
                    if let Some(pb) = &pb {
                        let sz = fs::metadata(&job.path).map(|m| m.len()).unwrap_or(0);
                        pb.inc(sz);
                    }
                    return Ok(());
                }

                if resume && tmp_file.exists() {
                    match validate_jsonl_part(&tmp_file) {
                        Ok(entry) => {
                            if let Some(acc) = &accumulator {
                                acc.commit(key.clone(), entry).with_context(|| {
                                    format!(
                                        "failed to durably update extract progress manifest for existing resume part {key}"
                                    )
                                })?;
                            }
                            if let Some(pb) = &pb {
                                let sz = fs::metadata(&job.path).map(|m| m.len()).unwrap_or(0);
                                pb.inc(sz);
                            }
                            return Ok(());
                        }
                        Err(e) => {
                            tracing::info!(path=%tmp_file.display(), error=%e, "resume part failed validation; rebuilding");
                            let _ = remove_with_backoff(&tmp_file, 8, 50);
                        }
                    }
                }

                let lines = match write_jsonl_atomic(&staging_dir, &tmp_file, write_buf, |w| {
                    let result = stream_job_with_partial_policy(
                        job,
                        w,
                        targets_ref,
                        query,
                        &whitelist,
                        pb.clone(),
                        bounds,
                        read_buf,
                        human_ts,
                        whitelist_tracker.as_deref(),
                        etl.opts.allow_partial,
                        Some(&etl.opts.partial_read_reporter),
                        record_limit.as_deref(),
                    )?;
                    complete_stream_job(job, result)
                }) {
                    Ok(lines) => lines,
                    Err(e) if etl.opts.allow_partial && is_partial_scan_error(&e) => {
                        tracing::warn!(path=%job.path.display(), part=%tmp_file.display(), error=%e, "Skipping extract month after zstd decode error; staged part was discarded and resume will retry it");
                        return Ok(());
                    }
                    Err(e) => return Err(e),
                };

                if let Some(acc) = &accumulator {
                    let size = fs::metadata(&tmp_file).map(|m| m.len()).unwrap_or(0);
                    let entry = MonthEntry {
                        size,
                        lines,
                        sha256: None,
                    };
                    acc.commit(key, entry).context(
                        "failed to durably update extract progress manifest after publishing resume part",
                    )?;
                }
                Ok(())
            },
        )?;

        if let Some(pb) = pb {
            pb.finish_with_message("done");
        }
        ensure_resume_manifest_durable(accumulator.as_ref(), "extract")?;

        match finalize {
            Finalize::Jsonl => stitch_tmp_parts(&tmp_dir, out_path, write_buf)?,
            Finalize::JsonArray { pretty } => {
                stitch_tmp_parts_to_json_array(&tmp_dir, out_path, pretty, write_buf)?
            }
        }
        if !resume {
            if let Err(e) = remove_dir_all_with_backoff(&tmp_dir, 8, 50) {
                tracing::warn!(path=%tmp_dir.display(), error=%e, "failed to remove extract scratch dir");
            }
        }
        Ok(())
    })
}

/// Shared scan+filter+emit loop for [`ScanPlan`] methods.
///
/// Encapsulates file discovery/planning, optional progress-bar setup, the
/// `for_each_file_limited` per-file fan-out, and the line-level
/// `parse_minimal` → `matches_minimal` → `within_bounds` →
/// (optional) `matches_full` filtering ladder. The caller owns whatever
/// accumulator the matched records feed into and supplies it through
/// interior mutability inside `on_record`.
///
/// Callers are responsible for entering `with_thread_pool` themselves when
/// they want bounded parallelism that *also* covers post-scan reductions like
/// `dedup` / `reduce_sum` (which use rayon `par_iter` internally). Methods
/// that historically don't enter a scoped pool — e.g. `count_by_month`,
/// `build_first_seen_index_to_tsv` — call this helper directly so the
/// per-file fan-out runs on the global rayon pool, matching the prior shape.
fn scan_records<F>(
    etl: &RedditETL,
    query: &QuerySpec,
    show_progress: bool,
    limit: Option<u64>,
    on_record: F,
) -> Result<()>
where
    F: Sync + Send + Fn(&MinimalRecord, FileKind, &str) -> Result<()>,
{
    let targets = resolve_target_subs_from(&etl.opts.subreddit, &query.subreddits);
    let targets_ref = targets.as_ref();
    let bounds = bounds_tuple(etl.opts.start, etl.opts.end);
    let read_buf = etl.opts.read_buffer_bytes;
    let record_limit = record_limit_from(limit);
    if record_limit.as_ref().is_some_and(|limit| limit.is_zero()) {
        return Ok(());
    }

    let files = plan_pipeline_files(etl)?;
    warn_if_unfiltered_undated_query(etl, query, &files);

    let pb = if show_progress && etl.opts.progress {
        let total_bytes = total_compressed_size(&files);
        Some(make_progress_bar_labeled(
            total_bytes,
            etl.opts.progress_label.as_deref(),
        ))
    } else {
        None
    };

    let fanout = crate::concurrency::for_each_file_limited(
        &files,
        etl.opts.file_concurrency,
        |job| -> Result<()> {
            if record_limit
                .as_ref()
                .is_some_and(|limit| limit.is_exhausted())
            {
                return Ok(());
            }
            let kind = job.kind;
            let mut line_number: u64 = 0;
            let line_cb = |line: &str| -> Result<()> {
                line_number += 1;
                let min = match parse_minimal(line) {
                    Ok(min) => min,
                    Err(_) => match serde_json::from_str::<serde_json::Value>(line) {
                        Ok(_) => return Ok(()),
                        Err(e) => return Err(malformed_json_error(&job.path, line_number, e)),
                    },
                };
                if !matches_minimal(&min, targets_ref, query) {
                    return Ok(());
                }
                if !within_bounds(&min, bounds) {
                    return Ok(());
                }
                if query.requires_full_parse() {
                    let val: serde_json::Value = serde_json::from_str(line)
                        .map_err(|e| malformed_json_error(&job.path, line_number, e))?;
                    if !matches_full(&val, kind, query) {
                        return Ok(());
                    }
                }
                claim_record_or_stop(record_limit.as_deref())?;
                on_record(&min, kind, line)?;
                Ok(())
            };
            let partial_read_policy = if etl.opts.allow_partial {
                PartialReadPolicy::AllowPartial
            } else {
                PartialReadPolicy::Strict
            };
            let mut progress_cb = pb.as_ref().map(|pb| move |delta| pb.inc(delta));
            let mut skip_cb = |path: &Path, err: &anyhow::Error| {
                etl.opts.partial_read_reporter.record(path, err);
            };
            for_each_line_with_opts_status(
                &job.path,
                LineStreamOpts {
                    read_buf_bytes: Some(read_buf),
                    progress: progress_cb.as_mut().map(|cb| cb as &mut dyn FnMut(u64)),
                    on_skip: etl
                        .opts
                        .allow_partial
                        .then_some(&mut skip_cb as &mut dyn FnMut(&Path, &anyhow::Error)),
                    partial_read_policy,
                    ..Default::default()
                },
                line_cb,
            )?;
            Ok(())
        },
    );
    match fanout {
        Ok(()) => {}
        Err(e) if is_record_limit_reached(&e) => {}
        Err(e) => return Err(e),
    }

    if let Some(pb) = pb {
        pb.finish_with_message("done");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{build_resume_fingerprint, extract_scratch_dir, ExportFormat};
    use crate::progress_manifest::testing::fail_saves_after_attempts_for_tests;
    use crate::{RedditETL, ScanPlan, Sources, YearMonth};
    use serde_json::{json, Value};
    use std::fs::{self, File};
    use std::io::Write;
    use std::path::{Path, PathBuf};

    fn write_zst_lines(path: &Path, lines: &[String]) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let file = File::create(path).unwrap();
        let mut enc = zstd::stream::write::Encoder::new(file, 3).unwrap();
        for line in lines {
            writeln!(&mut enc, "{line}").unwrap();
        }
        enc.finish().unwrap();
    }

    fn make_one_comment_corpus() -> PathBuf {
        let base = tempfile::tempdir().unwrap().keep();
        let line = json!({
            "id":"c1",
            "author":"alice",
            "subreddit":"programming",
            "created_utc":1136073600_i64,
            "score":1,
            "body":"hello",
        })
        .to_string();
        write_zst_lines(&base.join("comments").join("RC_2006-01.zst"), &[line]);
        fs::create_dir_all(base.join("submissions")).unwrap();
        base
    }

    fn one_month_scan(base: &Path) -> ScanPlan {
        RedditETL::new()
            .base_dir(base)
            .sources(Sources::Comments)
            .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
            .progress(false)
            .resume(true)
            .scan()
            .subreddit("programming")
    }

    fn manifest_json(out_dir: &Path) -> Value {
        serde_json::from_slice(&fs::read(out_dir.join("_progress.json")).unwrap()).unwrap()
    }

    #[test]
    fn spool_resume_commit_failure_after_publish_returns_error_and_next_run_succeeds() {
        let base = make_one_comment_corpus();
        let out_dir = base.join("spool_manifest_failure");

        let err = {
            // The first save is the initial pruned manifest; fail the per-month
            // commit that happens only after the spool part has been published.
            let _guard = fail_saves_after_attempts_for_tests(&out_dir, 1, 1);
            one_month_scan(&base)
                .extract_spool_monthly(&out_dir)
                .expect_err("manifest commit failure must fail the run")
        };
        assert!(
            err.to_string()
                .contains("durably update resume progress manifest"),
            "unexpected error: {err:#}"
        );

        let part = out_dir.join("part_RC_2006-01.jsonl");
        assert!(
            part.exists(),
            "data publish should remain atomic and durable"
        );
        let manifest = manifest_json(&out_dir);
        assert!(
            manifest["months"].as_object().unwrap().is_empty(),
            "failed commit must not claim the month is resumable: {manifest}"
        );

        let (_parts, written) = one_month_scan(&base)
            .extract_spool_monthly(&out_dir)
            .expect("subsequent run without manifest failure should succeed");
        assert_eq!(written, 1);
        let manifest = manifest_json(&out_dir);
        assert!(manifest["months"]
            .as_object()
            .unwrap()
            .contains_key("RC_2006-01"));
    }

    #[test]
    fn partitioned_resume_commit_failure_after_publish_returns_error() {
        let base = make_one_comment_corpus();
        let out_dir = base.join("partitioned_manifest_failure");

        let err = {
            let _guard = fail_saves_after_attempts_for_tests(&out_dir, 1, 1);
            one_month_scan(&base)
                .export_partitioned(&out_dir, ExportFormat::Jsonl)
                .expect_err("manifest commit failure must fail the partitioned export")
        };
        assert!(
            err.to_string()
                .contains("durably update partitioned export progress manifest"),
            "unexpected error: {err:#}"
        );

        let part = out_dir.join("comments").join("RC_2006-01.jsonl");
        assert!(
            part.exists(),
            "partition should remain published after commit failure"
        );
        let manifest = manifest_json(&out_dir);
        assert!(
            manifest["months"].as_object().unwrap().is_empty(),
            "failed commit must not mark the partition resumable: {manifest}"
        );

        one_month_scan(&base)
            .export_partitioned(&out_dir, ExportFormat::Jsonl)
            .expect("subsequent partitioned export should succeed");
        let manifest = manifest_json(&out_dir);
        assert!(manifest["months"]
            .as_object()
            .unwrap()
            .contains_key("RC_2006-01"));
    }

    #[test]
    fn extract_resume_commit_failure_after_temp_part_publish_returns_error_and_next_run_stitches() {
        let base = make_one_comment_corpus();
        let work_dir = base.join("work_manifest_failure");
        let out = base.join("out.jsonl");

        let plan = RedditETL::new()
            .base_dir(&base)
            .work_dir(&work_dir)
            .sources(Sources::Comments)
            .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
            .progress(false)
            .resume(true)
            .scan()
            .subreddit("programming")
            .build()
            .unwrap();
        let fingerprint =
            build_resume_fingerprint(&plan.etl, &plan.query, "extract", None).unwrap();
        let tmp_dir = extract_scratch_dir(
            &work_dir,
            "extract_jsonl_q_tmp",
            &out,
            true,
            Some(&fingerprint),
        )
        .unwrap();

        let err = {
            let _guard = fail_saves_after_attempts_for_tests(&tmp_dir, 1, 1);
            RedditETL::new()
                .base_dir(&base)
                .work_dir(&work_dir)
                .sources(Sources::Comments)
                .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
                .progress(false)
                .resume(true)
                .scan()
                .subreddit("programming")
                .extract_to_jsonl(&out)
                .expect_err("manifest commit failure must fail extract_to_jsonl")
        };
        assert!(
            err.to_string()
                .contains("durably update extract progress manifest"),
            "unexpected error: {err:#}"
        );

        let tmp_part = tmp_dir.join(".part_RC_2006-01.jsonl");
        assert!(
            tmp_part.exists(),
            "resume temp part should remain published"
        );
        assert!(
            !out.exists(),
            "final extract output should not be stitched after a failed checkpoint"
        );
        let manifest = manifest_json(&tmp_dir);
        assert!(
            manifest["months"].as_object().unwrap().is_empty(),
            "failed commit must not mark the temp part resumable: {manifest}"
        );

        RedditETL::new()
            .base_dir(&base)
            .work_dir(&work_dir)
            .sources(Sources::Comments)
            .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
            .progress(false)
            .resume(true)
            .scan()
            .subreddit("programming")
            .extract_to_jsonl(&out)
            .expect("subsequent extract should commit the temp part and stitch output");
        assert!(out.exists());
        let manifest = manifest_json(&tmp_dir);
        assert!(manifest["months"]
            .as_object()
            .unwrap()
            .contains_key("RC_2006-01"));
    }
}
