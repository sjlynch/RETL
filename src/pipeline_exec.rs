use crate::atomic_write::{
    ensure_staging_dir, sweep_stale_inprogress, write_jsonl_atomic, write_zst_atomic,
};
use crate::date::YearMonth;
use crate::dedupe::{build_runs_sorted, merge_runs_sorted, DedupeCfg};
use crate::filters::{
    bounds_tuple, matches_full, matches_minimal, resolve_target_subs_from, within_bounds,
    ym_from_epoch,
};
use crate::key_extractor::KeyExtractor;
use crate::kv_shard::ShardedKVWriter;
use crate::paths::{discover_all, plan_files_checked, FileJob, FileKind};
use crate::pipeline::{RedditETL, ScanPlan};
use crate::progress::{make_progress_bar_labeled, total_compressed_size};
use crate::progress_manifest::{ManifestAccumulator, MonthEntry};
use crate::query::QuerySpec;
use crate::shard::{ShardedWriter, UsernameStream};
use crate::stitch::{concat_tsvs, stitch_tmp_parts, stitch_tmp_parts_to_json_array};
use crate::streaming::{
    process_file_for_usernames_with_skip, stream_job, StreamJobResult, WhitelistMatchTracker,
};
use crate::util::{create_with_backoff, remove_with_backoff, with_thread_pool};
use crate::zstd_jsonl::{
    for_each_line_cfg, for_each_line_with_progress_cfg, parse_minimal, MinimalRecord,
};
use anyhow::{anyhow, Context, Result};
use indicatif::ProgressBar;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Export format for partitioned corpus-style outputs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExportFormat {
    Jsonl,
    Zst,
}

/// Finalization choice for extract operations (internal).
enum Finalize {
    Jsonl,
    JsonArray { pretty: bool },
}

const FILE_PREFIX_RC: &str = "part_RC";
const FILE_PREFIX_RS: &str = "part_RS";
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

fn is_partial_scan_error(e: &anyhow::Error) -> bool {
    e.chain()
        .any(|cause| cause.downcast_ref::<PartialScanError>().is_some())
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

            crate::concurrency::for_each_file_limited(&files, self.opts.file_concurrency, |job| {
                let skip_count_per_call = skip_count_inner.clone();
                process_file_for_usernames_with_skip(
                    job,
                    read_buf,
                    &subreddit,
                    &shard_writer,
                    pb.clone(),
                    move |_path, _err| {
                        skip_count_per_call.fetch_add(1, Ordering::Relaxed);
                    },
                )
                .with_context(|| format!("processing {}", job.path.display()))
            })?;

            let skipped = skip_count.load(Ordering::Relaxed);
            if skipped > 0 {
                tracing::warn!(
                    skipped_files = skipped,
                    "usernames: {} input file(s) skipped due to zstd decode errors; see prior warnings for paths",
                    skipped
                );
            }

            let deduped_files = shard_writer.dedup("usernames")?;
            UsernameStream::from_deduped_files(deduped_files)
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

            scan_records(
                &plan.etl,
                &plan.query,
                /*show_progress=*/ true,
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

            let deduped = shard_writer.dedup("usernames_q")?;
            UsernameStream::from_deduped_files(deduped)
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
        )
    }

    /// Write the distinct keys from matching records as one key per line.
    ///
    /// The scan/filter pass first spools matching raw JSONL records into the
    /// configured work directory, then reuses the public sorted-run dedupe
    /// engine (`build_runs_sorted` + `merge_runs_sorted`) with the supplied
    /// [`KeyExtractor`]. Built-in `author` and `subreddit` extractors keep the
    /// `MinimalRecord` fast path; `json:/pointer` extractors parse full JSON
    /// only for key extraction.
    pub fn dedupe_keys_to_lines(self, key: &KeyExtractor, out_path: &Path) -> Result<u64> {
        let parallelism = self.etl.opts.parallelism;
        with_thread_pool(parallelism, || {
            let work_dir = self.etl.ensure_work_dir()?;
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let tmp_dir = work_dir.join(format!("dedupe_{}_{}", std::process::id(), unique));
            fs::create_dir_all(&tmp_dir)
                .with_context(|| format!("creating dedupe work dir {}", tmp_dir.display()))?;

            let result = (|| -> Result<u64> {
                let input = tmp_dir.join("matched.ndjson");
                let f = create_with_backoff(&input, 16, 50)
                    .with_context(|| format!("create {}", input.display()))?;
                let writer = Mutex::new(BufWriter::with_capacity(
                    self.etl.opts.write_buffer_bytes,
                    f,
                ));
                let matched_records = AtomicU64::new(0);

                scan_records(
                    &self.etl,
                    &self.query,
                    /*show_progress=*/ true,
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

                let mut cfg = DedupeCfg {
                    read_buf_bytes: self.etl.opts.read_buffer_bytes,
                    write_buf_bytes: self.etl.opts.write_buffer_bytes,
                    inflight_bytes: self.etl.opts.inflight_bytes,
                    ..DedupeCfg::default()
                };
                if cfg.inflight_bytes > 0 {
                    let per_flush_mb = (cfg.inflight_bytes / 2 / (1024 * 1024)).max(1);
                    cfg.min_buf_mb = cfg.min_buf_mb.min(per_flush_mb);
                    cfg.max_buf_mb = cfg.max_buf_mb.min(per_flush_mb.max(cfg.min_buf_mb));
                }

                let runs_dir = tmp_dir.join("runs");
                let runs = build_runs_sorted(&input, &runs_dir, key, &cfg)?;
                let mut unique_count = 0_u64;
                merge_runs_sorted(&runs, out_path, key, &cfg, |current_key, _group, w| {
                    w.write_all(current_key.as_bytes())?;
                    w.write_all(b"\n")?;
                    unique_count += 1;
                    Ok(())
                })?;

                let matched_records = matched_records.load(Ordering::Relaxed);
                if matched_records > 0 && unique_count == 0 {
                    warn_dedupe_zero_keys(key, matched_records);
                }

                Ok(unique_count)
            })();

            let _ = fs::remove_dir_all(&tmp_dir);
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
        )
    }

    /// Spool per-month outputs for later parent attachment + aggregation.
    /// Writes **separate** files per source to avoid clobbering:
    ///   - comments → `part_RC_YYYY-MM.jsonl`
    ///   - submissions → `part_RS_YYYY-MM.jsonl`
    ///
    /// Each month is staged as `<out_dir>/_staging/<file>.inprogress` then
    /// atomically renamed onto the final path so a crashed run never publishes
    /// a partial file. On entry, leftover `*.inprogress` files from a prior
    /// crashed run are swept.
    ///
    /// Returns `(vector_of_paths, total_records_written)`.
    pub fn extract_spool_monthly(self, out_dir: &Path) -> Result<(Vec<PathBuf>, u64)> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        validate_export_whitelist(&plan.etl)?;
        let parallelism = plan.etl.opts.parallelism;

        with_thread_pool(parallelism, || {
            fs::create_dir_all(out_dir)?;
            let staging_dir = ensure_staging_dir(out_dir)?;
            sweep_stale_inprogress(out_dir, true)?;

            let targets =
                resolve_target_subs_from(&plan.etl.opts.subreddit, &plan.query.subreddits);
            let files = plan_pipeline_files(&plan.etl)?;

            let resume = plan.etl.opts.resume;
            let resume_fingerprint = build_resume_fingerprint(&plan.etl, &plan.query, "spool")?;
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
                        strict_whitelist: plan.etl.opts.strict_whitelist,
                        resume,
                        completed_keys: &completed_keys,
                    };
                    let outcome = process_month(job, &ctx)?;

                    if let Some(month) = outcome {
                        total_written.fetch_add(month.lines, Ordering::Relaxed);
                        parts.lock().unwrap().push(month.out_path.clone());
                        if let Some(acc) = &accumulator {
                            if let Err(e) = commit_entry_to_manifest(acc, month) {
                                tracing::warn!(error=%e, "failed to update progress manifest; continuing");
                            }
                        }
                    }
                    Ok(())
                },
            )?;

            if let Some(pb) = pb {
                pb.finish_with_message("done");
            }

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

            scan_records(
                &plan.etl,
                &plan.query,
                /*show_progress=*/ false,
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

            let shards = kv.reduce_sum("author_counts")?;
            concat_tsvs(&shards, out_path, plan.etl.opts.write_buffer_bytes)?;
            Ok(())
        })
    }

    pub fn build_first_seen_index_to_tsv(self, out_path: &Path) -> Result<()> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        let parallelism = plan.etl.opts.parallelism;
        with_thread_pool(parallelism, || {
            let work_dir = plan.etl.ensure_work_dir()?;
            let kv = ShardedKVWriter::create(&work_dir, "first_seen", plan.etl.opts.shard_count)?;

            scan_records(
                &plan.etl,
                &plan.query,
                /*show_progress=*/ false,
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

            let shards = kv.reduce_min("first_seen")?;
            concat_tsvs(&shards, out_path, plan.etl.opts.write_buffer_bytes)?;
            Ok(())
        })
    }

    /// Export corpus back to partitioned JSONL or ZST by month/kinds with query filters.
    /// This lives on ScanPlan (advanced query mode).
    ///
    /// Each output is staged as `<out_base_dir>/_staging/<file>.inprogress`,
    /// finalized (zstd frame closed with checksum, or buffer flushed) and
    /// atomically renamed onto its final path. Stale `*.inprogress` from a
    /// crashed prior run are swept on entry.
    pub fn export_partitioned(self, out_base_dir: &Path, format: ExportFormat) -> Result<()> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        validate_export_whitelist(&plan.etl)?;
        let targets = resolve_target_subs_from(&plan.etl.opts.subreddit, &plan.query.subreddits);
        let parallelism = plan.etl.opts.parallelism;

        with_thread_pool(parallelism, || {
            let files = plan_pipeline_files(&plan.etl)?;

            let comments_dir = out_base_dir.join("comments");
            let submissions_dir = out_base_dir.join("submissions");
            fs::create_dir_all(&comments_dir)?;
            fs::create_dir_all(&submissions_dir)?;
            let staging_dir = ensure_staging_dir(out_base_dir)?;
            sweep_stale_inprogress(out_base_dir, true)?;

            let whitelist = plan.etl.opts.whitelist_fields.clone();
            let whitelist_tracker = whitelist
                .as_ref()
                .map(|_| Arc::new(WhitelistMatchTracker::new(plan.etl.opts.strict_whitelist)));
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
                    let (prefix, out_dir) = match job.kind {
                        FileKind::Comment => ("RC", &comments_dir),
                        FileKind::Submission => ("RS", &submissions_dir),
                    };
                    let file_name = match format {
                        ExportFormat::Jsonl => format!("{}_{}.jsonl", prefix, job.ym),
                        ExportFormat::Zst => format!("{}_{}.zst", prefix, job.ym),
                    };
                    let out_path = out_dir.join(file_name);

                    let written_result = match format {
                        ExportFormat::Jsonl => {
                            write_jsonl_atomic(&staging_dir, &out_path, write_buf, |w| {
                                let result = stream_job(
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
                                )?;
                                complete_stream_job(job, result)
                            })
                        }
                        ExportFormat::Zst => {
                            write_zst_atomic(&staging_dir, &out_path, zst_level, write_buf, |w| {
                                let result = stream_job(
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
                                )?;
                                complete_stream_job(job, result)
                            })
                        }
                    };

                    let written = match written_result {
                        Ok(n) => n,
                        Err(e) if is_partial_scan_error(&e) => {
                            tracing::warn!(path=%job.path.display(), error=%e, "Skipping partitioned export month after zstd decode error; staged output was discarded");
                            return Ok(());
                        }
                        Err(e) => return Err(e),
                    };

                    if written == 0 {
                        let _ = fs::remove_file(&out_path);
                    }
                    Ok(())
                },
            )?;

            if let Some(pb) = pb {
                pb.finish_with_message("done");
            }
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
    let discovered = discover_all(&etl.opts.comments_dir, &etl.opts.submissions_dir);
    Ok(plan_files_checked(
        &discovered,
        &etl.opts.comments_dir,
        &etl.opts.submissions_dir,
        etl.opts.sources,
        etl.opts.start,
        etl.opts.end,
    )?)
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
fn build_resume_fingerprint(etl: &RedditETL, query: &QuerySpec, operation: &str) -> Result<String> {
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
        "query": {
            "subreddits": query.subreddits.as_ref(),
            "authors_in": query.authors_in.as_ref(),
            "authors_out": query.authors_out.as_ref(),
            "author_regex": query.author_regex.as_ref().map(|re| re.as_str()),
            "author_regex_pattern": query.author_regex_pattern.as_ref(),
            "min_score": query.min_score,
            "max_score": query.max_score,
            "keywords_any": query.keywords_any.as_ref(),
            "domains_in": query.domains_in.as_ref(),
            "contains_url": query.contains_url,
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
    for entry in fs::read_dir(dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let entry = entry?;
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
    bounds: Option<(YearMonth, YearMonth)>,
    read_buf: usize,
    write_buf: usize,
    human_ts: bool,
    whitelist_tracker: Option<&'a WhitelistMatchTracker>,
    strict_whitelist: bool,
    resume: bool,
    completed_keys: &'a HashSet<String>,
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
/// `write_jsonl_atomic`. Returns `Ok(None)` on resume-skip or transient atomic
/// write failure (already logged); `Ok(Some(MonthResult))` on a successful
/// publish whose entry the caller should commit to the manifest.
fn process_month(job: &FileJob, ctx: &MonthJobCtx<'_>) -> Result<Option<MonthResult>> {
    let (file_prefix, key_prefix) = match job.kind {
        FileKind::Comment => (FILE_PREFIX_RC, "RC"),
        FileKind::Submission => (FILE_PREFIX_RS, "RS"),
    };
    let out_path = ctx
        .out_dir
        .join(format!("{}_{}.jsonl", file_prefix, job.ym));
    let key = crate::progress_manifest::month_key(key_prefix, job.ym);

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
        let result = stream_job(
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
        )?;
        complete_stream_job(job, result)
    }) {
        Ok(n) => n,
        Err(e) if is_partial_scan_error(&e) => {
            tracing::warn!(path=%job.path.display(), output=%out_path.display(), error=%e, "Skipping month after zstd decode error; staged spool output was discarded and resume will retry it");
            return Ok(None);
        }
        Err(e) => {
            if ctx.strict_whitelist
                && e.chain().any(|cause| {
                    cause
                        .to_string()
                        .starts_with("--whitelist matched zero fields")
                })
            {
                return Err(e);
            }
            tracing::warn!(path=%out_path.display(), error=%e, "Skipping month: failed to atomically write spool output");
            if let Some(pb) = ctx.pb {
                let sz = fs::metadata(&job.path).map(|m| m.len()).unwrap_or(0);
                pb.inc(sz);
            }
            return Ok(None);
        }
    };

    Ok(Some(MonthResult {
        out_path,
        key,
        lines: n,
    }))
}

/// Atomically commit a month's manifest entry after a successful publish.
/// `write_jsonl_atomic` only returns Ok once the rename landed, so the size
/// stat is taken from the published path. Best-effort: a manifest rewrite
/// failure must not abort the run — at worst, a subsequent re-run redoes
/// this month.
fn commit_entry_to_manifest(acc: &ManifestAccumulator, result: MonthResult) -> Result<()> {
    let size = fs::metadata(&result.out_path).map(|m| m.len()).unwrap_or(0);
    let entry = MonthEntry {
        size,
        lines: result.lines,
        sha256: None,
    };
    acc.commit(result.key, entry)
}

fn export_part_key(job: &FileJob) -> String {
    let prefix = match job.kind {
        FileKind::Comment => "RC",
        FileKind::Submission => "RS",
    };
    crate::progress_manifest::month_key(prefix, job.ym)
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
    let file = fs::File::open(path)
        .with_context(|| format!("opening extract resume part {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut buf = String::new();
    let mut lines = 0_u64;
    loop {
        buf.clear();
        let n = reader.read_line(&mut buf)?;
        if n == 0 {
            break;
        }
        let line = buf.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            continue;
        }
        let _: serde_json::Value = serde_json::from_str(line)
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
) -> Result<()> {
    validate_export_whitelist(etl)?;
    let parallelism = etl.opts.parallelism;
    with_thread_pool(parallelism, || {
        let files = plan_pipeline_files(etl)?;

        let work_dir = etl.ensure_work_dir()?;
        let resume = etl.opts.resume;
        let resume_fingerprint = if resume {
            Some(build_resume_fingerprint(etl, query, "extract")?)
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
        fs::create_dir_all(&tmp_dir)?;
        let staging_dir = ensure_staging_dir(&tmp_dir)?;
        sweep_stale_inprogress(&tmp_dir, true)?;

        let (initial_months, completed_keys) =
            if let Some(fingerprint) = resume_fingerprint.as_deref() {
                load_and_validate_export_manifest(&tmp_dir, fingerprint)?
            } else {
                (HashMap::new(), HashSet::new())
            };
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
                                if let Err(e) = acc.commit(key.clone(), entry) {
                                    tracing::warn!(error=%e, key=%key, "failed to update extract progress manifest; continuing");
                                }
                            }
                            if let Some(pb) = &pb {
                                let sz = fs::metadata(&job.path).map(|m| m.len()).unwrap_or(0);
                                pb.inc(sz);
                            }
                            return Ok(());
                        }
                        Err(e) => {
                            tracing::info!(path=%tmp_file.display(), error=%e, "resume part failed validation; rebuilding");
                            let _ = fs::remove_file(&tmp_file);
                        }
                    }
                }

                let lines = match write_jsonl_atomic(&staging_dir, &tmp_file, write_buf, |w| {
                    let result = stream_job(
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
                    )?;
                    complete_stream_job(job, result)
                }) {
                    Ok(lines) => lines,
                    Err(e) if is_partial_scan_error(&e) => {
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
                    if let Err(e) = acc.commit(key, entry) {
                        tracing::warn!(error=%e, "failed to update extract progress manifest; continuing");
                    }
                }
                Ok(())
            },
        )?;

        if let Some(pb) = pb {
            pb.finish_with_message("done");
        }

        match finalize {
            Finalize::Jsonl => stitch_tmp_parts(&tmp_dir, out_path, write_buf)?,
            Finalize::JsonArray { pretty } => {
                stitch_tmp_parts_to_json_array(&tmp_dir, out_path, pretty, write_buf)?
            }
        }
        if !resume {
            if let Err(e) = fs::remove_dir_all(&tmp_dir) {
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
    on_record: F,
) -> Result<()>
where
    F: Sync + Send + Fn(&MinimalRecord, FileKind, &str) -> Result<()>,
{
    let targets = resolve_target_subs_from(&etl.opts.subreddit, &query.subreddits);
    let targets_ref = targets.as_ref();
    let bounds = bounds_tuple(etl.opts.start, etl.opts.end);
    let read_buf = etl.opts.read_buffer_bytes;

    let files = plan_pipeline_files(etl)?;

    let pb = if show_progress && etl.opts.progress {
        let total_bytes = total_compressed_size(&files);
        Some(make_progress_bar_labeled(
            total_bytes,
            etl.opts.progress_label.as_deref(),
        ))
    } else {
        None
    };

    crate::concurrency::for_each_file_limited(
        &files,
        etl.opts.file_concurrency,
        |job| -> Result<()> {
            let kind = job.kind;
            let line_cb = |line: &str| -> Result<()> {
                if let Ok(min) = parse_minimal(line) {
                    if !matches_minimal(&min, targets_ref, query) {
                        return Ok(());
                    }
                    if !within_bounds(&min, bounds) {
                        return Ok(());
                    }
                    if query.requires_full_parse() {
                        let val: serde_json::Value = serde_json::from_str(line)?;
                        if !matches_full(&val, kind, query) {
                            return Ok(());
                        }
                    }
                    on_record(&min, kind, line)?;
                }
                Ok(())
            };
            if let Some(pb) = &pb {
                for_each_line_with_progress_cfg(&job.path, read_buf, |delta| pb.inc(delta), line_cb)
            } else {
                for_each_line_cfg(&job.path, read_buf, line_cb)
            }
        },
    )?;

    if let Some(pb) = pb {
        pb.finish_with_message("done");
    }
    Ok(())
}
