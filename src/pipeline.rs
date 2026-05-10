use crate::atomic_write::{ensure_staging_dir, sweep_stale_inprogress, write_jsonl_atomic, write_zst_atomic};
use crate::config::{ETLOptions, Sources};
use crate::date::YearMonth;
use crate::filters::{bounds_tuple, resolve_target_subs_from, matches_minimal, matches_full, within_bounds, ym_from_epoch};
use crate::paths::{discover_all, plan_files, FileJob, FileKind};
use crate::progress::{make_progress_bar_labeled, total_compressed_size};
use crate::progress_manifest::{ManifestAccumulator, MonthEntry};
use crate::query::{normalize_str, QuerySpec};
use crate::shard::{ShardedWriter, UsernameStream};
use crate::stitch::{concat_tsvs, stitch_tmp_parts, stitch_tmp_parts_to_json_array, tmp_name_for_job};
use crate::streaming::{process_file_for_usernames_with_skip, stream_job};
use crate::util::{create_with_backoff, default_bot_authors, merge_extra_exclusions, with_thread_pool};
use crate::zstd_jsonl::{for_each_line_cfg, for_each_line_with_progress_cfg, parse_minimal, MinimalRecord};
use crate::kv_shard::ShardedKVWriter;
use anyhow::{anyhow, Context, Result};
use indicatif::ProgressBar;
use regex::Regex;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

#[derive(Clone)]
pub struct RedditETL {
    pub(crate) opts: ETLOptions,
}

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

impl RedditETL {
    pub fn new() -> Self {
        Self { opts: ETLOptions::default() }
    }

    // -------- Builder methods --------
    pub fn base_dir(mut self, base: impl AsRef<std::path::Path>) -> Self { self.opts = self.opts.with_base_dir(base); self }
    pub fn subreddit(mut self, sub: impl AsRef<str>) -> Self { self.opts = self.opts.with_subreddit(sub); self }
    pub fn sources(mut self, sources: Sources) -> Self { self.opts = self.opts.with_sources(sources); self }
    pub fn date_range(mut self, start: Option<YearMonth>, end: Option<YearMonth>) -> Self { self.opts = self.opts.with_date_range(start, end); self }
    pub fn whitelist_fields<I, S>(mut self, fields: I) -> Self where I: IntoIterator<Item = S>, S: Into<String> { self.opts = self.opts.with_whitelist_fields(fields); self }
    pub fn parallelism(mut self, threads: usize) -> Self { self.opts = self.opts.with_parallelism(threads); self }
    pub fn work_dir(mut self, dir: impl AsRef<Path>) -> Self { self.opts = self.opts.with_work_dir(dir); self }
    pub fn shard_count(mut self, count: usize) -> Self { self.opts = self.opts.with_shard_count(count); self }
    pub fn file_concurrency(mut self, n: usize) -> Self { self.opts = self.opts.with_file_concurrency(n); self }
    pub fn progress(mut self, yes: bool) -> Self { self.opts = self.opts.with_progress(yes); self }
    pub fn progress_label(mut self, label: impl Into<String>) -> Self { self.opts = self.opts.with_progress_label(label); self }
    pub fn io_read_buffer(mut self, bytes: usize) -> Self { self.opts = self.opts.with_io_read_buffer(bytes); self }
    pub fn io_write_buffer(mut self, bytes: usize) -> Self { self.opts = self.opts.with_io_write_buffer(bytes); self }
    pub fn io_buffers(mut self, read_bytes: usize, write_bytes: usize) -> Self { self.opts = self.opts.with_io_buffers(read_bytes, write_bytes); self }
    pub fn timestamps_human_readable(mut self, yes: bool) -> Self { self.opts = self.opts.with_human_timestamps(yes); self }
    pub fn zst_level(mut self, level: i32) -> Self { self.opts = self.opts.with_zst_level(level); self }
    /// Override the inflight-bytes backpressure budget used by bucketing/dedupe
    /// producer/consumer pairs. See `ETLOptions::inflight_bytes`.
    pub fn inflight_bytes(mut self, bytes: usize) -> Self { self.opts = self.opts.with_inflight_bytes(bytes); self }
    /// Opt in to resumable spool runs: when enabled, `extract_spool_monthly`
    /// reads and writes a `<out_dir>/_progress.json` sidecar so a re-run skips
    /// months already published by a previous (possibly crashed) invocation.
    /// Default false to preserve existing behavior.
    pub fn resume(mut self, yes: bool) -> Self { self.opts = self.opts.with_resume(yes); self }

    // -------- Original operations (back-compat) --------

    pub fn usernames(self) -> Result<UsernameStream> {
        let subreddit = self.opts.subreddit.clone().ok_or_else(|| anyhow!("subreddit is required"))?;
        let parallelism = self.opts.parallelism;

        with_thread_pool(parallelism, || {
            let work_dir = self.ensure_work_dir()?;
            let discovered = discover_all(&self.opts.comments_dir, &self.opts.submissions_dir);
            let files = plan_files(&discovered, self.opts.sources, self.opts.start, self.opts.end);

            if files.is_empty() {
                tracing::warn!("No files found matching selection. Check base_dir and date range.");
            } else {
                tracing::info!("Planned {} files for processing.", files.len());
            }

            let shard_writer = ShardedWriter::create(&work_dir, "usernames", self.opts.shard_count)?;
            let read_buf = self.opts.read_buffer_bytes;

            let total_bytes = total_compressed_size(&files);
            let pb = if self.opts.progress {
                Some(make_progress_bar_labeled(total_bytes, self.opts.progress_label.as_deref()))
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
                    move |_path, _err| { skip_count_per_call.fetch_add(1, Ordering::Relaxed); },
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

    // -------- Advanced: enter query mode --------
    pub fn scan(self) -> ScanPlan {
        ScanPlan { etl: self, query: QuerySpec { filter_pseudo_users: true, ..Default::default() }.normalize() }
    }

    pub(crate) fn ensure_work_dir(&self) -> Result<PathBuf> {
        let dir = self.opts.work_dir.clone().unwrap_or_else(|| self.opts.base_dir.join(".reddit_etl_work"));
        fs::create_dir_all(&dir)?; Ok(dir)
    }
}

// ----------------- Advanced ScanPlan -----------------

pub struct ScanPlan { etl: RedditETL, query: QuerySpec }

impl ScanPlan {
    /// Common implementation for setters that map an iterator of strings into
    /// an `Option<Vec<String>>` field on the [`QuerySpec`], then renormalize.
    /// `set_field` writes the collected list onto the chosen field; `norm` is
    /// applied per-element before collection (e.g. `normalize_str`, lowercase).
    fn set_string_list<I, S>(
        mut self,
        set_field: impl FnOnce(&mut QuerySpec, Vec<String>),
        iter: I,
        norm: fn(&str) -> String,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let v: Vec<String> = iter.into_iter().map(|s| norm(s.as_ref())).collect();
        set_field(&mut self.query, v);
        self.query = self.query.normalize();
        self
    }

    pub fn subreddit(mut self, s: impl AsRef<str>) -> Self { self.query.subreddits = Some(vec![normalize_str(s.as_ref())]); self }
    pub fn subreddits<I, S>(self, iter: I) -> Self where I: IntoIterator<Item = S>, S: AsRef<str> {
        self.set_string_list(|q, v| q.subreddits = Some(v), iter, normalize_str)
    }
    pub fn author(mut self, author: impl AsRef<str>) -> Self { self.query.authors_in = Some(vec![normalize_str(author.as_ref())]); self.query = self.query.normalize(); self }
    pub fn authors<I, S>(self, iter: I) -> Self where I: IntoIterator<Item = S>, S: AsRef<str> {
        self.set_string_list(|q, v| q.authors_in = Some(v), iter, normalize_str)
    }
    pub fn authors_in<I, S>(self, iter: I) -> Self where I: IntoIterator<Item = S>, S: AsRef<str> {
        self.set_string_list(|q, v| q.authors_in = Some(v), iter, normalize_str)
    }
    pub fn authors_out<I, S>(self, iter: I) -> Self where I: IntoIterator<Item = S>, S: AsRef<str> {
        self.set_string_list(|q, v| q.authors_out = Some(v), iter, normalize_str)
    }
    /// Alias for authors_out: exclude the provided authors (normalized).
    pub fn exclude_authors<I, S>(self, iter: I) -> Self where I: IntoIterator<Item = S>, S: AsRef<str> {
        self.authors_out(iter)
    }
    /// Convenience: exclude a default set of bot/service accounts, plus any env/file augments.
    pub fn exclude_common_bots(mut self) -> Self {
        let mut v = default_bot_authors();
        merge_extra_exclusions(&mut v);
        self.query.authors_out = Some(v);
        self.query = self.query.normalize();
        self
    }
    pub fn author_regex(mut self, re: Regex) -> Self { self.query.author_regex = Some(re); self }
    pub fn min_score(mut self, v: i64) -> Self { self.query.min_score = Some(v); self }
    pub fn max_score(mut self, v: i64) -> Self { self.query.max_score = Some(v); self }
    pub fn keywords_any<I, S>(self, iter: I) -> Self where I: IntoIterator<Item = S>, S: AsRef<str> {
        self.set_string_list(|q, v| q.keywords_any = Some(v), iter, lowercase_str)
    }
    pub fn domains_in<I, S>(self, iter: I) -> Self where I: IntoIterator<Item = S>, S: AsRef<str> {
        self.set_string_list(|q, v| q.domains_in = Some(v), iter, lowercase_str)
    }
    pub fn contains_url(mut self, yes: bool) -> Self { self.query.contains_url = Some(yes); self }
    pub fn allow_pseudo_users(mut self) -> Self { self.query.filter_pseudo_users = false; self }
    pub fn whitelist_fields<I, S>(mut self, fields: I) -> Self where I: IntoIterator<Item = S>, S: Into<String> {
        // correct builder on RedditETL instance
        self.etl = self.etl.whitelist_fields(fields); self
    }

    pub fn usernames(self) -> Result<UsernameStream> {
        let parallelism = self.etl.opts.parallelism;
        with_thread_pool(parallelism, || {
            let work_dir = self.etl.ensure_work_dir()?;
            let shard_writer = ShardedWriter::create(&work_dir, "usernames_q", self.etl.opts.shard_count)?;

            scan_records(&self.etl, &self.query, /*show_progress=*/true, |min, _kind, _line| {
                if let Some(a) = min.author.as_deref() {
                    let a = a.trim();
                    if a.is_empty() { return Ok(()); }
                    shard_writer.write(a)?;
                }
                Ok(())
            })?;

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
        let targets = resolve_target_subs_from(&self.etl.opts.subreddit, &self.query.subreddits);
        extract_common(&self.etl, &self.query, targets.as_ref(), "extract_jsonl_q_tmp", out_path, Finalize::Jsonl)
    }

    pub fn extract_to_json(self, out_path: &Path, pretty: bool) -> Result<()> {
        let targets = resolve_target_subs_from(&self.etl.opts.subreddit, &self.query.subreddits);
        extract_common(&self.etl, &self.query, targets.as_ref(), "extract_json_q_tmp", out_path, Finalize::JsonArray { pretty })
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
        let parallelism = self.etl.opts.parallelism;

        with_thread_pool(parallelism, || {
            fs::create_dir_all(out_dir)?;
            let staging_dir = ensure_staging_dir(out_dir)?;
            sweep_stale_inprogress(out_dir, true)?;

            let targets = resolve_target_subs_from(&self.etl.opts.subreddit, &self.query.subreddits);
            let discovered = discover_all(&self.etl.opts.comments_dir, &self.etl.opts.submissions_dir);
            let files = plan_files(&discovered, self.etl.opts.sources, self.etl.opts.start, self.etl.opts.end);

            if files.is_empty() {
                return Ok((Vec::new(), 0));
            }

            let resume = self.etl.opts.resume;
            // Resume manifest: load on entry, validate each entry against the
            // file actually on disk, and snapshot the surviving keys before any
            // worker mutates the accumulator.
            let (initial_months, completed_keys) = if resume {
                load_and_validate_manifest(out_dir)?
            } else {
                (HashMap::new(), HashSet::new())
            };

            let total_bytes = total_compressed_size(&files);
            let pb = if self.etl.opts.progress {
                Some(make_progress_bar_labeled(total_bytes, self.etl.opts.progress_label.as_deref()))
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
                crate::progress_manifest::save(out_dir, &initial_months)?;
                Some(ManifestAccumulator::new(out_dir, initial_months))
            } else {
                None
            };

            let whitelist = self.etl.opts.whitelist_fields.clone();
            let targets_ref = targets.as_ref();
            let bounds = bounds_tuple(self.etl.opts.start, self.etl.opts.end);
            let read_buf = self.etl.opts.read_buffer_bytes;
            let write_buf = self.etl.opts.write_buffer_bytes;
            let human_ts = self.etl.opts.human_readable_timestamps;

            crate::concurrency::for_each_file_limited(&files, self.etl.opts.file_concurrency, |job| -> Result<()> {
                let outcome = process_month(
                    job,
                    out_dir,
                    &staging_dir,
                    targets_ref,
                    &self.query,
                    &whitelist,
                    pb.as_ref(),
                    bounds,
                    read_buf,
                    write_buf,
                    human_ts,
                    resume,
                    &completed_keys,
                )?;

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
            })?;

            if let Some(pb) = pb { pb.finish_with_message("done"); }

            let mut list = parts.into_inner().unwrap();
            list.sort();
            list.dedup();
            Ok((list, total_written.load(Ordering::Relaxed)))
        })
    }

    pub fn count_by_month(self) -> Result<BTreeMap<YearMonth, u64>> {
        let total = Mutex::new(BTreeMap::<YearMonth, u64>::new());
        scan_records(&self.etl, &self.query, /*show_progress=*/false, |min, _kind, _line| {
            if let Some(ts) = min.created_utc {
                let ym = ym_from_epoch(ts);
                *total.lock().unwrap().entry(ym).or_insert(0) += 1;
            }
            Ok(())
        })?;
        Ok(total.into_inner().unwrap())
    }

    pub fn author_counts_to_tsv(self, out_path: &Path) -> Result<()> {
        let parallelism = self.etl.opts.parallelism;
        with_thread_pool(parallelism, || {
            let work_dir = self.etl.ensure_work_dir()?;
            let kv = ShardedKVWriter::create(&work_dir, "author_counts", self.etl.opts.shard_count)?;

            scan_records(&self.etl, &self.query, /*show_progress=*/false, |min, _kind, _line| {
                if let Some(a) = min.author.as_deref() {
                    let a = a.trim();
                    if a.is_empty() { return Ok(()); }
                    kv.write_kv(a, 1)?;
                }
                Ok(())
            })?;

            let shards = kv.reduce_sum("author_counts")?;
            concat_tsvs(&shards, out_path, self.etl.opts.write_buffer_bytes)?;
            Ok(())
        })
    }

    pub fn build_first_seen_index_to_tsv(self, out_path: &Path) -> Result<()> {
        let work_dir = self.etl.ensure_work_dir()?;
        let kv = ShardedKVWriter::create(&work_dir, "first_seen", self.etl.opts.shard_count)?;

        scan_records(&self.etl, &self.query, /*show_progress=*/false, |min, _kind, _line| {
            if let (Some(a), Some(ts)) = (min.author.as_deref(), min.created_utc) {
                let a = a.trim();
                if a.is_empty() { return Ok(()); }
                kv.write_kv(a, ts)?;
            }
            Ok(())
        })?;

        let shards = kv.reduce_min("first_seen")?;
        concat_tsvs(&shards, out_path, self.etl.opts.write_buffer_bytes)?;
        Ok(())
    }

    /// Export corpus back to partitioned JSONL or ZST by month/kinds with query filters.
    /// This lives on ScanPlan (advanced query mode).
    ///
    /// Each output is staged as `<out_base_dir>/_staging/<file>.inprogress`,
    /// finalized (zstd frame closed with checksum, or buffer flushed) and
    /// atomically renamed onto its final path. Stale `*.inprogress` from a
    /// crashed prior run are swept on entry.
    pub fn export_partitioned(self, out_base_dir: &Path, format: ExportFormat) -> Result<()> {
        let targets = resolve_target_subs_from(&self.etl.opts.subreddit, &self.query.subreddits);
        let parallelism = self.etl.opts.parallelism;

        with_thread_pool(parallelism, || {
        let discovered = discover_all(&self.etl.opts.comments_dir, &self.etl.opts.submissions_dir);
        let files = plan_files(&discovered, self.etl.opts.sources, self.etl.opts.start, self.etl.opts.end);

        let comments_dir = out_base_dir.join("comments");
        let submissions_dir = out_base_dir.join("submissions");
        fs::create_dir_all(&comments_dir)?;
        fs::create_dir_all(&submissions_dir)?;
        let staging_dir = ensure_staging_dir(out_base_dir)?;
        sweep_stale_inprogress(out_base_dir, true)?;

        let whitelist = self.etl.opts.whitelist_fields.clone();
        let targets_ref = targets.as_ref();
        let bounds = bounds_tuple(self.etl.opts.start, self.etl.opts.end);
        let total_bytes = total_compressed_size(&files);
        let pb = if self.etl.opts.progress {
            Some(make_progress_bar_labeled(total_bytes, self.etl.opts.progress_label.as_deref()))
        } else {
            None
        };
        let read_buf = self.etl.opts.read_buffer_bytes;
        let write_buf = self.etl.opts.write_buffer_bytes;
        let human_ts = self.etl.opts.human_readable_timestamps;
        let zst_level = self.etl.opts.zst_level;

        crate::concurrency::for_each_file_limited(&files, self.etl.opts.file_concurrency, |job| -> Result<()> {
            let (prefix, out_dir) = match job.kind {
                FileKind::Comment => ("RC", &comments_dir),
                FileKind::Submission => ("RS", &submissions_dir),
            };
            let file_name = match format {
                ExportFormat::Jsonl => format!("{}_{}.jsonl", prefix, job.ym),
                ExportFormat::Zst   => format!("{}_{}.zst",   prefix, job.ym),
            };
            let out_path = out_dir.join(file_name);

            let written = match format {
                ExportFormat::Jsonl => {
                    write_jsonl_atomic(&staging_dir, &out_path, write_buf, |w| {
                        stream_job(job, w, targets_ref, &self.query, &whitelist, pb.clone(), bounds, read_buf, human_ts)
                    })?
                }
                ExportFormat::Zst => {
                    write_zst_atomic(&staging_dir, &out_path, zst_level, write_buf, |w| {
                        stream_job(job, w, targets_ref, &self.query, &whitelist, pb.clone(), bounds, read_buf, human_ts)
                    })?
                }
            };

            if written == 0 { let _ = fs::remove_file(&out_path); }
            Ok(())
        })?;

        if let Some(pb) = pb { pb.finish_with_message("done"); }
        Ok(())
        })
    }
}

#[inline]
fn lowercase_str(s: &str) -> String {
    s.to_lowercase()
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

    let discovered = discover_all(&etl.opts.comments_dir, &etl.opts.submissions_dir);
    let files = plan_files(&discovered, etl.opts.sources, etl.opts.start, etl.opts.end);

    let pb = if show_progress && etl.opts.progress {
        let total_bytes = total_compressed_size(&files);
        Some(make_progress_bar_labeled(total_bytes, etl.opts.progress_label.as_deref()))
    } else {
        None
    };

    crate::concurrency::for_each_file_limited(&files, etl.opts.file_concurrency, |job| -> Result<()> {
        let kind = job.kind;
        let line_cb = |line: &str| -> Result<()> {
            if let Ok(min) = parse_minimal(line) {
                if !matches_minimal(&min, targets_ref, query) { return Ok(()); }
                if !within_bounds(&min, bounds) { return Ok(()); }
                if query.requires_full_parse() {
                    let val: serde_json::Value = serde_json::from_str(line)?;
                    if !matches_full(&val, kind, query) { return Ok(()); }
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
    })?;

    if let Some(pb) = pb { pb.finish_with_message("done"); }
    Ok(())
}

// ----------------- extract_spool_monthly helpers -----------------

/// Result of a single month's atomic publish — used by the orchestration
/// shell in `extract_spool_monthly` to update both the returned `parts` list
/// and the on-disk progress manifest.
struct MonthResult {
    out_path: PathBuf,
    key: String,
    lines: u64,
}

/// Load `_progress.json`, validate each entry against the on-disk file size,
/// drop mismatches with a `tracing::info` warning, and return the surviving
/// entries plus a snapshot of completed keys for fast skip-lookup.
fn load_and_validate_manifest(
    out_dir: &Path,
) -> Result<(HashMap<String, MonthEntry>, HashSet<String>)> {
    let manifest = crate::progress_manifest::load(out_dir);
    let mut keep: HashMap<String, MonthEntry> = HashMap::new();
    for (k, v) in manifest.months {
        // Determine destination by the key's prefix (RC_ → comments, RS_ → submissions).
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
#[allow(clippy::too_many_arguments)]
fn process_month(
    job: &FileJob,
    out_dir: &Path,
    staging_dir: &Path,
    targets: Option<&Vec<String>>,
    query: &QuerySpec,
    whitelist: &Option<Vec<String>>,
    pb: Option<&ProgressBar>,
    bounds: Option<(YearMonth, YearMonth)>,
    read_buf: usize,
    write_buf: usize,
    human_ts: bool,
    resume: bool,
    completed_keys: &HashSet<String>,
) -> Result<Option<MonthResult>> {
    let (file_prefix, key_prefix) = match job.kind {
        FileKind::Comment => ("part_RC", "RC"),
        FileKind::Submission => ("part_RS", "RS"),
    };
    let out_path = out_dir.join(format!("{}_{}.jsonl", file_prefix, job.ym));
    let key = crate::progress_manifest::month_key(key_prefix, job.ym);

    // Resume fast-path: skip the month entirely if it's already in the
    // manifest (and the on-disk file matched at load time). Still bump the
    // progress bar by the input file's compressed size so the user sees the
    // work accounted for.
    if resume && completed_keys.contains(&key) {
        if let Some(pb) = pb {
            let sz = fs::metadata(&job.path).map(|m| m.len()).unwrap_or(0);
            pb.inc(sz);
        }
        return Ok(None);
    }

    let n = match write_jsonl_atomic(staging_dir, &out_path, write_buf, |w| {
        stream_job(
            job, w, targets, query, whitelist,
            pb.cloned(), bounds, read_buf, human_ts,
        )
    }) {
        Ok(n) => n,
        Err(e) => {
            tracing::warn!(path=%out_path.display(), error=%e, "Skipping month: failed to atomically write spool output");
            if let Some(pb) = pb {
                let sz = fs::metadata(&job.path).map(|m| m.len()).unwrap_or(0);
                pb.inc(sz);
            }
            return Ok(None);
        }
    };

    Ok(Some(MonthResult { out_path, key, lines: n }))
}

/// Atomically commit a month's manifest entry after a successful publish.
/// `write_jsonl_atomic` only returns Ok once the rename landed, so the size
/// stat is taken from the published path. Best-effort: a manifest rewrite
/// failure must not abort the run — at worst, a subsequent re-run redoes
/// this month.
fn commit_entry_to_manifest(acc: &ManifestAccumulator, result: MonthResult) -> Result<()> {
    let size = fs::metadata(&result.out_path).map(|m| m.len()).unwrap_or(0);
    let entry = MonthEntry { size, lines: result.lines, sha256: None };
    acc.commit(result.key, entry)
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
    let parallelism = etl.opts.parallelism;
    with_thread_pool(parallelism, || {
    let discovered = discover_all(&etl.opts.comments_dir, &etl.opts.submissions_dir);
    let files = plan_files(&discovered, etl.opts.sources, etl.opts.start, etl.opts.end);

    match finalize {
        Finalize::Jsonl => {
            if files.is_empty() {
                tracing::warn!("No files found matching selection. Nothing to extract.");
                return Ok(());
            }
        }
        Finalize::JsonArray { .. } => {
            if files.is_empty() {
                let mut w = BufWriter::with_capacity(etl.opts.write_buffer_bytes, create_with_backoff(out_path, 16, 50)?);
                w.write_all(b"[]")?;
                w.flush()?;
                return Ok(());
            }
        }
    }

    let work_dir = etl.ensure_work_dir()?;
    let tmp_dir = work_dir.join(tmp_dir_name);
    fs::create_dir_all(&tmp_dir)?;
    let whitelist = etl.opts.whitelist_fields.clone();

    let total_bytes = total_compressed_size(&files);
    let pb = if etl.opts.progress {
        Some(make_progress_bar_labeled(total_bytes, etl.opts.progress_label.as_deref()))
    } else {
        None
    };

    let targets_ref = targets;
    let bounds = bounds_tuple(etl.opts.start, etl.opts.end);
    let read_buf = etl.opts.read_buffer_bytes;
    let write_buf = etl.opts.write_buffer_bytes;
    let human_ts = etl.opts.human_readable_timestamps;

    crate::concurrency::for_each_file_limited(&files, etl.opts.file_concurrency, |job| -> Result<()> {
        let tmp_file = tmp_dir.join(tmp_name_for_job(job));
        let file = create_with_backoff(&tmp_file, 16, 50)
            .with_context(|| format!("create {}", tmp_file.display()))?;
        let mut writer = BufWriter::with_capacity(write_buf, file);

        stream_job(job, &mut writer, targets_ref, query, &whitelist, pb.clone(), bounds, read_buf, human_ts)?;
        writer.flush()?;
        Ok(())
    })?;

    if let Some(pb) = pb { pb.finish_with_message("done"); }

    match finalize {
        Finalize::Jsonl => stitch_tmp_parts(&tmp_dir, out_path, write_buf)?,
        Finalize::JsonArray { pretty } => stitch_tmp_parts_to_json_array(&tmp_dir, out_path, pretty, write_buf)?,
    }
    Ok(())
    })
}
