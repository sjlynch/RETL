use crate::config::{ETLOptions, Sources};
use crate::date::YearMonth;
use crate::filters::{bounds_tuple, resolve_target_subs_from, matches_minimal, matches_full, within_bounds};
use crate::paths::{discover_all, plan_files};
use crate::progress::{make_progress_bar_labeled, total_compressed_size};
use crate::query::{normalize_str, QuerySpec};
use crate::shard::{ShardedWriter, UsernameStream};
use crate::stitch::{concat_tsvs, stitch_tmp_parts, stitch_tmp_parts_to_json_array, tmp_name_for_job};
use crate::streaming::{process_file_for_usernames, stream_job};
use crate::util::{basic_query_allow_all, init_tracing_once, create_with_backoff, default_bot_authors, merge_extra_exclusions};
use crate::zstd_jsonl::{for_each_line_cfg, for_each_line_with_progress_cfg, parse_minimal};
use crate::kv_shard::ShardedKVWriter;
use anyhow::{anyhow, Context, Result};
use rayon::prelude::*;
use regex::Regex;
use std::collections::BTreeMap;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use zstd::stream::write::Encoder as ZstdEncoder;

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

    // -------- Original operations (back-compat) --------

    pub fn usernames(self) -> Result<UsernameStream> {
        let subreddit = self.opts.subreddit.clone().ok_or_else(|| anyhow!("subreddit is required"))?;
        init_tracing_once();
        if let Some(n) = self.opts.parallelism { if n > 0 { rayon::ThreadPoolBuilder::new().num_threads(n).build_global().ok(); } }

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

        crate::concurrency::for_each_file_limited(&files, self.opts.file_concurrency, |job| {
            process_file_for_usernames(job, read_buf, &subreddit, &shard_writer, pb.clone())
                .with_context(|| format!("processing {}", job.path.display()))
        })?;

        let deduped_files = shard_writer.dedup("usernames")?;
        UsernameStream::from_deduped_files(deduped_files)
    }

    /// Back-compat: extract with a single subreddit (no advanced query).
    pub fn extract_to_jsonl(self, out_path: &Path) -> Result<()> {
        let subreddit = self.opts.subreddit.clone().ok_or_else(|| anyhow!("subreddit is required"))?;
        let target_vec = vec![subreddit];
        let targets = Some(&target_vec);
        let q = basic_query_allow_all();
        extract_common(&self, &q, targets, "extract_jsonl_tmp", out_path, Finalize::Jsonl)
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
    pub fn subreddit(mut self, s: impl AsRef<str>) -> Self { self.query.subreddits = Some(vec![normalize_str(s.as_ref())]); self }
    pub fn subreddits<I, S>(mut self, iter: I) -> Self where I: IntoIterator<Item = S>, S: AsRef<str> {
        self.query.subreddits = Some(iter.into_iter().map(|s| normalize_str(s.as_ref())).collect()); self.query = self.query.normalize(); self
    }
    pub fn author(mut self, author: impl AsRef<str>) -> Self { self.query.authors_in = Some(vec![normalize_str(author.as_ref())]); self.query = self.query.normalize(); self }
    pub fn authors<I, S>(mut self, iter: I) -> Self where I: IntoIterator<Item = S>, S: AsRef<str> {
        self.query.authors_in = Some(iter.into_iter().map(|s| normalize_str(s.as_ref())).collect()); self.query = self.query.normalize(); self
    }
    pub fn authors_in<I, S>(mut self, iter: I) -> Self where I: IntoIterator<Item = S>, S: AsRef<str> {
        self.query.authors_in = Some(iter.into_iter().map(|s| normalize_str(s.as_ref())).collect()); self.query = self.query.normalize(); self
    }
    pub fn authors_out<I, S>(mut self, iter: I) -> Self where I: IntoIterator<Item = S>, S: AsRef<str> {
        self.query.authors_out = Some(iter.into_iter().map(|s| normalize_str(s.as_ref())).collect()); self.query = self.query.normalize(); self
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
    pub fn keywords_any<I, S>(mut self, iter: I) -> Self where I: IntoIterator<Item = S>, S: AsRef<str> {
        self.query.keywords_any = Some(iter.into_iter().map(|s| s.as_ref().to_lowercase()).collect()); self.query = self.query.normalize(); self
    }
    pub fn domains_in<I, S>(mut self, iter: I) -> Self where I: IntoIterator<Item = S>, S: AsRef<str> {
        self.query.domains_in = Some(iter.into_iter().map(|s| s.as_ref().to_lowercase()).collect()); self.query = self.query.normalize(); self
    }
    pub fn contains_url(mut self, yes: bool) -> Self { self.query.contains_url = Some(yes); self }
    pub fn allow_pseudo_users(mut self) -> Self { self.query.filter_pseudo_users = false; self }
    pub fn whitelist_fields<I, S>(mut self, fields: I) -> Self where I: IntoIterator<Item = S>, S: Into<String> {
        // correct builder on RedditETL instance
        self.etl = self.etl.whitelist_fields(fields); self
    }

    pub fn usernames(self) -> Result<UsernameStream> {
        let targets = resolve_target_subs_from(&self.etl.opts.subreddit, &self.query.subreddits);
        init_tracing_once();
        if let Some(n) = self.etl.opts.parallelism { if n > 0 { rayon::ThreadPoolBuilder::new().num_threads(n).build_global().ok(); } }

        let work_dir = self.etl.ensure_work_dir()?;
        let discovered = discover_all(&self.etl.opts.comments_dir, &self.etl.opts.submissions_dir);
        let files = plan_files(&discovered, self.etl.opts.sources, self.etl.opts.start, self.etl.opts.end);
        let shard_writer = ShardedWriter::create(&work_dir, "usernames_q", self.etl.opts.shard_count)?;
        let targets_ref = targets.as_ref();
        let bounds = bounds_tuple(self.etl.opts.start, self.etl.opts.end);
        let read_buf = self.etl.opts.read_buffer_bytes;

        let total_bytes = total_compressed_size(&files);
        let pb = if self.etl.opts.progress {
            Some(make_progress_bar_labeled(total_bytes, self.etl.opts.progress_label.as_deref()))
        } else {
            None
        };

        crate::concurrency::for_each_file_limited(&files, self.etl.opts.file_concurrency, |job| -> Result<()> {
            // progress-aware reader when enabled
            if let Some(pb) = &pb {
                for_each_line_with_progress_cfg(&job.path, read_buf, |delta| pb.inc(delta), |line| {
                    if let Ok(min) = parse_minimal(line) {
                        if !matches_minimal(&min, targets_ref, &self.query) { return Ok(()); }
                        if !within_bounds(&min, bounds) { return Ok(()); }
                        if let Some(a) = min.author.as_deref() {
                            let a = a.trim();
                            if a.is_empty() { return Ok(()); }
                            shard_writer.write(a)?;
                        }
                    }
                    Ok(())
                })?;
            } else {
                for_each_line_cfg(&job.path, read_buf, |line| {
                    if let Ok(min) = parse_minimal(line) {
                        if !matches_minimal(&min, targets_ref, &self.query) { return Ok(()); }
                        if !within_bounds(&min, bounds) { return Ok(()); }
                        if let Some(a) = min.author.as_deref() {
                            let a = a.trim();
                            if a.is_empty() { return Ok(()); }
                            shard_writer.write(a)?;
                        }
                    }
                    Ok(())
                })?;
            }
            Ok(())
        })?;

        let deduped = shard_writer.dedup("usernames_q")?;
        UsernameStream::from_deduped_files(deduped)
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
    /// Returns `(vector_of_paths, total_records_written)`.
    /// Note: `resume` is accepted for API compatibility but ignored.
    pub fn extract_spool_monthly(self, out_dir: &Path, _resume: bool) -> Result<(Vec<PathBuf>, u64)> {
        init_tracing_once();
        if let Some(n) = self.etl.opts.parallelism { if n > 0 { rayon::ThreadPoolBuilder::new().num_threads(n).build_global().ok(); } }

        fs::create_dir_all(out_dir)?;

        let targets = resolve_target_subs_from(&self.etl.opts.subreddit, &self.query.subreddits);
        let discovered = discover_all(&self.etl.opts.comments_dir, &self.etl.opts.submissions_dir);
        let files = plan_files(&discovered, self.etl.opts.sources, self.etl.opts.start, self.etl.opts.end);

        if files.is_empty() {
            return Ok((Vec::new(), 0));
        }

        let whitelist = self.etl.opts.whitelist_fields.clone();
        let targets_ref = targets.as_ref();
        let bounds = bounds_tuple(self.etl.opts.start, self.etl.opts.end);
        let read_buf = self.etl.opts.read_buffer_bytes;
        let write_buf = self.etl.opts.write_buffer_bytes;
        let human_ts = self.etl.opts.human_readable_timestamps;

        let total_bytes = total_compressed_size(&files);
        let pb = if self.etl.opts.progress {
            Some(make_progress_bar_labeled(total_bytes, self.etl.opts.progress_label.as_deref()))
        } else {
            None
        };

        let total_written = AtomicU64::new(0);
        let parts = Mutex::new(Vec::<PathBuf>::new());

        crate::concurrency::for_each_file_limited(&files, self.etl.opts.file_concurrency, |job| -> Result<()> {
            let prefix = match job.kind {
                crate::paths::FileKind::Comment => "part_RC",
                crate::paths::FileKind::Submission => "part_RS",
            };
            let out_path = out_dir.join(format!("{}_{}.jsonl", prefix, job.ym));

            // Always (re)create the output file.
            let file = match create_with_backoff(&out_path, 16, 50) {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!(path=%out_path.display(), error=%e, "Skipping month: failed to create spool output after retries");
                    if let Some(pb) = &pb {
                        let sz = fs::metadata(&job.path).map(|m| m.len()).unwrap_or(0);
                        pb.inc(sz);
                    }
                    return Ok(());
                }
            };
            let mut writer = BufWriter::with_capacity(write_buf, file);

            let n = stream_job(
                job, &mut writer, targets_ref, &self.query, &whitelist,
                pb.clone(), bounds, read_buf, human_ts
            )?;
            writer.flush()?;
            total_written.fetch_add(n, Ordering::Relaxed);
            parts.lock().unwrap().push(out_path);
            Ok(())
        })?;

        if let Some(pb) = pb { pb.finish_with_message("done"); }

        let mut list = parts.into_inner().unwrap();
        list.sort();
        list.dedup();
        Ok((list, total_written.load(Ordering::Relaxed)))
    }

    pub fn count_by_month(self) -> Result<BTreeMap<YearMonth, u64>> {
        let targets = resolve_target_subs_from(&self.etl.opts.subreddit, &self.query.subreddits);
        init_tracing_once();

        let discovered = discover_all(&self.etl.opts.comments_dir, &self.etl.opts.submissions_dir);
        let files = plan_files(&discovered, self.etl.opts.sources, self.etl.opts.start, self.etl.opts.end);

        let targets_ref = targets.as_ref();
        let mut total = BTreeMap::<YearMonth, u64>::new();
        let bounds = bounds_tuple(self.etl.opts.start, self.etl.opts.end);
        let read_buf = self.etl.opts.read_buffer_bytes;

        if self.etl.opts.file_concurrency <= 1 {
            for job in &files {
                let part = crate::counting::count_for_job(job, targets_ref, &self.query, bounds, read_buf)?;
                crate::counting::merge_counts(&mut total, part);
            }
        } else {
            for chunk in files.chunks(self.etl.opts.file_concurrency) {
                let parts = chunk.par_iter().map(|job| {
                    crate::counting::count_for_job(job, targets_ref, &self.query, bounds, read_buf)
                }).collect::<Result<Vec<_>>>()?;
                for p in parts { crate::counting::merge_counts(&mut total, p); }
            }
        }
        Ok(total)
    }

    pub fn author_counts_to_tsv(self, out_path: &Path) -> Result<()> {
        let targets = resolve_target_subs_from(&self.etl.opts.subreddit, &self.query.subreddits);
        init_tracing_once();
        if let Some(n) = self.etl.opts.parallelism { if n > 0 { rayon::ThreadPoolBuilder::new().num_threads(n).build_global().ok(); } }

        let discovered = discover_all(&self.etl.opts.comments_dir, &self.etl.opts.submissions_dir);
        let files = plan_files(&discovered, self.etl.opts.sources, self.etl.opts.start, self.etl.opts.end);
        let work_dir = self.etl.ensure_work_dir()?;
        let kv = ShardedKVWriter::create(&work_dir, "author_counts", self.etl.opts.shard_count)?;

        let targets_ref = targets.as_ref();
        let bounds = bounds_tuple(self.etl.opts.start, self.etl.opts.end);
        let read_buf = self.etl.opts.read_buffer_bytes;

        crate::concurrency::for_each_file_limited(&files, self.etl.opts.file_concurrency, |job| -> Result<()> {
            for_each_line_cfg(&job.path, read_buf, |line| {
                if let Ok(min) = parse_minimal(line) {
                    if !matches_minimal(&min, targets_ref, &self.query) { return Ok(()); }
                    if !within_bounds(&min, bounds) { return Ok(()); }
                    if self.query.requires_full_parse() {
                        let val: serde_json::Value = serde_json::from_str(line)?;
                        if !matches_full(&val, job.kind, &self.query) { return Ok(()); }
                    }
                    if let Some(a) = min.author.as_deref() {
                        let a = a.trim();
                        if a.is_empty() { return Ok(()); }
                        kv.write_kv(a, 1)?;
                    }
                }
                Ok(())
            })
        })?;

        let shards = kv.reduce_sum("author_counts")?;
        concat_tsvs(&shards, out_path, self.etl.opts.write_buffer_bytes)?;
        Ok(())
    }

    pub fn build_first_seen_index_to_tsv(self, out_path: &Path) -> Result<()> {
        let targets = resolve_target_subs_from(&self.etl.opts.subreddit, &self.query.subreddits);
        init_tracing_once();

        let discovered = discover_all(&self.etl.opts.comments_dir, &self.etl.opts.submissions_dir);
        let files = plan_files(&discovered, self.etl.opts.sources, self.etl.opts.start, self.etl.opts.end);
        let work_dir = self.etl.ensure_work_dir()?;
        let kv = ShardedKVWriter::create(&work_dir, "first_seen", self.etl.opts.shard_count)?;

        let targets_ref = targets.as_ref();
        let bounds = bounds_tuple(self.etl.opts.start, self.etl.opts.end);
        let read_buf = self.etl.opts.read_buffer_bytes;

        crate::concurrency::for_each_file_limited(&files, self.etl.opts.file_concurrency, |job| -> Result<()> {
            for_each_line_cfg(&job.path, read_buf, |line| {
                if let Ok(min) = parse_minimal(line) {
                    if !matches_minimal(&min, targets_ref, &self.query) { return Ok(()); }
                    if !within_bounds(&min, bounds) { return Ok(()); }
                    if self.etl.opts.progress && self.query.requires_full_parse() {
                        let val: serde_json::Value = serde_json::from_str(line)?;
                        if !matches_full(&val, job.kind, &self.query) { return Ok(()); }
                    }
                    if let (Some(a), Some(ts)) = (min.author.as_deref(), min.created_utc) {
                        let a = a.trim();
                        if a.is_empty() { return Ok(()); }
                        kv.write_kv(a, ts)?;
                    }
                }
                Ok(())
            })
        })?;

        let shards = kv.reduce_min("first_seen")?;
        concat_tsvs(&shards, out_path, self.etl.opts.write_buffer_bytes)?;
        Ok(())
    }

    /// Export corpus back to partitioned JSONL or ZST by month/kinds with query filters.
    /// This lives on ScanPlan (advanced query mode).
    pub fn export_partitioned(self, out_base_dir: &Path, format: ExportFormat) -> Result<()> {
        let targets = resolve_target_subs_from(&self.etl.opts.subreddit, &self.query.subreddits);
        init_tracing_once();
        if let Some(n) = self.etl.opts.parallelism { if n > 0 { rayon::ThreadPoolBuilder::new().num_threads(n).build_global().ok(); } }

        let discovered = discover_all(&self.etl.opts.comments_dir, &self.etl.opts.submissions_dir);
        let files = plan_files(&discovered, self.etl.opts.sources, self.etl.opts.start, self.etl.opts.end);

        let comments_dir = out_base_dir.join("comments");
        let submissions_dir = out_base_dir.join("submissions");
        fs::create_dir_all(&comments_dir)?;
        fs::create_dir_all(&submissions_dir)?;

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

        crate::concurrency::for_each_file_limited(&files, self.etl.opts.file_concurrency, |job| -> Result<()> {
            let (prefix, out_dir) = match job.kind {
                crate::paths::FileKind::Comment => ("RC", &comments_dir),
                crate::paths::FileKind::Submission => ("RS", &submissions_dir),
            };
            let file_name = match format {
                ExportFormat::Jsonl => format!("{}_{}.jsonl", prefix, job.ym),
                ExportFormat::Zst   => format!("{}_{}.zst",   prefix, job.ym),
            };
            let out_path = out_dir.join(file_name);

            let written = match format {
                ExportFormat::Jsonl => {
                    let file = create_with_backoff(&out_path, 16, 50)
                        .with_context(|| format!("create {}", out_path.display()))?;
                    let mut writer = BufWriter::with_capacity(write_buf, file);
                    let n = stream_job(job, &mut writer, targets_ref, &self.query, &whitelist, pb.clone(), bounds, read_buf, human_ts)?;
                    writer.flush()?;
                    n
                }
                ExportFormat::Zst => {
                    let file = create_with_backoff(&out_path, 16, 50)
                        .with_context(|| format!("create {}", out_path.display()))?;
                    let mut enc = ZstdEncoder::new(file, 19)?;
                    let n = stream_job(job, &mut enc, targets_ref, &self.query, &whitelist, pb.clone(), bounds, read_buf, human_ts)?;
                    enc.finish()?;
                    n
                }
            };

            if written == 0 { let _ = fs::remove_file(&out_path); }
            Ok(())
        })?;

        if let Some(pb) = pb { pb.finish_with_message("done"); }
        Ok(())
    }
}

/// Shared extraction logic used by:
///  - `RedditETL::extract_to_jsonl` (single-sub mode)
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
    init_tracing_once();
    if let Some(n) = etl.opts.parallelism { if n > 0 { rayon::ThreadPoolBuilder::new().num_threads(n).build_global().ok(); } }

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
}
