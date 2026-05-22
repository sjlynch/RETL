use crate::config::{ETLOptions, Sources};
use crate::date::YearMonth;
use crate::mem::AdaptiveMemCfg;
use crate::parents::ParentPayloadSpec;
use crate::query::{
    normalize_str, read_record_ids_file, JsonPointerPredicate, NumericComparison, QueryBuildError,
    QuerySpec, RecordIdKind, TimestampBounds,
};
use crate::util::{default_bot_authors, try_merge_extra_exclusions};
use anyhow::Result;
use regex::Regex;
use std::path::{Path, PathBuf};

#[derive(Clone)]
pub struct RedditETL {
    pub(crate) opts: ETLOptions,
}

impl Default for RedditETL {
    fn default() -> Self {
        Self::new()
    }
}

impl RedditETL {
    pub fn new() -> Self {
        Self {
            opts: ETLOptions::default(),
        }
    }

    // -------- Builder methods --------
    pub fn base_dir(mut self, base: impl AsRef<std::path::Path>) -> Self {
        self.opts = self.opts.with_base_dir(base);
        self
    }
    #[deprecated(
        note = "use RedditETL::scan().subreddits([...]) instead; ETLOptions::subreddit is a single-value default"
    )]
    pub fn subreddit(mut self, sub: impl AsRef<str>) -> Self {
        #[allow(deprecated)]
        {
            self.opts = self.opts.with_subreddit(sub);
        }
        self
    }
    pub fn sources(mut self, sources: Sources) -> Self {
        self.opts = self.opts.with_sources(sources);
        self
    }
    pub fn date_range(mut self, start: Option<YearMonth>, end: Option<YearMonth>) -> Self {
        self.opts = self.opts.with_date_range(start, end);
        self
    }
    pub fn whitelist_fields<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.opts = self.opts.with_whitelist_fields(fields);
        self
    }
    pub fn strict_whitelist(mut self, yes: bool) -> Self {
        self.opts = self.opts.with_strict_whitelist(yes);
        self
    }
    pub fn strict_key(mut self, yes: bool) -> Self {
        self.opts = self.opts.with_strict_key(yes);
        self
    }
    /// Fail the whole aggregate run when any input is fatal (open error,
    /// malformed JSON, shard write failure). Default is the historical
    /// tolerant behavior — a fatal input is reported but the run still merges
    /// the surviving shards. Honored by
    /// [`RedditETL::aggregate_jsonls_parallel`].
    pub fn aggregate_strict(mut self, yes: bool) -> Self {
        self.opts = self.opts.with_aggregate_strict(yes);
        self
    }
    pub fn parallelism(mut self, threads: usize) -> Self {
        self.opts = self.opts.with_parallelism(threads);
        self
    }
    pub fn work_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.opts = self.opts.with_work_dir(dir);
        self
    }
    pub fn shard_count(mut self, count: usize) -> Self {
        self.opts = self.opts.with_shard_count(count);
        self
    }
    pub fn file_concurrency(mut self, n: usize) -> Self {
        self.opts = self.opts.with_file_concurrency(n);
        self
    }
    pub fn progress(mut self, yes: bool) -> Self {
        self.opts = self.opts.with_progress(yes);
        self
    }
    pub fn progress_label(mut self, label: impl Into<String>) -> Self {
        self.opts = self.opts.with_progress_label(label);
        self
    }
    pub fn io_read_buffer(mut self, bytes: usize) -> Self {
        self.opts = self.opts.with_io_read_buffer(bytes);
        self
    }
    pub fn io_write_buffer(mut self, bytes: usize) -> Self {
        self.opts = self.opts.with_io_write_buffer(bytes);
        self
    }
    pub fn io_buffers(mut self, read_bytes: usize, write_bytes: usize) -> Self {
        self.opts = self.opts.with_io_buffers(read_bytes, write_bytes);
        self
    }
    pub fn timestamps_human_readable(mut self, yes: bool) -> Self {
        self.opts = self.opts.with_human_timestamps(yes);
        self
    }
    pub fn zst_level(mut self, level: i32) -> Self {
        self.opts = self.opts.with_zst_level(level);
        self
    }
    /// Override the inflight-bytes backpressure budget used by bucketing/dedupe
    /// producer/consumer pairs.
    ///
    /// Setting this alone does **not** bound the bucketing worst-case peak —
    /// `inflight_groups` adds a multiplier on top, so the actual peak is
    /// `(1 + inflight_groups) * inflight_bytes / 2`. See
    /// [`ETLOptions::inflight_bytes`] for the full formula and
    /// [`Self::inflight_budget`] for a helper that derives both values.
    pub fn inflight_bytes(mut self, bytes: usize) -> Self {
        self.opts = self.opts.with_inflight_bytes(bytes);
        self
    }
    /// Override the bounded-channel depth used by bucketing producer/consumer
    /// pairs.
    ///
    /// Paired with `inflight_bytes`: raising this raises the bucketing memory
    /// peak by `inflight_bytes / 2` per added group. See
    /// [`ETLOptions::inflight_groups`] for the worst-case formula and
    /// [`Self::inflight_budget`] for a helper that derives both values
    /// together.
    pub fn inflight_groups(mut self, groups: usize) -> Self {
        self.opts = self.opts.with_inflight_groups(groups);
        self
    }

    /// Set both `inflight_bytes` and `inflight_groups` from a single budget so
    /// the bucketing worst-case peak is bounded by the declared value.
    ///
    /// Equivalent to `.inflight_bytes(bytes).inflight_groups(1)`. Prefer this
    /// when you want the value you pass to be the actual RAM ceiling; use the
    /// individual setters only when you need to tune throughput (deeper
    /// channel) and have measured headroom.
    pub fn inflight_budget(mut self, bytes: usize) -> Self {
        self.opts = self.opts.with_inflight_budget(bytes);
        self
    }

    /// Explicitly disable the `inflight_bytes` memory cap (producers fall back
    /// to adaptive memory-fraction sampling).
    ///
    /// This is the deliberate, warning-free opt-out for that mode; passing `0`
    /// to [`Self::inflight_bytes`] / [`Self::inflight_budget`] instead emits a
    /// one-shot warning, since a budget computation rounding to `0` is almost
    /// always a bug. See [`ETLOptions::disable_inflight_cap`].
    pub fn disable_inflight_cap(mut self) -> Self {
        self.opts = self.opts.disable_inflight_cap();
        self
    }
    /// Override the adaptive-memory policy used by bucketing/dedupe producers.
    pub fn adaptive_mem(mut self, cfg: AdaptiveMemCfg) -> Self {
        self.opts = self.opts.with_adaptive_mem(cfg);
        self
    }
    /// Opt in to resumable scan/export runs: when enabled, supported analytics
    /// and export paths read/write a `_progress.json` sidecar keyed by month and
    /// by a fingerprint of the current query/config/corpus, so changing filters
    /// or input files invalidates stale parts instead of reusing them. Default
    /// false to preserve existing behavior.
    pub fn resume(mut self, yes: bool) -> Self {
        self.opts = self.opts.with_resume(yes);
        self
    }

    /// Enable or disable user-facing provenance manifest sidecars next to
    /// file/directory outputs. Enabled by default; disable when local absolute
    /// paths should not be written to sidecar metadata.
    pub fn run_manifest(mut self, yes: bool) -> Self {
        self.opts = self.opts.with_run_manifest(yes);
        self
    }

    /// Select top-level parent fields attached by the parents pipeline. The
    /// default is backwards-compatible (`body` for comments and
    /// `title,selftext` for submissions).
    pub fn parent_fields<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.opts = self.opts.with_parent_fields(fields);
        self
    }

    /// Attach each resolved parent as its full source JSON record, plus RETL's
    /// `kind` and `id` metadata.
    pub fn parent_full(mut self, yes: bool) -> Self {
        self.opts = self.opts.with_parent_full(yes);
        self
    }

    /// Replace the full parent-payload specification used by parents helpers.
    pub fn parent_payload_spec(mut self, spec: ParentPayloadSpec) -> Self {
        self.opts = self.opts.with_parent_payload_spec(spec);
        self
    }

    /// Opt in to lossy scans/exports that skip corrupt zstd monthly files
    /// instead of failing. Skipped paths are recorded in the shared
    /// partial-read reporter; resume manifests never mark skipped months
    /// complete.
    pub fn allow_partial(mut self, yes: bool) -> Self {
        self.opts = self.opts.with_allow_partial(yes);
        self
    }

    /// Return a handle that snapshots tolerated partial zstd reads recorded by
    /// this builder. Clone it before starting a consuming operation.
    ///
    /// Each consuming operation clears the reporter before processing any
    /// month, so a snapshot taken after a run reflects only that run's
    /// skipped files — running two operations on one builder does not make
    /// the second snapshot include the first run's skips.
    pub fn partial_read_reporter(&self) -> crate::config::PartialReadReporter {
        self.opts.partial_read_reporter.clone()
    }

    // -------- Advanced: enter query mode --------
    pub fn scan(self) -> ScanPlan {
        ScanPlan {
            etl: self,
            query: QuerySpec {
                filter_pseudo_users: true,
                ..Default::default()
            }
            .normalize(),
            limit: None,
        }
    }

    pub(crate) fn ensure_work_dir(&self) -> Result<PathBuf> {
        let dir = self
            .opts
            .work_dir
            .clone()
            .unwrap_or_else(|| self.opts.base_dir.join(".reddit_etl_work"));
        crate::util::create_dir_all_with_default_backoff(&dir)?;
        Ok(dir)
    }
}
