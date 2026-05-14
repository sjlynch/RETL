use crate::date::YearMonth;
use crate::mem::AdaptiveMemCfg;
use crate::parents::ParentPayloadSpec;
use parking_lot::Mutex;
use serde::Serialize;
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Structured error returned when ETL option builders contain invalid settings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfigBuildError {
    InvalidDateRange { start: YearMonth, end: YearMonth },
}

impl fmt::Display for ConfigBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigBuildError::InvalidDateRange { start, end } => {
                write!(f, "invalid date range: start {start} is after end {end}")
            }
        }
    }
}

impl Error for ConfigBuildError {}

/// Data source toggle (comments, submissions, both).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sources {
    Comments,
    Submissions,
    Both,
}

/// A single input file skipped because `allow_partial` tolerated a zstd
/// decode error.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SkippedFile {
    pub path: PathBuf,
    pub error: String,
}

/// Machine-readable snapshot of partial-read skips observed by a run.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct PartialReadReport {
    pub skipped_file_count: usize,
    pub skipped_files: Vec<SkippedFile>,
}

/// Shared collector for tolerated partial zstd reads.
///
/// Clone the handle before starting a consuming operation, then call
/// [`PartialReadReporter::snapshot`] afterwards to inspect the skipped paths.
#[derive(Clone, Debug, Default)]
pub struct PartialReadReporter {
    inner: Arc<Mutex<Vec<SkippedFile>>>,
}

impl PartialReadReporter {
    pub fn record(&self, path: &Path, error: impl fmt::Display) {
        self.inner.lock().push(SkippedFile {
            path: path.to_path_buf(),
            error: error.to_string(),
        });
    }

    pub fn snapshot(&self) -> PartialReadReport {
        let skipped_files = self.inner.lock().clone();
        PartialReadReport {
            skipped_file_count: skipped_files.len(),
            skipped_files,
        }
    }

    pub fn clear(&self) {
        self.inner.lock().clear();
    }
}

/// User-facing options with sensible defaults and builder chaining.
#[derive(Clone, Debug)]
pub struct ETLOptions {
    /// Corpus base directory containing `comments/` and `submissions/`.
    /// Defaults to `./data`, matching the `retl` CLI and README examples.
    pub base_dir: PathBuf,
    pub comments_dir: PathBuf,
    pub submissions_dir: PathBuf,
    pub subreddit: Option<String>, // normalized lowercase, no "r/"; deprecated single-subreddit default
    pub sources: Sources,
    pub start: Option<YearMonth>, // inclusive
    pub end: Option<YearMonth>,   // inclusive
    pub shard_count: usize,       // number of on-disk dedup shards
    pub whitelist_fields: Option<Vec<String>>,
    pub strict_whitelist: bool,     // fail instead of warn when whitelisted keys match nothing
    pub strict_key: bool,           // fail dedupe when matching records lack the requested key
    pub parallelism: Option<usize>, // Some(N) to set rayon threads, None to use default
    pub work_dir: Option<PathBuf>,  // if None, create in base_dir/.reddit_etl_work/
    pub file_concurrency: usize,    // limit number of monthly files processed concurrently
    pub progress: bool,             // show progress bar
    pub progress_label: Option<String>, // optional label for progress bar

    // IO tuning
    pub read_buffer_bytes: usize,  // BufReader capacity
    pub write_buffer_bytes: usize, // BufWriter capacity

    // output formatting
    pub human_readable_timestamps: bool, // convert unix timestamps to RFC3339 strings

    // zstd compression level used by partitioned ZST writers
    pub zst_level: i32,

    /// Bound (in bytes) on data inflight between bucketing/dedupe producers
    /// and their downstream consumers. Sets `per_flush_cap = inflight_bytes /
    /// 2` for the producer-side map. Defaults to 256 MiB.
    ///
    /// **This is not the only memory lever.** [`inflight_groups`] adds a
    /// channel of buffered groups on top, so the worst-case bucketing peak is
    ///
    /// ```text
    /// peak ≈ (1 + inflight_groups) * (inflight_bytes / 2)
    /// ```
    ///
    /// (the dedupe pipeline pins channel capacity to 1, so its peak stays at
    /// `~inflight_bytes`). With the defaults
    /// (`inflight_bytes = 256 MiB`, `inflight_groups = 8`) the bucketing peak
    /// is ≈ 1.125 GiB, *not* 256 MiB. Use [`with_inflight_budget`] to set both
    /// values together so the declared budget matches the actual peak.
    ///
    /// [`inflight_groups`]: ETLOptions::inflight_groups
    /// [`with_inflight_budget`]: ETLOptions::with_inflight_budget
    pub inflight_bytes: usize,

    /// Number of buffered groups allowed between bucketing producers and
    /// consumers. Affects the bucketing stage only (the dedupe stage hard-codes
    /// channel capacity to 1). Defaults to 8.
    ///
    /// **Interacts with [`inflight_bytes`]:** each buffered group can hold up
    /// to `inflight_bytes / 2` bytes, so raising this value raises the
    /// bucketing memory peak proportionally. Worst-case peak:
    ///
    /// ```text
    /// peak ≈ (1 + inflight_groups) * (inflight_bytes / 2)
    /// ```
    ///
    /// The two values are NOT independent. To keep the peak ≤ the declared
    /// budget, use [`with_inflight_budget`] which sets both for you.
    /// `retl` emits a one-shot `tracing::warn!` when a configured pair would
    /// exceed roughly 2× the declared `inflight_bytes`.
    ///
    /// [`inflight_bytes`]: ETLOptions::inflight_bytes
    /// [`with_inflight_budget`]: ETLOptions::with_inflight_budget
    pub inflight_groups: usize,

    /// Adaptive-memory policy shared by bucketing/dedupe producers. Controls
    /// the free-memory fractions used to shrink/grow producer buffers and the
    /// minimum cooldown between target recomputations.
    pub adaptive_mem: AdaptiveMemCfg,

    /// Opt-in: when true, supported extract/export operations read/write a
    /// `_progress.json`-style sidecar and skip months already committed by a
    /// prior run. Default false to preserve current behavior.
    pub resume: bool,

    /// Parent payload fields attached by the parents pipeline. Defaults to the
    /// legacy output shape (`body` for comments, `title`/`selftext` for
    /// submissions).
    pub parent_payload_spec: ParentPayloadSpec,

    /// Opt-in lossy mode for corrupt zstd inputs. The default (`false`) treats
    /// zstd decode errors as fatal so scans/exports cannot silently return
    /// partial results. When `true`, corrupt monthly files are skipped, recorded
    /// in [`partial_read_reporter`], and never committed to resume manifests.
    pub allow_partial: bool,
    pub partial_read_reporter: PartialReadReporter,

    #[doc(hidden)]
    pub build_error: Option<ConfigBuildError>,
}

impl Default for ETLOptions {
    fn default() -> Self {
        let base = PathBuf::from("./data");
        // Defaults chosen to be safe but noticeably faster than std defaults.
        // Adjust at runtime via io_* builder methods.
        let default_read = 256 * 1024;
        let default_write = 256 * 1024;

        Self {
            comments_dir: base.join("comments"),
            submissions_dir: base.join("submissions"),
            base_dir: base,
            subreddit: None,
            sources: Sources::Both,
            start: None,
            end: None,
            shard_count: 256,
            whitelist_fields: None,
            strict_whitelist: false,
            strict_key: false,
            parallelism: None,
            work_dir: None,
            file_concurrency: 1, // safe default to prevent OOM on big .zst windows
            progress: true,
            progress_label: None,

            read_buffer_bytes: default_read,
            write_buffer_bytes: default_write,

            human_readable_timestamps: false,

            zst_level: 7,

            inflight_bytes: 256 * 1024 * 1024,
            inflight_groups: 8,
            adaptive_mem: AdaptiveMemCfg::default(),
            resume: false,
            parent_payload_spec: ParentPayloadSpec::default(),
            allow_partial: false,
            partial_read_reporter: PartialReadReporter::default(),
            build_error: None,
        }
    }
}

impl ETLOptions {
    pub fn with_base_dir(mut self, base_dir: impl AsRef<Path>) -> Self {
        let base = base_dir.as_ref().to_path_buf();
        self.comments_dir = base.join("comments");
        self.submissions_dir = base.join("submissions");
        self.base_dir = base;
        self
    }
    #[deprecated(note = "use RedditETL::scan().subreddits([...]) instead")]
    pub fn with_subreddit(mut self, sub: impl AsRef<str>) -> Self {
        let mut s = sub.as_ref().trim().to_lowercase();
        if let Some(rest) = s.strip_prefix("r/") {
            s = rest.to_string();
        }
        self.subreddit = Some(s);
        self
    }
    pub fn with_sources(mut self, sources: Sources) -> Self {
        self.sources = sources;
        self
    }
    pub fn with_date_range(mut self, start: Option<YearMonth>, end: Option<YearMonth>) -> Self {
        self.start = start;
        self.end = end;
        self.build_error = match (start, end) {
            (Some(s), Some(e)) if s > e => {
                Some(ConfigBuildError::InvalidDateRange { start: s, end: e })
            }
            _ => None,
        };
        self
    }
    pub fn with_shard_count(mut self, shards: usize) -> Self {
        self.shard_count = shards.max(1);
        self
    }
    pub fn with_whitelist_fields<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.whitelist_fields = Some(
            fields
                .into_iter()
                .filter_map(|field| {
                    let field = field.into();
                    let field = field.trim();
                    if field.is_empty() {
                        None
                    } else {
                        Some(field.to_string())
                    }
                })
                .collect(),
        );
        self
    }
    pub fn with_strict_whitelist(mut self, yes: bool) -> Self {
        self.strict_whitelist = yes;
        self
    }
    pub fn with_strict_key(mut self, yes: bool) -> Self {
        self.strict_key = yes;
        self
    }
    pub fn with_parallelism(mut self, threads: usize) -> Self {
        self.parallelism = Some(threads);
        self
    }
    pub fn with_work_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.work_dir = Some(dir.as_ref().to_path_buf());
        self
    }
    pub fn with_file_concurrency(mut self, n: usize) -> Self {
        self.file_concurrency = n.max(1);
        self
    }
    pub fn with_progress(mut self, yes: bool) -> Self {
        self.progress = yes;
        self
    }
    pub fn with_progress_label(mut self, label: impl Into<String>) -> Self {
        self.progress_label = Some(label.into());
        self
    }

    // IO buffers tuning
    pub fn with_io_read_buffer(mut self, bytes: usize) -> Self {
        self.read_buffer_bytes = bytes.max(8 * 1024);
        self
    }
    pub fn with_io_write_buffer(mut self, bytes: usize) -> Self {
        self.write_buffer_bytes = bytes.max(8 * 1024);
        self
    }
    pub fn with_io_buffers(mut self, read_bytes: usize, write_bytes: usize) -> Self {
        self.read_buffer_bytes = read_bytes.max(8 * 1024);
        self.write_buffer_bytes = write_bytes.max(8 * 1024);
        self
    }

    // Output: human-readable timestamps
    pub fn with_human_timestamps(mut self, yes: bool) -> Self {
        self.human_readable_timestamps = yes;
        self
    }

    /// Set the zstd compression level used when writing partitioned `.zst`
    /// outputs. zstd's accepted range is 1..=22; values outside that band are
    /// clamped. Default: 7 (good ratio, ~5x faster than 19 on real workloads).
    pub fn with_zst_level(mut self, level: i32) -> Self {
        self.zst_level = level.clamp(1, 22);
        self
    }

    /// Set the inflight-bytes budget that bounds bucketing/dedupe producers.
    /// Lower values trade smaller memory peaks for more frequent flushes.
    /// 0 disables the explicit cap and falls back to memory-fraction sampling.
    ///
    /// Note: this does **not** also bound the bucketing-stage channel. See the
    /// docs on [`ETLOptions::inflight_bytes`] for the worst-case peak formula,
    /// or use [`Self::with_inflight_budget`] to set both knobs together.
    pub fn with_inflight_bytes(mut self, bytes: usize) -> Self {
        self.inflight_bytes = bytes;
        self
    }

    /// Set the bounded-channel depth used by bucketing producer/consumer
    /// pairs. Values below 1 are clamped to 1.
    ///
    /// Note: this is paired with `inflight_bytes`; raising it raises the
    /// bucketing memory peak. See [`ETLOptions::inflight_groups`] for the
    /// worst-case formula and [`Self::with_inflight_budget`] for a helper that
    /// derives both values together.
    pub fn with_inflight_groups(mut self, groups: usize) -> Self {
        self.inflight_groups = groups.max(1);
        self
    }

    /// Convenience setter that derives `inflight_bytes` and `inflight_groups`
    /// from a single budget so the bucketing worst-case peak matches the
    /// declared value.
    ///
    /// Sets `inflight_bytes = bytes` and `inflight_groups = 1`, which pins the
    /// producer→consumer channel to one in-flight group. With that pairing the
    /// worst-case peak is bounded by `inflight_bytes` (per the formula in
    /// [`ETLOptions::inflight_bytes`]). Use this when you want the value you
    /// pass to be the actual RAM ceiling; use the individual setters when you
    /// need throughput tuning (a deeper channel) and have measured headroom.
    pub fn with_inflight_budget(mut self, bytes: usize) -> Self {
        self.inflight_bytes = bytes;
        self.inflight_groups = 1;
        self
    }

    /// Override the adaptive-memory policy used by bucketing/dedupe
    /// producers. This tunes cooperative throttling thresholds without
    /// changing the hard `inflight_bytes` backpressure cap.
    pub fn with_adaptive_mem(mut self, cfg: AdaptiveMemCfg) -> Self {
        self.adaptive_mem = cfg;
        self
    }

    /// Opt in to resumable extract/export runs (`extract_to_jsonl`,
    /// `extract_to_json`, `extract_spool_monthly`, `export_partitioned`, and
    /// parents helpers).
    pub fn with_resume(mut self, yes: bool) -> Self {
        self.resume = yes;
        self
    }

    /// Select top-level parent fields attached by `resolve_parent_maps` /
    /// `attach_parents_jsonls_parallel`.
    pub fn with_parent_fields<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.parent_payload_spec = ParentPayloadSpec::from_fields(fields);
        self
    }

    /// Attach the full source parent JSON record when resolving parents.
    pub fn with_parent_full(mut self, yes: bool) -> Self {
        self.parent_payload_spec = self.parent_payload_spec.with_full_record(yes);
        self
    }

    /// Replace the full parent-payload specification.
    pub fn with_parent_payload_spec(mut self, spec: ParentPayloadSpec) -> Self {
        self.parent_payload_spec = spec;
        self
    }

    /// Opt in to lossy corpus scans/exports that skip corrupt zstd monthly
    /// files instead of failing the operation. Skipped paths are collected in
    /// [`PartialReadReporter`] and incomplete months are not committed to
    /// resume manifests.
    pub fn with_allow_partial(mut self, yes: bool) -> Self {
        self.allow_partial = yes;
        self
    }
}

/// Worst-case bucketing peak for the configured `(inflight_bytes,
/// inflight_groups)` pair. Mirrors the formula documented on
/// [`ETLOptions::inflight_bytes`]:
///
/// ```text
/// peak ≈ (1 + inflight_groups) * (inflight_bytes / 2)
/// ```
///
/// Returns 0 when `inflight_bytes == 0` (the cap is disabled and the peak is
/// driven by `AdaptiveMemCfg` instead). The dedupe pipeline pins channel
/// capacity to 1 so its peak is bounded by `inflight_bytes`; this helper is
/// the bucketing-side bound.
pub(crate) fn inflight_worst_case_peak_bytes(
    inflight_bytes: usize,
    inflight_groups: usize,
) -> usize {
    if inflight_bytes == 0 {
        return 0;
    }
    let chan_cap = inflight_groups.max(1);
    chan_cap.saturating_add(1).saturating_mul(inflight_bytes / 2)
}

/// One-shot tracing warning when a configured `(inflight_bytes,
/// inflight_groups)` pair would produce a worst-case bucketing peak above
/// roughly 2× the declared `inflight_bytes`.
///
/// Fires from `BucketingCfg::from(&ETLOptions)` and the dedupe-config builder
/// in `pipeline_exec`; gated by a process-wide [`Once`] so the warning is
/// emitted at most once per process. Tests that don't initialize tracing
/// won't see it; binaries that call [`crate::util::init_tracing_for_binary`]
/// will.
pub(crate) fn warn_if_inflight_pair_pathological(inflight_bytes: usize, inflight_groups: usize) {
    use std::sync::Once;
    static WARNED: Once = Once::new();

    if inflight_bytes == 0 {
        return;
    }
    let peak = inflight_worst_case_peak_bytes(inflight_bytes, inflight_groups);
    if peak <= inflight_bytes.saturating_mul(2) {
        return;
    }
    WARNED.call_once(|| {
        let mib = |b: usize| b / (1024 * 1024);
        let ratio = peak as f64 / inflight_bytes as f64;
        let peak_mib = mib(peak);
        let declared_mib = mib(inflight_bytes);
        tracing::warn!(
            inflight_bytes,
            inflight_groups,
            worst_case_peak_bytes = peak,
            ratio,
            "bucketing memory peak ≈ {peak_mib} MiB ({ratio:.1}× declared inflight_bytes={declared_mib} MiB) — worst-case = (1 + inflight_groups) * inflight_bytes/2. \
             Lower --inflight-groups (or call RedditETL::inflight_budget(bytes) to set both together) to bring the peak back under the declared budget.",
        );
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paths::{plan_files_checked, Discovered};
    use crate::pipeline::RedditETL;
    use std::collections::BTreeMap;

    #[test]
    fn reddit_etl_new_planning_error_mentions_documented_default_data_dir() {
        let etl = RedditETL::new();
        assert_eq!(etl.opts.base_dir, PathBuf::from("./data"));
        assert_eq!(
            etl.opts.comments_dir,
            PathBuf::from("./data").join("comments")
        );
        assert_eq!(
            etl.opts.submissions_dir,
            PathBuf::from("./data").join("submissions")
        );

        let discovered = Discovered {
            comments: BTreeMap::new(),
            submissions: BTreeMap::new(),
        };
        let err = plan_files_checked(
            &discovered,
            &etl.opts.comments_dir,
            &etl.opts.submissions_dir,
            etl.opts.sources,
            etl.opts.start,
            etl.opts.end,
        )
        .expect_err("empty default corpus should produce a planning error");

        let msg = err.to_string();
        assert!(
            msg.contains(&etl.opts.comments_dir.display().to_string()),
            "{msg}"
        );
        assert!(
            msg.contains(&etl.opts.submissions_dir.display().to_string()),
            "{msg}"
        );
        assert!(!msg.contains("../reddit"), "{msg}");
    }

    #[test]
    fn worst_case_peak_matches_documented_formula() {
        // Disabled cap → 0 (no explicit ceiling; AdaptiveMemCfg drives it).
        assert_eq!(inflight_worst_case_peak_bytes(0, 8), 0);
        // Default knobs: (1 + 8) * 128 MiB = 1.125 GiB, not 256 MiB.
        let mib = 1024 * 1024;
        assert_eq!(
            inflight_worst_case_peak_bytes(256 * mib, 8),
            9 * 128 * mib
        );
        // inflight_groups = 1 (what with_inflight_budget sets) → peak == declared budget.
        assert_eq!(
            inflight_worst_case_peak_bytes(256 * mib, 1),
            2 * (256 * mib / 2)
        );
    }

    #[test]
    fn with_inflight_budget_pins_channel_depth_to_one() {
        let opts = ETLOptions::default().with_inflight_budget(64 * 1024 * 1024);
        assert_eq!(opts.inflight_bytes, 64 * 1024 * 1024);
        assert_eq!(opts.inflight_groups, 1);
        // Confirmed against the formula: worst-case peak == declared budget.
        let peak = inflight_worst_case_peak_bytes(opts.inflight_bytes, opts.inflight_groups);
        assert_eq!(peak, opts.inflight_bytes);
    }
}
