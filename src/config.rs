use crate::date::YearMonth;
use crate::mem::AdaptiveMemCfg;
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BuildError {
    InvalidDateRange { start: YearMonth, end: YearMonth },
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BuildError::InvalidDateRange { start, end } => {
                write!(f, "invalid date range: start {start} is after end {end}")
            }
        }
    }
}

impl Error for BuildError {}

/// Data source toggle (comments, submissions, both).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sources {
    Comments,
    Submissions,
    Both,
}

/// User-facing options with sensible defaults and builder chaining.
#[derive(Clone, Debug)]
pub struct ETLOptions {
    pub base_dir: PathBuf,
    pub comments_dir: PathBuf,
    pub submissions_dir: PathBuf,
    pub subreddit: Option<String>, // normalized lowercase, no "r/"; deprecated single-subreddit default
    pub sources: Sources,
    pub start: Option<YearMonth>, // inclusive
    pub end: Option<YearMonth>,   // inclusive
    pub shard_count: usize,       // number of on-disk dedup shards
    pub whitelist_fields: Option<Vec<String>>,
    pub strict_whitelist: bool,       // fail instead of warn when whitelisted keys match nothing
    pub parallelism: Option<usize>,   // Some(N) to set rayon threads, None to use default
    pub work_dir: Option<PathBuf>,    // if None, create in base_dir/.reddit_etl_work/
    pub file_concurrency: usize,      // limit number of monthly files processed concurrently
    pub progress: bool,               // show progress bar
    pub progress_label: Option<String>, // optional label for progress bar

    // IO tuning
    pub read_buffer_bytes: usize,  // BufReader capacity
    pub write_buffer_bytes: usize, // BufWriter capacity

    // output formatting
    pub human_readable_timestamps: bool, // convert unix timestamps to RFC3339 strings

    // zstd compression level used by partitioned ZST writers
    pub zst_level: i32,

    /// Bound (in bytes) on data inflight between bucketing/dedupe producers
    /// and their downstream consumers. Acts as the primary backpressure
    /// mechanism so producers stall when consumers fall behind, rather than
    /// growing in-memory hash maps to multi-GiB peaks driven by
    /// available_memory_fraction. Defaults to 256 MiB.
    pub inflight_bytes: usize,

    /// Adaptive-memory policy shared by bucketing/dedupe producers. Controls
    /// the free-memory fractions used to shrink/grow producer buffers and the
    /// minimum cooldown between target recomputations.
    pub adaptive_mem: AdaptiveMemCfg,

    /// Opt-in: when true, supported extract/export operations read/write a
    /// `_progress.json`-style sidecar and skip months already committed by a
    /// prior run. Default false to preserve current behavior.
    pub resume: bool,

    #[doc(hidden)]
    pub build_error: Option<BuildError>,
}

impl Default for ETLOptions {
    fn default() -> Self {
        let base = PathBuf::from("../reddit");
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
            adaptive_mem: AdaptiveMemCfg::default(),
            resume: false,
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
            (Some(s), Some(e)) if s > e => Some(BuildError::InvalidDateRange { start: s, end: e }),
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
                    if field.is_empty() { None } else { Some(field.to_string()) }
                })
                .collect(),
        );
        self
    }
    pub fn with_strict_whitelist(mut self, yes: bool) -> Self {
        self.strict_whitelist = yes;
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
    pub fn with_inflight_bytes(mut self, bytes: usize) -> Self {
        self.inflight_bytes = bytes;
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
    /// `extract_to_json`, `extract_spool_monthly`, and parents helpers).
    pub fn with_resume(mut self, yes: bool) -> Self {
        self.resume = yes;
        self
    }
}
