use crate::date::YearMonth;
use std::path::{Path, PathBuf};

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
    pub subreddit: Option<String>,    // normalized lowercase, no "r/"
    pub sources: Sources,
    pub start: Option<YearMonth>,     // inclusive
    pub end: Option<YearMonth>,       // inclusive
    pub shard_count: usize,           // number of on-disk dedup shards
    pub whitelist_fields: Option<Vec<String>>,
    pub parallelism: Option<usize>,   // Some(N) to set rayon threads, None to use default
    pub work_dir: Option<PathBuf>,    // if None, create in base_dir/.reddit_etl_work/
    pub file_concurrency: usize,      // limit number of monthly files processed concurrently
    pub progress: bool,               // show progress bar
    pub progress_label: Option<String>, // optional label for progress bar

    // IO tuning
    pub read_buffer_bytes: usize,     // BufReader capacity
    pub write_buffer_bytes: usize,    // BufWriter capacity

    // output formatting
    pub human_readable_timestamps: bool, // convert unix timestamps to RFC3339 strings
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
            parallelism: None,
            work_dir: None,
            file_concurrency: 1, // safe default to prevent OOM on big .zst windows
            progress: true,
            progress_label: None,

            read_buffer_bytes: default_read,
            write_buffer_bytes: default_write,

            human_readable_timestamps: false,
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
        self.whitelist_fields = Some(fields.into_iter().map(Into::into).collect());
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
}
