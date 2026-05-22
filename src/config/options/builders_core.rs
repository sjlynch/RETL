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
            // Re-trim after stripping the prefix: "r/  foo" otherwise yields
            // a leading-space subreddit that passes validation and then
            // silently matches zero records.
            s = rest.trim().to_string();
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

    /// Surface any deferred [`ConfigBuildError`] recorded by a builder setter.
    ///
    /// `with_date_range` stores an invalid (`start > end`) range as a deferred
    /// `build_error` instead of failing at the call site. Every public
    /// operation entry point that consumes these options — `extract`/`spool`
    /// (via file planning), `integrity`, and the parents resolver — calls this
    /// first so a backwards date range fails fast with the purpose-built
    /// "invalid date range" message rather than a confusing downstream
    /// "planned zero corpus files" / `DateRangeNoFiles` error.
    pub fn check_config(&self) -> anyhow::Result<()> {
        if let Some(err) = self.build_error.clone() {
            return Err(err.into());
        }
        Ok(())
    }

    pub fn with_shard_count(mut self, shards: usize) -> Self {
        self.shard_count = clamp_shard_count(shards, "ETLOptions::with_shard_count");
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

    /// Fail the whole aggregate run when any input is fatal, instead of
    /// reporting it and merging the surviving shards. See
    /// [`ETLOptions::aggregate_strict`].
    pub fn with_aggregate_strict(mut self, yes: bool) -> Self {
        self.aggregate_strict = yes;
        self
    }

    pub fn with_parallelism(mut self, threads: usize) -> Self {
        self.parallelism = Some(clamp_parallelism_threads(
            threads,
            "ETLOptions::with_parallelism",
        ));
        self
    }

    pub fn with_work_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.work_dir = Some(dir.as_ref().to_path_buf());
        self
    }

    pub fn with_file_concurrency(mut self, n: usize) -> Self {
        self.file_concurrency = clamp_file_concurrency(n, "ETLOptions::with_file_concurrency");
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
}
