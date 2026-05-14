use crate::config::{ETLOptions, Sources};
use crate::date::YearMonth;
use crate::mem::AdaptiveMemCfg;
use crate::query::{normalize_str, QueryBuildError, QuerySpec};
use crate::util::{default_bot_authors, merge_extra_exclusions};
use anyhow::Result;
use regex::Regex;
use std::fs;
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
    /// producer/consumer pairs. See `ETLOptions::inflight_bytes`.
    pub fn inflight_bytes(mut self, bytes: usize) -> Self {
        self.opts = self.opts.with_inflight_bytes(bytes);
        self
    }
    /// Override the bounded-channel depth used by bucketing producer/consumer
    /// pairs. See `ETLOptions::inflight_groups`.
    pub fn inflight_groups(mut self, groups: usize) -> Self {
        self.opts = self.opts.with_inflight_groups(groups);
        self
    }
    /// Override the adaptive-memory policy used by bucketing/dedupe producers.
    pub fn adaptive_mem(mut self, cfg: AdaptiveMemCfg) -> Self {
        self.opts = self.opts.with_adaptive_mem(cfg);
        self
    }
    /// Opt in to resumable extract/spool runs: when enabled, supported export
    /// paths read/write a `_progress.json` sidecar keyed by month and by a
    /// fingerprint of the current query/config, so changing filters invalidates
    /// stale parts instead of reusing them. Default false to preserve existing
    /// behavior.
    pub fn resume(mut self, yes: bool) -> Self {
        self.opts = self.opts.with_resume(yes);
        self
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
        }
    }

    pub(crate) fn ensure_work_dir(&self) -> Result<PathBuf> {
        let dir = self
            .opts
            .work_dir
            .clone()
            .unwrap_or_else(|| self.opts.base_dir.join(".reddit_etl_work"));
        fs::create_dir_all(&dir)?;
        Ok(dir)
    }
}

// ----------------- Advanced ScanPlan -----------------

pub struct ScanPlan {
    pub(crate) etl: RedditETL,
    pub(crate) query: QuerySpec,
}

/// Input accepted by [`ScanPlan::author_regex`]. Passing a raw pattern defers
/// compilation until [`ScanPlan::build`], so malformed regexes return a
/// structured [`QueryBuildError`] instead of panicking during builder construction.
#[doc(hidden)]
pub enum AuthorRegexInput {
    Compiled(Regex),
    Pattern(String),
}

#[doc(hidden)]
pub trait IntoAuthorRegex {
    fn into_author_regex(self) -> AuthorRegexInput;
}

impl IntoAuthorRegex for Regex {
    fn into_author_regex(self) -> AuthorRegexInput {
        AuthorRegexInput::Compiled(self)
    }
}

impl IntoAuthorRegex for &str {
    fn into_author_regex(self) -> AuthorRegexInput {
        AuthorRegexInput::Pattern(self.to_string())
    }
}

impl IntoAuthorRegex for String {
    fn into_author_regex(self) -> AuthorRegexInput {
        AuthorRegexInput::Pattern(self)
    }
}

impl IntoAuthorRegex for &String {
    fn into_author_regex(self) -> AuthorRegexInput {
        AuthorRegexInput::Pattern(self.clone())
    }
}

pub(crate) fn log_domain_filter_comment_drop(query: &QuerySpec, sources: Sources) {
    if query.domains_in.is_some() && matches!(sources, Sources::Comments | Sources::Both) {
        tracing::warn!(
            "domains_in filters Reddit's submission-only `domain` field; comment records have no domain and will be dropped. Use sources(Sources::Submissions) to scan only link/submission records."
        );
    }
}

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

    pub fn subreddit(mut self, s: impl AsRef<str>) -> Self {
        self.query.subreddits = Some(vec![normalize_str(s.as_ref())]);
        self
    }
    pub fn subreddits<I, S>(self, iter: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.set_string_list(|q, v| q.subreddits = Some(v), iter, normalize_str)
    }
    pub fn author(mut self, author: impl AsRef<str>) -> Self {
        self.query.authors_in = Some(vec![normalize_str(author.as_ref())]);
        self.query = self.query.normalize();
        self
    }
    pub fn authors<I, S>(self, iter: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.set_string_list(|q, v| q.authors_in = Some(v), iter, normalize_str)
    }
    pub fn authors_in<I, S>(self, iter: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.set_string_list(|q, v| q.authors_in = Some(v), iter, normalize_str)
    }
    pub fn authors_out<I, S>(self, iter: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.set_string_list(|q, v| q.authors_out = Some(v), iter, normalize_str)
    }
    /// Alias for authors_out: exclude the provided authors (normalized).
    pub fn exclude_authors<I, S>(self, iter: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.authors_out(iter)
    }
    /// Convenience: exclude a default set of bot/service accounts, plus any env/file augments.
    ///
    /// This composes with [`ScanPlan::authors_out`] / [`ScanPlan::exclude_authors`]
    /// regardless of call order; the actual merge happens in [`ScanPlan::build`]
    /// so explicit deny-list entries are never overwritten by the defaults.
    pub fn exclude_common_bots(mut self) -> Self {
        self.query.exclude_common_bots = true;
        self
    }
    pub fn author_regex<R: IntoAuthorRegex>(mut self, re: R) -> Self {
        match re.into_author_regex() {
            AuthorRegexInput::Compiled(re) => {
                self.query.author_regex_pattern = Some(re.as_str().to_string());
                self.query.author_regex = Some(re);
            }
            AuthorRegexInput::Pattern(pattern) => {
                self.query.author_regex_pattern = Some(pattern);
                self.query.author_regex = None;
            }
        }
        self
    }
    pub fn min_score(mut self, v: i64) -> Self {
        self.query.min_score = Some(v);
        self
    }
    pub fn max_score(mut self, v: i64) -> Self {
        self.query.max_score = Some(v);
        self
    }
    pub fn keywords_any<I, S>(self, iter: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.set_string_list(|q, v| q.keywords_any = Some(v), iter, lowercase_str)
    }
    /// Restrict to submissions whose top-level `domain` field matches one of
    /// the provided domains (case-insensitive).
    ///
    /// Reddit comments do not carry a `domain` field. When this filter is used
    /// with [`Sources::Comments`] or [`Sources::Both`], comments are rejected
    /// by the filter and a warning is emitted when the plan is built.
    pub fn domains_in<I, S>(self, iter: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.set_string_list(|q, v| q.domains_in = Some(v), iter, lowercase_str)
    }
    pub fn contains_url(mut self, yes: bool) -> Self {
        self.query.contains_url = Some(yes);
        self
    }
    pub fn include_pseudo_users(mut self) -> Self {
        self.query.filter_pseudo_users = false;
        self
    }
    #[deprecated(note = "use include_pseudo_users()")]
    pub fn allow_pseudo_users(self) -> Self {
        self.include_pseudo_users()
    }
    pub fn build(mut self) -> std::result::Result<Self, QueryBuildError> {
        self.query = self.query.normalize();
        if self.query.exclude_common_bots {
            let mut authors_out = self.query.authors_out.take().unwrap_or_default();
            authors_out.extend(default_bot_authors());
            merge_extra_exclusions(&mut authors_out);
            self.query.authors_out = Some(authors_out);
            self.query = self.query.normalize();
        }
        self.query.validate()?;
        self.query = self.query.compile_author_regex()?;
        log_domain_filter_comment_drop(&self.query, self.etl.opts.sources);
        Ok(self)
    }
    pub fn whitelist_fields<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        // correct builder on RedditETL instance
        self.etl = self.etl.whitelist_fields(fields);
        self
    }
    pub fn strict_whitelist(mut self, yes: bool) -> Self {
        self.etl = self.etl.strict_whitelist(yes);
        self
    }
    pub fn strict_key(mut self, yes: bool) -> Self {
        self.etl = self.etl.strict_key(yes);
        self
    }
}

#[inline]
fn lowercase_str(s: &str) -> String {
    s.to_lowercase()
}
