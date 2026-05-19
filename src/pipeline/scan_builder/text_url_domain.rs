impl ScanPlan {
    /// Keep records where at least one keyword appears in `body`, `selftext`, or `title`.
    pub fn keywords_any<I, S>(self, iter: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.set_string_list(|q, v| q.keywords_any = Some(v), iter, lowercase_str)
    }
    /// Keep records only when every keyword appears across `body`, `selftext`, and `title`.
    pub fn keywords_all<I, S>(self, iter: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.set_string_list(|q, v| q.keywords_all = Some(v), iter, lowercase_str)
    }
    /// Reject records where any keyword appears in `body`, `selftext`, or `title`.
    pub fn exclude_keywords<I, S>(self, iter: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.set_string_list(|q, v| q.keywords_exclude = Some(v), iter, lowercase_str)
    }
    /// Keep records where `pattern` matches `body`, `selftext`, or `title`.
    ///
    /// The pattern uses Rust `regex` syntax and is compiled by [`ScanPlan::build`],
    /// so malformed patterns return [`QueryBuildError`] before scanning starts.
    pub fn text_regex(mut self, pattern: impl Into<String>) -> Self {
        self.query.text_regex_pattern = Some(pattern.into());
        self.query.text_regex = None;
        self
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
    /// Keep only records that contain an HTTP(S) URL when `yes` is true.
    ///
    /// Passing `false` clears/disables the positive URL filter. Use
    /// [`ScanPlan::no_url`] for the negative URL predicate.
    pub fn contains_url(mut self, yes: bool) -> Self {
        self.query.contains_url = yes.then_some(true);
        self
    }
    /// Keep only records without an HTTP(S) URL in text and without an outbound
    /// link-submission URL.
    pub fn no_url(mut self) -> Self {
        self.query.no_url = true;
        self
    }
    /// Alias for [`ScanPlan::no_url`].
    pub fn without_url(self) -> Self {
        self.no_url()
    }
}
