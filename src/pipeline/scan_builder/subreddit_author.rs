impl ScanPlan {
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
    pub fn authors_out<I, S>(mut self, iter: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self = self.set_string_list(|q, v| q.authors_out = Some(v), iter, normalize_str);
        self.query.authors_out_explicit = true;
        self
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
}
