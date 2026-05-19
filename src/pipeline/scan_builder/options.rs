impl ScanPlan {
    /// Stop after approximately `n` matching records have been emitted.
    ///
    /// With `file_concurrency > 1`, already-running monthly workers may emit a
    /// small bounded over-shoot before they observe the shared limit.
    pub fn limit(mut self, n: u64) -> Self {
        self.limit = Some(n);
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
