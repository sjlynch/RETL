impl ScanPlan {
    pub fn min_score(mut self, v: i64) -> Self {
        self.query.min_score = Some(v);
        self
    }
    pub fn max_score(mut self, v: i64) -> Self {
        self.query.max_score = Some(v);
        self
    }
    /// Set an inclusive lower bound for the top-level `created_utc` Unix timestamp.
    ///
    /// This is evaluated on the MinimalRecord fast path and does not require a
    /// full JSON parse. Records without an integer `created_utc` are rejected
    /// when any timestamp bound is active.
    pub fn created_utc_gte(mut self, ts: i64) -> Self {
        self.query.timestamp_bounds.created_utc_gte = Some(ts);
        self
    }
    /// Set an exclusive upper bound for the top-level `created_utc` Unix timestamp.
    ///
    /// This is evaluated on the MinimalRecord fast path and does not require a
    /// full JSON parse. Records without an integer `created_utc` are rejected
    /// when any timestamp bound is active.
    pub fn created_utc_lt(mut self, ts: i64) -> Self {
        self.query.timestamp_bounds.created_utc_lt = Some(ts);
        self
    }
    /// Set exact `created_utc` bounds (`>= created_utc_gte`, `< created_utc_lt`).
    pub fn timestamp_bounds(
        mut self,
        created_utc_gte: Option<i64>,
        created_utc_lt: Option<i64>,
    ) -> Self {
        self.query.timestamp_bounds = TimestampBounds::new(created_utc_gte, created_utc_lt);
        self
    }
    /// Alias for [`ScanPlan::created_utc_gte`].
    pub fn after(self, ts: i64) -> Self {
        self.created_utc_gte(ts)
    }
    /// Alias for [`ScanPlan::created_utc_lt`].
    pub fn before(self, ts: i64) -> Self {
        self.created_utc_lt(ts)
    }
}
