impl ScanPlan {
    pub fn build(mut self) -> std::result::Result<Self, QueryBuildError> {
        if self
            .etl
            .opts
            .subreddit
            .as_deref()
            .is_some_and(str::is_empty)
        {
            return Err(QueryBuildError::new(
                "subreddits contains a blank entry after normalization; blank entries are not allowed",
            ));
        }

        self.query = self.query.normalize();
        self.query.validate()?;
        if self.query.exclude_common_bots {
            let mut authors_out = self.query.authors_out.take().unwrap_or_default();
            authors_out.extend(default_bot_authors());
            try_merge_extra_exclusions(&mut authors_out)?;
            self.query.authors_out = Some(authors_out);
            self.query = self.query.normalize();
            self.query.validate()?;
        }
        self.query = self.query.compile_author_regex()?;
        self.query = self.query.compile_text_regex()?;
        self.query = self.query.compile_json_predicates()?;
        apply_id_source_hints(&mut self);
        log_domain_filter_comment_drop(&self.query, self.etl.opts.sources);
        Ok(self)
    }
}
