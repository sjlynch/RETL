fn apply_id_source_hints(plan: &mut ScanPlan) {
    if !plan.query.has_id_filters() {
        return;
    }

    match plan.etl.opts.sources {
        Sources::Comments if plan.query.has_submission_id_selectors() => {
            tracing::warn!(
                "record ID filters include t3_ submission IDs, but sources is Comments; those IDs cannot match"
            );
        }
        Sources::Submissions if plan.query.has_comment_id_selectors() => {
            tracing::warn!(
                "record ID filters include t1_ comment IDs, but sources is Submissions; those IDs cannot match"
            );
        }
        Sources::Both => match plan.query.id_source_hint() {
            Some(RecordIdKind::Comment) => {
                tracing::info!(
                    "record ID filters contain only t1_ comment IDs; constraining scan sources to Comments"
                );
                plan.etl.opts.sources = Sources::Comments;
            }
            Some(RecordIdKind::Submission) => {
                tracing::info!(
                    "record ID filters contain only t3_ submission IDs; constraining scan sources to Submissions"
                );
                plan.etl.opts.sources = Sources::Submissions;
            }
            None => {}
        },
        _ => {}
    }
}

impl ScanPlan {
    /// Restrict the scan to records whose top-level Reddit `id` matches one of
    /// the provided selectors.
    ///
    /// Selectors may be bare IDs (`abc123`) or fullnames with `t1_` / `t3_`
    /// prefixes. Prefixed IDs are normalized to the bare record ID and retain a
    /// source constraint, so `t1_abc123` will not accidentally match a
    /// submission with the same bare ID. Duplicate or blank IDs are rejected by
    /// [`ScanPlan::build`].
    ///
    /// Repeated calls **accumulate** selectors (like the other list-builders,
    /// e.g. [`json_predicate`](ScanPlan::json_predicate)); a second call does
    /// not discard IDs supplied by an earlier one.
    pub fn ids<I, S>(self, iter: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.ids_in(iter)
    }
    /// Alias for [`ScanPlan::ids`]; like `ids`, repeated calls accumulate.
    pub fn ids_in<I, S>(mut self, iter: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        // Append onto the unqualified bucket and re-normalize: `normalize`
        // re-buckets every ID list, so previously supplied `t1_`/`t3_`
        // selectors (already split into `comment_ids_in`/`submission_ids_in`)
        // keep their source constraint instead of being wiped.
        let new = iter.into_iter().map(|s| s.as_ref().to_string());
        self.query.ids_in.get_or_insert_with(Vec::new).extend(new);
        self.query = self.query.normalize();
        self
    }
    /// Load record ID selectors from a newline-delimited file and apply them as
    /// an [`ids`](ScanPlan::ids) filter.
    ///
    /// Blank lines and lines whose first non-whitespace character is `#` are
    /// ignored. Inline comments are not stripped. The returned `Result` covers
    /// file I/O and UTF-8/line-length errors; blank or duplicate IDs are still
    /// reported by [`ScanPlan::build`] before scanning starts.
    pub fn ids_file(self, path: impl AsRef<Path>) -> Result<Self> {
        let ids = read_record_ids_file(path.as_ref())?;
        Ok(self.ids_in(ids))
    }
}
