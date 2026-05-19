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
}

#[inline]
fn lowercase_str(s: &str) -> String {
    s.to_lowercase()
}
