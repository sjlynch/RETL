//! Query specification (DSL) and normalization helpers used by the filters.

use regex::Regex;

/// High-level query/filter spec for advanced scans.
/// All string lists are matched case-insensitively (we store normalized lowercase).
#[derive(Clone, Debug, Default)]
pub struct QuerySpec {
    pub subreddits: Option<Vec<String>>,
    pub authors_in: Option<Vec<String>>,
    pub authors_out: Option<Vec<String>>,
    pub author_regex: Option<Regex>,
    pub min_score: Option<i64>,
    pub max_score: Option<i64>,
    pub keywords_any: Option<Vec<String>>, // substring in body/selftext/title (case-insensitive)
    pub domains_in: Option<Vec<String>>,   // submissions only (domain field)
    pub contains_url: Option<bool>,        // if true, keep only records with http(s)
    pub filter_pseudo_users: bool,         // exclude [deleted]/[removed]; default true
}

impl QuerySpec {
    /// Normalize to lowercase, then sort + dedup for binary_search-based filters.
    pub fn normalize(mut self) -> Self {
        let lower_sort_dedup = |v: &mut Option<Vec<String>>| {
            if let Some(list) = v.as_mut() {
                for s in list.iter_mut() {
                    *s = normalize_str(s);
                }
                list.sort();
                list.dedup();
            }
        };

        lower_sort_dedup(&mut self.subreddits);
        lower_sort_dedup(&mut self.authors_in);
        lower_sort_dedup(&mut self.authors_out);

        if let Some(kws) = self.keywords_any.as_mut() {
            for s in kws.iter_mut() { *s = s.trim().to_lowercase(); }
            kws.sort(); kws.dedup();
        }
        if let Some(domains) = self.domains_in.as_mut() {
            for s in domains.iter_mut() { *s = s.trim().to_lowercase(); }
            domains.sort(); domains.dedup();
        }

        self
    }

    /// All common filters, including domains_in, are handled on the MinimalRecord fast path.
    pub fn requires_full_parse(&self) -> bool {
        false
    }
}

#[inline]
pub fn normalize_str(s: &str) -> String {
    let s = s.trim().to_lowercase();
    if let Some(rest) = s.strip_prefix("r/") { rest.to_string() } else { s }
}
