//! Query specification (DSL) and normalization helpers used by the filters.

use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind};
use regex::Regex;
use std::sync::{Arc, OnceLock};

/// High-level query/filter spec for advanced scans.
/// All string lists are matched case-insensitively (we store normalized lowercase).
#[derive(Debug, Default)]
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

    // Lazily-built case-insensitive automaton over `keywords_any`.
    // Built once per QuerySpec on first call to `keywords_automaton()`.
    pub(crate) compiled_keywords: OnceLock<Arc<AhoCorasick>>,
}

impl Clone for QuerySpec {
    fn clone(&self) -> Self {
        let cache: OnceLock<Arc<AhoCorasick>> = OnceLock::new();
        if let Some(ac) = self.compiled_keywords.get() {
            let _ = cache.set(Arc::clone(ac));
        }
        Self {
            subreddits: self.subreddits.clone(),
            authors_in: self.authors_in.clone(),
            authors_out: self.authors_out.clone(),
            author_regex: self.author_regex.clone(),
            min_score: self.min_score,
            max_score: self.max_score,
            keywords_any: self.keywords_any.clone(),
            domains_in: self.domains_in.clone(),
            contains_url: self.contains_url,
            filter_pseudo_users: self.filter_pseudo_users,
            compiled_keywords: cache,
        }
    }
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
            kws.retain(|s| !s.is_empty());
            kws.sort(); kws.dedup();
        }
        if let Some(domains) = self.domains_in.as_mut() {
            for s in domains.iter_mut() { *s = s.trim().to_lowercase(); }
            domains.sort(); domains.dedup();
        }

        // Reset any prior compiled cache; keywords may have changed shape.
        self.compiled_keywords = OnceLock::new();

        self
    }

    /// All common filters, including domains_in, are handled on the MinimalRecord fast path.
    pub fn requires_full_parse(&self) -> bool {
        false
    }

    /// Returns a lazily-built case-insensitive Aho-Corasick automaton over
    /// `keywords_any`, or `None` when there are no keywords to match.
    /// The automaton is built once per `QuerySpec` and reused across records.
    pub fn keywords_automaton(&self) -> Option<&AhoCorasick> {
        let kws = self.keywords_any.as_ref()?;
        if kws.is_empty() { return None; }
        let arc = self.compiled_keywords.get_or_init(|| {
            let ac = AhoCorasickBuilder::new()
                .ascii_case_insensitive(true)
                .match_kind(MatchKind::LeftmostFirst)
                .build(kws.iter())
                .expect("aho-corasick build from non-empty keyword list");
            Arc::new(ac)
        });
        Some(arc.as_ref())
    }
}

#[inline]
pub fn normalize_str(s: &str) -> String {
    let s = s.trim().to_lowercase();
    if let Some(rest) = s.strip_prefix("r/") { rest.to_string() } else { s }
}
