//! Query specification (DSL) and normalization helpers used by the filters.

use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind};
use regex::Regex;
use serde_json::Value;
use std::fmt;
use std::sync::{Arc, OnceLock};

/// Structured error returned when a scan/query builder contains contradictory
/// or invalid filter settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryBuildError {
    message: String,
}

impl QueryBuildError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for QueryBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for QueryBuildError {}

/// Numeric operator used by [`JsonPointerPredicate::number`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericComparison {
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

impl NumericComparison {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GreaterThan => ">",
            Self::GreaterThanOrEqual => ">=",
            Self::LessThan => "<",
            Self::LessThanOrEqual => "<=",
        }
    }

    #[inline]
    fn matches(self, left: f64, right: f64) -> bool {
        match self {
            Self::GreaterThan => left > right,
            Self::GreaterThanOrEqual => left >= right,
            Self::LessThan => left < right,
            Self::LessThanOrEqual => left <= right,
        }
    }
}

#[derive(Debug, Clone)]
enum JsonPredicateKind {
    Exists,
    Equals(Value),
    NotEquals(Value),
    Number {
        op: NumericComparison,
        value: f64,
    },
    Regex {
        pattern: String,
        compiled: Option<Regex>,
    },
}

/// A predicate evaluated against an arbitrary JSON Pointer on the full record.
///
/// These predicates opt a query into the full-parse slow path; scans without
/// any JSON-pointer predicates continue to use only [`MinimalRecord`](crate::MinimalRecord)
/// fast-path fields.
#[derive(Debug, Clone)]
pub struct JsonPointerPredicate {
    pointer: String,
    kind: JsonPredicateKind,
}

impl JsonPointerPredicate {
    /// Match when the pointer resolves to any value, including JSON `null`.
    pub fn exists(pointer: impl Into<String>) -> Self {
        Self {
            pointer: pointer.into(),
            kind: JsonPredicateKind::Exists,
        }
    }

    /// Match when the pointer resolves to the provided scalar JSON value.
    pub fn equals(pointer: impl Into<String>, value: impl Into<Value>) -> Self {
        Self {
            pointer: pointer.into(),
            kind: JsonPredicateKind::Equals(value.into()),
        }
    }

    /// Match when the pointer resolves and is not equal to the provided scalar JSON value.
    pub fn not_equals(pointer: impl Into<String>, value: impl Into<Value>) -> Self {
        Self {
            pointer: pointer.into(),
            kind: JsonPredicateKind::NotEquals(value.into()),
        }
    }

    /// Match when the pointer resolves to a finite number (or numeric string)
    /// satisfying the numeric comparison.
    pub fn number(pointer: impl Into<String>, op: NumericComparison, value: f64) -> Self {
        Self {
            pointer: pointer.into(),
            kind: JsonPredicateKind::Number { op, value },
        }
    }

    /// Match when the pointer resolves to a string matched by `pattern`.
    /// The regex is compiled during [`ScanPlan::build`](crate::ScanPlan::build).
    pub fn regex(pointer: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self {
            pointer: pointer.into(),
            kind: JsonPredicateKind::Regex {
                pattern: pattern.into(),
                compiled: None,
            },
        }
    }

    pub fn pointer(&self) -> &str {
        &self.pointer
    }

    pub(crate) fn validate(&self) -> Result<(), QueryBuildError> {
        validate_json_pointer_syntax(&self.pointer)?;
        match &self.kind {
            JsonPredicateKind::Equals(value) | JsonPredicateKind::NotEquals(value) => {
                if !is_scalar_json(value) {
                    return Err(QueryBuildError::new(format!(
                        "JSON pointer predicate at {:?} uses a non-scalar equality value: {}",
                        self.pointer, value
                    )));
                }
            }
            JsonPredicateKind::Number { value, .. } => {
                if !value.is_finite() {
                    return Err(QueryBuildError::new(format!(
                        "JSON pointer predicate at {:?} uses a non-finite numeric comparison value: {}",
                        self.pointer, value
                    )));
                }
            }
            JsonPredicateKind::Regex { pattern, .. } => {
                Regex::new(pattern).map_err(|e| {
                    QueryBuildError::new(format!(
                        "JSON pointer predicate at {:?} has invalid regex value {:?}: {e}",
                        self.pointer, pattern
                    ))
                })?;
            }
            JsonPredicateKind::Exists => {}
        }
        Ok(())
    }

    pub(crate) fn compile_regex(mut self) -> Result<Self, QueryBuildError> {
        if let JsonPredicateKind::Regex { pattern, compiled } = &mut self.kind {
            let re = Regex::new(pattern).map_err(|e| {
                QueryBuildError::new(format!(
                    "JSON pointer predicate at {:?} has invalid regex value {:?}: {e}",
                    self.pointer, pattern
                ))
            })?;
            *compiled = Some(re);
        }
        Ok(self)
    }

    pub(crate) fn matches(&self, record: &Value) -> bool {
        match &self.kind {
            JsonPredicateKind::Exists => record.pointer(&self.pointer).is_some(),
            JsonPredicateKind::Equals(expected) => record
                .pointer(&self.pointer)
                .is_some_and(|actual| scalar_values_equal(actual, expected)),
            JsonPredicateKind::NotEquals(expected) => record
                .pointer(&self.pointer)
                .is_some_and(|actual| !scalar_values_equal(actual, expected)),
            JsonPredicateKind::Number { op, value } => record
                .pointer(&self.pointer)
                .and_then(value_to_f64)
                .is_some_and(|actual| op.matches(actual, *value)),
            JsonPredicateKind::Regex { pattern, compiled } => {
                let Some(text) = record.pointer(&self.pointer).and_then(Value::as_str) else {
                    return false;
                };
                match compiled {
                    Some(re) => re.is_match(text),
                    None => Regex::new(pattern)
                        .map(|re| re.is_match(text))
                        .unwrap_or(false),
                }
            }
        }
    }

    pub(crate) fn fingerprint_value(&self) -> Value {
        match &self.kind {
            JsonPredicateKind::Exists => serde_json::json!({
                "pointer": self.pointer.as_str(),
                "op": "exists",
            }),
            JsonPredicateKind::Equals(value) => serde_json::json!({
                "pointer": self.pointer.as_str(),
                "op": "=",
                "value": value,
            }),
            JsonPredicateKind::NotEquals(value) => serde_json::json!({
                "pointer": self.pointer.as_str(),
                "op": "!=",
                "value": value,
            }),
            JsonPredicateKind::Number { op, value } => serde_json::json!({
                "pointer": self.pointer.as_str(),
                "op": op.as_str(),
                "value": value,
            }),
            JsonPredicateKind::Regex { pattern, .. } => serde_json::json!({
                "pointer": self.pointer.as_str(),
                "op": "~=",
                "value": pattern,
            }),
        }
    }
}

fn is_scalar_json(value: &Value) -> bool {
    matches!(
        value,
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)
    )
}

fn value_to_f64(value: &Value) -> Option<f64> {
    let n = match value {
        Value::Number(n) => n.as_f64()?,
        Value::String(s) => s.parse::<f64>().ok()?,
        _ => return None,
    };
    n.is_finite().then_some(n)
}

fn scalar_values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(a), Value::Number(b)) => match (a.as_f64(), b.as_f64()) {
            (Some(a), Some(b)) if a.is_finite() && b.is_finite() => a == b,
            _ => left == right,
        },
        _ => left == right,
    }
}

fn validate_json_pointer_syntax(pointer: &str) -> Result<(), QueryBuildError> {
    if !(pointer.is_empty() || pointer.starts_with('/')) {
        return Err(QueryBuildError::new(format!(
            "JSON pointer predicate has bad pointer {:?}; pointers must be empty or start with '/'",
            pointer
        )));
    }

    for token in pointer.split('/').skip(1) {
        let mut chars = token.chars();
        while let Some(ch) = chars.next() {
            if ch == '~' {
                match chars.next() {
                    Some('0') | Some('1') => {}
                    other => {
                        return Err(QueryBuildError::new(format!(
                            "JSON pointer predicate has bad pointer {:?}; '~' must be escaped as '~0' or '~1' (found {:?})",
                            pointer, other
                        )));
                    }
                }
            }
        }
    }

    Ok(())
}

/// High-level query/filter spec for advanced scans.
/// All string lists are matched case-insensitively (Unicode-aware for keywords;
/// subreddit/author/domain matching normalizes non-ASCII values via lowercase).
#[derive(Debug, Default)]
pub struct QuerySpec {
    pub subreddits: Option<Vec<String>>,
    pub authors_in: Option<Vec<String>>,
    pub authors_out: Option<Vec<String>>,
    pub(crate) authors_out_explicit: bool,
    /// When true, [`ScanPlan::build`](crate::ScanPlan::build) merges RETL's
    /// default bot/service deny-list and `ETL_EXCLUDE_AUTHORS*` augments into
    /// `authors_out`. Keeping this as intent until build time avoids
    /// order-dependent builder calls.
    pub exclude_common_bots: bool,
    /// Compiled author regex used by the hot-path filter. Builders that accept
    /// raw patterns also keep `author_regex_pattern` so invalid regexes can be
    /// surfaced as [`QueryBuildError`] from `ScanPlan::build()` instead of panicking
    /// during builder construction.
    pub author_regex: Option<Regex>,
    pub(crate) author_regex_pattern: Option<String>,
    pub min_score: Option<i64>,
    pub max_score: Option<i64>,
    pub keywords_any: Option<Vec<String>>, // substring in body/selftext/title (case-insensitive)
    /// Submissions only: matches the top-level `domain` field. Comments do not
    /// have this field and are rejected when the filter is active.
    pub domains_in: Option<Vec<String>>,
    /// Positive URL-presence filter. `Some(true)` keeps only records with
    /// http(s) in text or a submission `url` whose value starts with http(s).
    /// `Some(false)` is canonicalized to `None` during normalization; RETL
    /// does not currently expose a negative "without URL" filter.
    pub contains_url: Option<bool>,
    /// Full-record predicates evaluated against arbitrary JSON Pointer paths.
    pub json_predicates: Vec<JsonPointerPredicate>,
    pub filter_pseudo_users: bool, // exclude [deleted]/[removed]/empty author; default true

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
            authors_out_explicit: self.authors_out_explicit,
            exclude_common_bots: self.exclude_common_bots,
            author_regex: self.author_regex.clone(),
            author_regex_pattern: self.author_regex_pattern.clone(),
            min_score: self.min_score,
            max_score: self.max_score,
            keywords_any: self.keywords_any.clone(),
            domains_in: self.domains_in.clone(),
            contains_url: self.contains_url,
            json_predicates: self.json_predicates.clone(),
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
            for s in kws.iter_mut() {
                *s = s.trim().to_lowercase();
            }
            kws.sort();
            kws.dedup();
        }
        if let Some(domains) = self.domains_in.as_mut() {
            for s in domains.iter_mut() {
                *s = s.trim().to_lowercase();
            }
            domains.sort();
            domains.dedup();
        }

        // `contains_url(false)` is a no-op/clear request, not a negative URL
        // predicate. Canonicalize direct QuerySpec construction too so resume
        // fingerprints and selectivity match the builder default.
        if self.contains_url == Some(false) {
            self.contains_url = None;
        }

        // Reset any prior compiled cache; keywords may have changed shape.
        self.compiled_keywords = OnceLock::new();

        self
    }

    /// Validate contradictory or malformed query filters before a scan starts.
    /// Error messages name the offending field(s) so callers can surface them
    /// directly to users.
    pub fn validate(&self) -> Result<(), QueryBuildError> {
        if let (Some(min), Some(max)) = (self.min_score, self.max_score) {
            if min > max {
                return Err(QueryBuildError::new(format!(
                    "min_score ({min}) cannot be greater than max_score ({max})"
                )));
            }
        }

        validate_string_list_filter("subreddits", &self.subreddits)?;
        validate_string_list_filter("authors_in", &self.authors_in)?;
        validate_string_list_filter("authors_out", &self.authors_out)?;
        validate_string_list_filter("domains_in", &self.domains_in)?;
        validate_string_list_filter("keywords_any", &self.keywords_any)?;

        if let (Some(allow), Some(deny)) = (&self.authors_in, &self.authors_out) {
            if let Some(author) = allow.iter().find(|a| deny.iter().any(|d| d == *a)) {
                return Err(QueryBuildError::new(format!(
                    "authors_in and authors_out both contain '{author}'"
                )));
            }
        }

        if let Some(pattern) = &self.author_regex_pattern {
            Regex::new(pattern).map_err(|e| {
                QueryBuildError::new(format!("author_regex is invalid: {e}; pattern={pattern:?}"))
            })?;
        }

        for predicate in &self.json_predicates {
            predicate.validate()?;
        }

        Ok(())
    }

    pub(crate) fn compile_author_regex(mut self) -> Result<Self, QueryBuildError> {
        if let Some(pattern) = &self.author_regex_pattern {
            let re = Regex::new(pattern).map_err(|e| {
                QueryBuildError::new(format!("author_regex is invalid: {e}; pattern={pattern:?}"))
            })?;
            self.author_regex = Some(re);
        }
        Ok(self)
    }

    pub(crate) fn compile_json_predicates(mut self) -> Result<Self, QueryBuildError> {
        self.json_predicates = self
            .json_predicates
            .into_iter()
            .map(JsonPointerPredicate::compile_regex)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(self)
    }

    /// Only JSON-pointer predicates require full-record parsing; all fixed
    /// filters are handled on the MinimalRecord fast path.
    pub fn requires_full_parse(&self) -> bool {
        !self.json_predicates.is_empty()
    }

    /// Returns a lazily-built case-insensitive Aho-Corasick automaton over
    /// `keywords_any`, or `None` when there are no keywords to match.
    /// The automaton is built once per `QuerySpec` and reused across records.
    pub fn keywords_automaton(&self) -> Option<&AhoCorasick> {
        let kws = self.keywords_any.as_ref()?;
        if kws.is_empty() {
            return None;
        }
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

    pub(crate) fn keywords_all_ascii(&self) -> bool {
        self.keywords_any
            .as_ref()
            .map_or(true, |kws| kws.iter().all(|kw| kw.is_ascii()))
    }

    pub(crate) fn json_predicates_fingerprint(&self) -> Vec<Value> {
        self.json_predicates
            .iter()
            .map(JsonPointerPredicate::fingerprint_value)
            .collect()
    }

    pub(crate) fn has_selective_filters(&self) -> bool {
        self.subreddits.as_ref().is_some_and(|v| !v.is_empty())
            || self.authors_in.as_ref().is_some_and(|v| !v.is_empty())
            || self.author_regex.is_some()
            || self.author_regex_pattern.is_some()
            || (self.authors_out_explicit
                && self.authors_out.as_ref().is_some_and(|v| !v.is_empty()))
            || self.min_score.is_some()
            || self.max_score.is_some()
            || self.keywords_any.as_ref().is_some_and(|v| !v.is_empty())
            || self.domains_in.as_ref().is_some_and(|v| !v.is_empty())
            || self.contains_url == Some(true)
            || !self.json_predicates.is_empty()
    }
}

#[inline]
fn validate_string_list_filter(
    field: &'static str,
    value: &Option<Vec<String>>,
) -> Result<(), QueryBuildError> {
    let Some(list) = value else {
        return Ok(());
    };
    if list.is_empty() {
        return Err(QueryBuildError::new(format!(
            "{field} cannot be an empty list; omit {field} to match all"
        )));
    }
    if list.iter().any(|s| s.is_empty()) {
        return Err(QueryBuildError::new(format!(
            "{field} contains a blank entry after normalization; blank entries are not allowed"
        )));
    }
    Ok(())
}

pub fn normalize_str(s: &str) -> String {
    let s = s.trim().to_lowercase();
    if let Some(rest) = s.strip_prefix("r/") {
        rest.to_string()
    } else {
        s
    }
}
