//! Query specification (DSL) and normalization helpers used by the filters.

use crate::date::YearMonth;
use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind};
use anyhow::Context;
use regex::Regex;
use serde_json::Value;
use std::borrow::Cow;
use std::fmt;
use std::io::BufReader;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use time::OffsetDateTime;

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

/// Source kind encoded by Reddit fullname prefixes accepted by record-ID
/// filters. `t1_` selects comments and `t3_` selects submissions; unprefixed
/// IDs remain source-agnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordIdKind {
    Comment,
    Submission,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NormalizedRecordId {
    pub(crate) bare: String,
    pub(crate) kind: Option<RecordIdKind>,
}

fn split_record_kind_prefix(s: &str) -> (Option<RecordIdKind>, &str) {
    let bytes = s.as_bytes();
    if bytes.len() >= 3 && bytes[2] == b'_' && bytes[0].eq_ignore_ascii_case(&b't') {
        match bytes[1] {
            b'1' => return (Some(RecordIdKind::Comment), &s[3..]),
            b'3' => return (Some(RecordIdKind::Submission), &s[3..]),
            _ => {}
        }
    }
    (None, s)
}

fn lowercase_ascii_if_needed(s: &str) -> Cow<'_, str> {
    if s.bytes().any(|b| b.is_ascii_uppercase()) {
        Cow::Owned(s.to_lowercase())
    } else {
        Cow::Borrowed(s)
    }
}

/// Normalize a user-provided Reddit record ID selector.
///
/// Inputs are trimmed, `t1_` / `t3_` fullname prefixes are stripped and kept as
/// kind constraints, and bare IDs are lowercased for Reddit's base36 IDs. Other
/// prefixes (for example `t5_`) are treated as part of the bare ID so callers do
/// not accidentally strip unsupported fullname kinds.
pub(crate) fn normalize_record_id_selector(raw: &str) -> NormalizedRecordId {
    let trimmed = raw.trim();
    let (kind, bare) = split_record_kind_prefix(trimmed);
    let bare = bare.trim();
    NormalizedRecordId {
        bare: lowercase_ascii_if_needed(bare).into_owned(),
        kind,
    }
}

fn normalize_record_id_value(raw: &str) -> Cow<'_, str> {
    let trimmed = raw.trim();
    let (_kind, bare) = split_record_kind_prefix(trimmed);
    lowercase_ascii_if_needed(bare.trim())
}

/// Read a newline-delimited record-ID file.
///
/// Each non-empty, non-comment line is one ID selector. Leading/trailing
/// whitespace is trimmed; blank lines and lines whose first non-whitespace
/// character is `#` are ignored. Inline comments are not stripped, so put one
/// bare ID (or `t1_` / `t3_` fullname) per line.
pub fn read_record_ids_file(path: impl AsRef<Path>) -> anyhow::Result<Vec<String>> {
    let path = path.as_ref();
    let file = crate::util::open_with_backoff(path, 16, 50)
        .with_context(|| format!("open IDs file {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    let mut ids = Vec::new();
    let mut line_number = 0_u64;

    loop {
        let next_line = line_number + 1;
        let read = crate::ndjson::read_line_capped(
            &mut reader,
            &mut line,
            crate::ndjson::DEFAULT_MAX_LINE_BYTES,
            path,
        )
        .with_context(|| format!("read IDs file {} line {}", path.display(), next_line))?;
        if read == 0 {
            break;
        }
        line_number = next_line;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        ids.push(trimmed.to_string());
    }

    Ok(ids)
}

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

/// Inclusive/exclusive fast-path bounds for a record's top-level
/// `created_utc` Unix timestamp.
///
/// `created_utc_gte` is inclusive (`created_utc >= value`) and
/// `created_utc_lt` is exclusive (`created_utc < value`). When either side is
/// present, records whose `created_utc` is missing or not an integer timestamp
/// are rejected on the [`MinimalRecord`](crate::MinimalRecord) fast path.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TimestampBounds {
    pub created_utc_gte: Option<i64>,
    pub created_utc_lt: Option<i64>,
}

impl TimestampBounds {
    pub const fn new(created_utc_gte: Option<i64>, created_utc_lt: Option<i64>) -> Self {
        Self {
            created_utc_gte,
            created_utc_lt,
        }
    }

    #[inline]
    pub fn is_active(self) -> bool {
        self.created_utc_gte.is_some() || self.created_utc_lt.is_some()
    }

    #[inline]
    pub fn contains(self, created_utc: i64) -> bool {
        if let Some(lo) = self.created_utc_gte {
            if created_utc < lo {
                return false;
            }
        }
        if let Some(hi) = self.created_utc_lt {
            if created_utc >= hi {
                return false;
            }
        }
        true
    }

    pub(crate) fn validate(self) -> Result<(), QueryBuildError> {
        if let (Some(lo), Some(hi)) = (self.created_utc_gte, self.created_utc_lt) {
            if lo >= hi {
                return Err(QueryBuildError::new(format!(
                    "created_utc_gte ({lo}) must be less than created_utc_lt ({hi})"
                )));
            }
        }

        for (field, value) in [
            ("created_utc_gte", self.created_utc_gte),
            ("created_utc_lt", self.created_utc_lt),
        ] {
            if let Some(ts) = value {
                unix_timestamp_to_year_month(ts).ok_or_else(|| {
                    QueryBuildError::new(format!(
                        "{field} ({ts}) is outside RETL's supported timestamp range"
                    ))
                })?;
            }
        }

        Ok(())
    }

    pub(crate) fn derived_start_month(self) -> Option<YearMonth> {
        self.created_utc_gte.and_then(unix_timestamp_to_year_month)
    }

    pub(crate) fn derived_end_month(self) -> Option<YearMonth> {
        self.created_utc_lt
            .and_then(|ts| ts.checked_sub(1))
            .and_then(unix_timestamp_to_year_month)
    }
}

fn unix_timestamp_to_year_month(ts: i64) -> Option<YearMonth> {
    let dt = OffsetDateTime::from_unix_timestamp(ts).ok()?;
    let year = dt.year();
    if !(0..=u16::MAX as i32).contains(&year) {
        return None;
    }
    Some(YearMonth {
        year: year as u16,
        month: dt.month() as u8,
    })
}

/// High-level query/filter spec for advanced scans.
/// All string lists are matched case-insensitively (Unicode-aware for keywords;
/// subreddit/author/domain matching normalizes non-ASCII values via lowercase).
#[derive(Debug, Default)]
pub struct QuerySpec {
    pub subreddits: Option<Vec<String>>,
    /// Bare Reddit record IDs (without `t1_` / `t3_`) that match either source.
    /// Prefer [`ScanPlan::ids`](crate::ScanPlan::ids) /
    /// [`ScanPlan::ids_in`](crate::ScanPlan::ids_in) when accepting user input;
    /// those builders also understand prefixed fullnames and preserve the
    /// source constraint.
    pub ids_in: Option<Vec<String>>,
    pub(crate) comment_ids_in: Option<Vec<String>>,
    pub(crate) submission_ids_in: Option<Vec<String>>,
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
    /// Exact Unix timestamp bounds for the top-level `created_utc` field.
    /// Lower bound is inclusive; upper bound is exclusive.
    pub timestamp_bounds: TimestampBounds,
    /// Keep records where at least one keyword is present in `body`, `selftext`, or `title`.
    pub keywords_any: Option<Vec<String>>, // substring in body/selftext/title (case-insensitive)
    /// Keep records only when every keyword is present across `body`, `selftext`, and `title`.
    pub keywords_all: Option<Vec<String>>,
    /// Reject records when any excluded keyword is present in `body`, `selftext`, or `title`.
    pub keywords_exclude: Option<Vec<String>>,
    /// Regex matched against `body`, `selftext`, or `title` on the MinimalRecord fast path.
    pub text_regex: Option<Regex>,
    pub(crate) text_regex_pattern: Option<String>,
    /// Submissions only: matches the top-level `domain` field. Comments do not
    /// have this field and are rejected when the filter is active.
    pub domains_in: Option<Vec<String>>,
    /// Positive URL-presence filter. `Some(true)` keeps only records with
    /// http(s) in text or a link-submission `url` whose value starts with
    /// http(s). `Some(false)` is canonicalized to `None` during normalization;
    /// use [`QuerySpec::no_url`] / [`ScanPlan::no_url`](crate::ScanPlan::no_url)
    /// for the negative predicate.
    pub contains_url: Option<bool>,
    /// Negative URL-presence filter. When true, rejects records with http(s) in
    /// text or a link-submission `url` whose value starts with http(s).
    pub no_url: bool,
    /// Full-record predicates evaluated against arbitrary JSON Pointer paths.
    pub json_predicates: Vec<JsonPointerPredicate>,
    pub filter_pseudo_users: bool, // exclude [deleted]/[removed]/empty author; default true

    // Lazily-built case-insensitive automatons over keyword families.
    // Built once per QuerySpec on first call to the corresponding accessor.
    pub(crate) compiled_keywords_any: OnceLock<Arc<AhoCorasick>>,
    pub(crate) compiled_keywords_all: OnceLock<Arc<AhoCorasick>>,
    pub(crate) compiled_keywords_exclude: OnceLock<Arc<AhoCorasick>>,
}

fn clone_keyword_cache(cache: &OnceLock<Arc<AhoCorasick>>) -> OnceLock<Arc<AhoCorasick>> {
    let cloned = OnceLock::new();
    if let Some(ac) = cache.get() {
        let _ = cloned.set(Arc::clone(ac));
    }
    cloned
}

impl Clone for QuerySpec {
    fn clone(&self) -> Self {
        Self {
            subreddits: self.subreddits.clone(),
            ids_in: self.ids_in.clone(),
            comment_ids_in: self.comment_ids_in.clone(),
            submission_ids_in: self.submission_ids_in.clone(),
            authors_in: self.authors_in.clone(),
            authors_out: self.authors_out.clone(),
            authors_out_explicit: self.authors_out_explicit,
            exclude_common_bots: self.exclude_common_bots,
            author_regex: self.author_regex.clone(),
            author_regex_pattern: self.author_regex_pattern.clone(),
            min_score: self.min_score,
            max_score: self.max_score,
            timestamp_bounds: self.timestamp_bounds,
            keywords_any: self.keywords_any.clone(),
            keywords_all: self.keywords_all.clone(),
            keywords_exclude: self.keywords_exclude.clone(),
            text_regex: self.text_regex.clone(),
            text_regex_pattern: self.text_regex_pattern.clone(),
            domains_in: self.domains_in.clone(),
            contains_url: self.contains_url,
            no_url: self.no_url,
            json_predicates: self.json_predicates.clone(),
            filter_pseudo_users: self.filter_pseudo_users,
            compiled_keywords_any: clone_keyword_cache(&self.compiled_keywords_any),
            compiled_keywords_all: clone_keyword_cache(&self.compiled_keywords_all),
            compiled_keywords_exclude: clone_keyword_cache(&self.compiled_keywords_exclude),
        }
    }
}

impl QuerySpec {
    /// Normalize to lowercase and sort list filters. Most string lists are
    /// deduplicated; record-ID filters intentionally keep duplicates so
    /// [`QuerySpec::validate`] can reject accidental repeated IDs.
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
        normalize_id_filters(&mut self);
        lower_sort_dedup(&mut self.authors_in);
        lower_sort_dedup(&mut self.authors_out);

        normalize_trim_lower_list(&mut self.keywords_any);
        normalize_trim_lower_list(&mut self.keywords_all);
        normalize_trim_lower_list(&mut self.keywords_exclude);
        normalize_trim_lower_list(&mut self.domains_in);

        // `contains_url(false)` is a no-op/clear request, not a negative URL
        // predicate. Canonicalize direct QuerySpec construction too so resume
        // fingerprints and selectivity match the builder default.
        if self.contains_url == Some(false) {
            self.contains_url = None;
        }

        // Reset any prior compiled caches; keywords may have changed shape.
        self.compiled_keywords_any = OnceLock::new();
        self.compiled_keywords_all = OnceLock::new();
        self.compiled_keywords_exclude = OnceLock::new();

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
        self.timestamp_bounds.validate()?;

        validate_string_list_filter("subreddits", &self.subreddits)?;
        validate_id_list_filter("ids_in", &self.ids_in)?;
        validate_id_list_filter("ids_in", &self.comment_ids_in)?;
        validate_id_list_filter("ids_in", &self.submission_ids_in)?;
        validate_id_filter_overlaps(self)?;
        validate_string_list_filter("authors_in", &self.authors_in)?;
        validate_string_list_filter("authors_out", &self.authors_out)?;
        validate_string_list_filter("domains_in", &self.domains_in)?;
        validate_string_list_filter("keywords_any", &self.keywords_any)?;
        validate_string_list_filter("keywords_all", &self.keywords_all)?;
        validate_string_list_filter("keywords_exclude", &self.keywords_exclude)?;

        if self.contains_url == Some(true) && self.no_url {
            return Err(QueryBuildError::new(
                "contains_url and no_url cannot both be set; choose either --contains-url or --no-url",
            ));
        }

        validate_text_regex_filter(&self.text_regex_pattern, &self.text_regex)?;

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

    pub(crate) fn compile_text_regex(mut self) -> Result<Self, QueryBuildError> {
        if let Some(pattern) = self.text_regex_pattern.clone() {
            validate_text_regex_pattern(&pattern)?;
            let re = Regex::new(&pattern).map_err(|e| {
                QueryBuildError::new(format!("text_regex is invalid: {e}; pattern={pattern:?}"))
            })?;
            self.text_regex = Some(re);
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
    /// filters, including `created_utc` timestamp bounds, are handled on the
    /// MinimalRecord fast path.
    pub fn requires_full_parse(&self) -> bool {
        !self.json_predicates.is_empty()
    }

    /// Returns a lazily-built case-insensitive Aho-Corasick automaton over
    /// `keywords_any`, or `None` when there are no keywords to match.
    /// The automaton is built once per `QuerySpec` and reused across records.
    pub fn keywords_automaton(&self) -> Option<&AhoCorasick> {
        self.keywords_any_automaton()
    }

    pub fn keywords_any_automaton(&self) -> Option<&AhoCorasick> {
        keyword_automaton_for(
            &self.keywords_any,
            &self.compiled_keywords_any,
            MatchKind::LeftmostFirst,
        )
    }

    pub(crate) fn keywords_all_automaton(&self) -> Option<&AhoCorasick> {
        keyword_automaton_for(
            &self.keywords_all,
            &self.compiled_keywords_all,
            MatchKind::Standard,
        )
    }

    pub(crate) fn keywords_exclude_automaton(&self) -> Option<&AhoCorasick> {
        keyword_automaton_for(
            &self.keywords_exclude,
            &self.compiled_keywords_exclude,
            MatchKind::LeftmostFirst,
        )
    }

    pub(crate) fn keywords_any_all_ascii(&self) -> bool {
        keyword_list_all_ascii(&self.keywords_any)
    }

    pub(crate) fn keywords_all_all_ascii(&self) -> bool {
        keyword_list_all_ascii(&self.keywords_all)
    }

    pub(crate) fn keywords_exclude_all_ascii(&self) -> bool {
        keyword_list_all_ascii(&self.keywords_exclude)
    }

    pub(crate) fn has_unqualified_id_selectors(&self) -> bool {
        self.ids_in.as_ref().is_some_and(|v| !v.is_empty())
    }

    pub(crate) fn has_comment_id_selectors(&self) -> bool {
        self.comment_ids_in.as_ref().is_some_and(|v| !v.is_empty())
    }

    pub(crate) fn has_submission_id_selectors(&self) -> bool {
        self.submission_ids_in
            .as_ref()
            .is_some_and(|v| !v.is_empty())
    }

    pub(crate) fn has_id_filters(&self) -> bool {
        self.has_unqualified_id_selectors()
            || self.has_comment_id_selectors()
            || self.has_submission_id_selectors()
    }

    pub(crate) fn id_source_hint(&self) -> Option<RecordIdKind> {
        if self.has_unqualified_id_selectors() {
            return None;
        }
        match (
            self.has_comment_id_selectors(),
            self.has_submission_id_selectors(),
        ) {
            (true, false) => Some(RecordIdKind::Comment),
            (false, true) => Some(RecordIdKind::Submission),
            _ => None,
        }
    }

    pub(crate) fn id_filter_matches(&self, kind: RecordIdKind, raw_id: Option<&str>) -> bool {
        if !self.has_id_filters() {
            return true;
        }
        let Some(raw_id) = raw_id else {
            return false;
        };
        let bare = normalize_record_id_value(raw_id);
        if bare.is_empty() {
            return false;
        }
        sorted_id_list_contains(self.ids_in.as_ref(), &bare)
            || match kind {
                RecordIdKind::Comment => {
                    sorted_id_list_contains(self.comment_ids_in.as_ref(), &bare)
                }
                RecordIdKind::Submission => {
                    sorted_id_list_contains(self.submission_ids_in.as_ref(), &bare)
                }
            }
    }

    pub(crate) fn json_predicates_fingerprint(&self) -> Vec<Value> {
        self.json_predicates
            .iter()
            .map(JsonPointerPredicate::fingerprint_value)
            .collect()
    }

    pub(crate) fn has_selective_filters(&self) -> bool {
        self.subreddits.as_ref().is_some_and(|v| !v.is_empty())
            || self.has_id_filters()
            || self.authors_in.as_ref().is_some_and(|v| !v.is_empty())
            || self.author_regex.is_some()
            || self.author_regex_pattern.is_some()
            || (self.authors_out_explicit
                && self.authors_out.as_ref().is_some_and(|v| !v.is_empty()))
            || self.min_score.is_some()
            || self.max_score.is_some()
            || self.timestamp_bounds.is_active()
            || self.keywords_any.as_ref().is_some_and(|v| !v.is_empty())
            || self.keywords_all.as_ref().is_some_and(|v| !v.is_empty())
            || self
                .keywords_exclude
                .as_ref()
                .is_some_and(|v| !v.is_empty())
            || self.text_regex.is_some()
            || self.text_regex_pattern.is_some()
            || self.domains_in.as_ref().is_some_and(|v| !v.is_empty())
            || self.contains_url == Some(true)
            || self.no_url
            || !self.json_predicates.is_empty()
    }
}

fn keyword_automaton_for<'a>(
    keywords: &'a Option<Vec<String>>,
    cache: &'a OnceLock<Arc<AhoCorasick>>,
    match_kind: MatchKind,
) -> Option<&'a AhoCorasick> {
    let kws = keywords.as_ref()?;
    if kws.is_empty() {
        return None;
    }
    let arc = cache.get_or_init(|| {
        let ac = AhoCorasickBuilder::new()
            .ascii_case_insensitive(true)
            .match_kind(match_kind)
            .build(kws.iter())
            .expect("aho-corasick build from non-empty keyword list");
        Arc::new(ac)
    });
    Some(arc.as_ref())
}

#[inline]
fn keyword_list_all_ascii(keywords: &Option<Vec<String>>) -> bool {
    keywords
        .as_ref()
        .map_or(true, |kws| kws.iter().all(|kw| kw.is_ascii()))
}

fn normalize_trim_lower_list(value: &mut Option<Vec<String>>) {
    if let Some(list) = value.as_mut() {
        for s in list.iter_mut() {
            *s = s.trim().to_lowercase();
        }
        list.sort();
        list.dedup();
    }
}

fn validate_text_regex_pattern(pattern: &str) -> Result<(), QueryBuildError> {
    if pattern.trim().is_empty() {
        return Err(QueryBuildError::new(
            "text_regex contains a blank pattern; blank regex patterns are not allowed",
        ));
    }
    Regex::new(pattern).map_err(|e| {
        QueryBuildError::new(format!("text_regex is invalid: {e}; pattern={pattern:?}"))
    })?;
    Ok(())
}

fn validate_text_regex_filter(
    pattern: &Option<String>,
    compiled: &Option<Regex>,
) -> Result<(), QueryBuildError> {
    if let Some(pattern) = pattern {
        validate_text_regex_pattern(pattern)?;
    }
    if let Some(re) = compiled {
        if re.as_str().trim().is_empty() {
            return Err(QueryBuildError::new(
                "text_regex contains a blank pattern; blank regex patterns are not allowed",
            ));
        }
    }
    Ok(())
}

fn sort_id_list(list: &mut Vec<String>) {
    list.sort();
}

fn normalize_id_filters(query: &mut QuerySpec) {
    let had_any_id_field = query.ids_in.is_some()
        || query.comment_ids_in.is_some()
        || query.submission_ids_in.is_some();

    let mut ids_any = Vec::new();
    let mut ids_comments = Vec::new();
    let mut ids_submissions = Vec::new();

    if let Some(list) = query.ids_in.take() {
        for raw in list {
            let normalized = normalize_record_id_selector(&raw);
            match normalized.kind {
                Some(RecordIdKind::Comment) => ids_comments.push(normalized.bare),
                Some(RecordIdKind::Submission) => ids_submissions.push(normalized.bare),
                None => ids_any.push(normalized.bare),
            }
        }
    }
    if let Some(list) = query.comment_ids_in.take() {
        for raw in list {
            ids_comments.push(normalize_record_id_selector(&raw).bare);
        }
    }
    if let Some(list) = query.submission_ids_in.take() {
        for raw in list {
            ids_submissions.push(normalize_record_id_selector(&raw).bare);
        }
    }

    sort_id_list(&mut ids_any);
    sort_id_list(&mut ids_comments);
    sort_id_list(&mut ids_submissions);

    let all_empty = ids_any.is_empty() && ids_comments.is_empty() && ids_submissions.is_empty();
    query.ids_in = if !ids_any.is_empty() || (had_any_id_field && all_empty) {
        Some(ids_any)
    } else {
        None
    };
    query.comment_ids_in = (!ids_comments.is_empty()).then_some(ids_comments);
    query.submission_ids_in = (!ids_submissions.is_empty()).then_some(ids_submissions);
}

fn sorted_id_list_contains(list: Option<&Vec<String>>, needle: &str) -> bool {
    list.is_some_and(|list| {
        list.binary_search_by(|candidate| candidate.as_str().cmp(needle))
            .is_ok()
    })
}

fn validate_id_filter_overlaps(query: &QuerySpec) -> Result<(), QueryBuildError> {
    let Some(any_ids) = query.ids_in.as_ref() else {
        return Ok(());
    };
    for id in any_ids {
        if sorted_id_list_contains(query.comment_ids_in.as_ref(), id)
            || sorted_id_list_contains(query.submission_ids_in.as_ref(), id)
        {
            return Err(QueryBuildError::new(format!(
                "ids_in contains duplicate ID '{id}' after normalization"
            )));
        }
    }
    Ok(())
}

fn validate_id_list_filter(
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
    for id in list {
        if id.trim().is_empty() {
            return Err(QueryBuildError::new(format!(
                "{field} contains a blank entry after normalization; blank entries are not allowed"
            )));
        }
    }

    let mut sorted = list.clone();
    sorted.sort();
    if let Some(duplicate) = sorted
        .windows(2)
        .find_map(|pair| (pair[0] == pair[1]).then(|| pair[0].clone()))
    {
        return Err(QueryBuildError::new(format!(
            "{field} contains duplicate ID '{duplicate}' after normalization"
        )));
    }

    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_bounds_validate_order_and_stay_minimal() {
        let query = QuerySpec {
            timestamp_bounds: TimestampBounds::new(Some(1_606_737_600), Some(1_606_824_000)),
            ..Default::default()
        };
        query.validate().expect("valid timestamp range");
        assert!(query.has_selective_filters());
        assert!(
            !query.requires_full_parse(),
            "created_utc timestamp bounds are a MinimalRecord fast-path filter"
        );

        let invalid = QuerySpec {
            timestamp_bounds: TimestampBounds::new(Some(10), Some(10)),
            ..Default::default()
        };
        let err = invalid
            .validate()
            .expect_err("empty timestamp range should be rejected");
        assert!(err.to_string().contains("created_utc_gte"));
    }

    #[test]
    fn timestamp_bounds_derive_months_from_inclusive_exclusive_edges() {
        // 2020-11-30T12:00:00Z .. 2020-12-01T00:00:00Z should plan Nov only;
        // the exclusive upper endpoint is exactly at the start of December.
        let bounds = TimestampBounds::new(Some(1_606_737_600), Some(1_606_780_800));
        assert_eq!(bounds.derived_start_month(), Some(YearMonth::new(2020, 11)));
        assert_eq!(bounds.derived_end_month(), Some(YearMonth::new(2020, 11)));

        let spanning = TimestampBounds::new(Some(1_606_737_600), Some(1_606_824_000));
        assert_eq!(
            spanning.derived_start_month(),
            Some(YearMonth::new(2020, 11))
        );
        assert_eq!(spanning.derived_end_month(), Some(YearMonth::new(2020, 12)));
    }
}
