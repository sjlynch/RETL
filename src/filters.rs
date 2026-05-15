//! Filtering helpers and query logic that work on MinimalRecord fast-path fields,
//! plus optional full-parse checks and record-level date/timestamp bounds.

use crate::date::YearMonth;
use crate::paths::FileKind;
use crate::query::{QuerySpec, RecordIdKind};
use crate::zstd_jsonl::MinimalRecord;
use serde_json::Value;
use time::{Date, OffsetDateTime};

pub fn matches_subreddit_basic(min: &MinimalRecord, sub: &str) -> bool {
    if let Some(s) = &min.subreddit {
        s.eq_ignore_ascii_case(sub)
    } else {
        false
    }
}

/// Linear scan of a small (<10) list of pre-lowercased targets, comparing to
/// `needle` case-insensitively. ASCII fast path uses byte-level case folding;
/// non-ASCII needles fall back to a single Unicode lowercase + equality check.
#[inline]
fn list_contains_ci(list: &[String], needle: &str) -> bool {
    if needle.is_ascii() {
        list.iter().any(|s| s.eq_ignore_ascii_case(needle))
    } else {
        let lower = needle.to_lowercase();
        list.iter().any(|s| s == &lower)
    }
}

#[inline]
fn ascii_ci_is_http_prefix(bytes: &[u8]) -> bool {
    bytes.len() >= 4
        && bytes[0].eq_ignore_ascii_case(&b'h')
        && bytes[1].eq_ignore_ascii_case(&b't')
        && bytes[2].eq_ignore_ascii_case(&b't')
        && bytes[3].eq_ignore_ascii_case(&b'p')
}

/// Case-insensitive byte search for the literal "http" (https is a superset).
#[inline]
fn ascii_ci_contains_http(haystack: &[u8]) -> bool {
    if haystack.len() < 4 {
        return false;
    }
    haystack.windows(4).any(ascii_ci_is_http_prefix)
}

#[inline]
fn keyword_field_matches(
    ac: &aho_corasick::AhoCorasick,
    keywords_all_ascii: bool,
    text: &str,
) -> bool {
    if keywords_all_ascii && text.is_ascii() {
        return ac.is_match(text.as_bytes());
    }

    // First try the raw bytes: this preserves the zero-allocation path for
    // already-lowercase Unicode text and for ASCII folds inside non-ASCII text.
    if ac.is_match(text.as_bytes()) {
        return true;
    }

    // Unicode-aware fallback. Keyword inputs are stored lowercased in
    // QuerySpec::normalize(); lowercase the haystack only when either the
    // keyword set or the text field leaves the all-ASCII fast path.
    let lower = text.to_lowercase();
    ac.is_match(lower.as_bytes())
}

#[inline]
fn any_text_field_matches(min: &MinimalRecord, mut pred: impl FnMut(&str) -> bool) -> bool {
    min.body.as_deref().map_or(false, &mut pred)
        || min.selftext.as_deref().map_or(false, &mut pred)
        || min.title.as_deref().map_or(false, pred)
}

#[inline]
fn keyword_any_matches_record(
    min: &MinimalRecord,
    ac: &aho_corasick::AhoCorasick,
    keywords_all_ascii: bool,
) -> bool {
    any_text_field_matches(min, |s| keyword_field_matches(ac, keywords_all_ascii, s))
}

fn mark_keyword_matches_bytes(
    ac: &aho_corasick::AhoCorasick,
    bytes: &[u8],
    seen: &mut [bool],
    remaining: &mut usize,
) {
    for mat in ac.find_overlapping_iter(bytes) {
        let idx = mat.pattern().as_usize();
        if !seen[idx] {
            seen[idx] = true;
            *remaining -= 1;
            if *remaining == 0 {
                return;
            }
        }
    }
}

fn mark_keyword_field_matches(
    ac: &aho_corasick::AhoCorasick,
    keywords_all_ascii: bool,
    text: &str,
    seen: &mut [bool],
    remaining: &mut usize,
) {
    if *remaining == 0 {
        return;
    }
    mark_keyword_matches_bytes(ac, text.as_bytes(), seen, remaining);
    if *remaining == 0 || (keywords_all_ascii && text.is_ascii()) {
        return;
    }
    let lower = text.to_lowercase();
    mark_keyword_matches_bytes(ac, lower.as_bytes(), seen, remaining);
}

fn keyword_all_matches_record(
    min: &MinimalRecord,
    ac: &aho_corasick::AhoCorasick,
    keyword_count: usize,
    keywords_all_ascii: bool,
) -> bool {
    if keyword_count == 0 {
        return true;
    }
    let mut seen = vec![false; keyword_count];
    let mut remaining = keyword_count;

    if let Some(body) = min.body.as_deref() {
        mark_keyword_field_matches(ac, keywords_all_ascii, body, &mut seen, &mut remaining);
    }
    if let Some(selftext) = min.selftext.as_deref() {
        mark_keyword_field_matches(ac, keywords_all_ascii, selftext, &mut seen, &mut remaining);
    }
    if let Some(title) = min.title.as_deref() {
        mark_keyword_field_matches(ac, keywords_all_ascii, title, &mut seen, &mut remaining);
    }

    remaining == 0
}

#[inline]
fn record_text_regex_matches(min: &MinimalRecord, re: &regex::Regex) -> bool {
    any_text_field_matches(min, |s| re.is_match(s))
}

#[inline]
fn domain_marks_self_post(domain: &str) -> bool {
    domain.eq_ignore_ascii_case("self")
        || domain
            .get(..5)
            .is_some_and(|p| p.eq_ignore_ascii_case("self."))
}

#[inline]
fn is_self_submission(min: &MinimalRecord) -> bool {
    min.is_self == Some(true) || min.domain.as_deref().is_some_and(domain_marks_self_post)
}

#[inline]
fn submission_url_matches_url_filter(min: &MinimalRecord) -> bool {
    min.url
        .as_deref()
        .is_some_and(|s| ascii_ci_is_http_prefix(s.as_bytes()))
        && !is_self_submission(min)
}

#[inline]
fn record_contains_url(min: &MinimalRecord) -> bool {
    any_text_field_matches(min, |s| ascii_ci_contains_http(s.as_bytes()))
        || submission_url_matches_url_filter(min)
}

/// Decide using only fields in MinimalRecord (fast path).
/// If `targets_opt` is None, accept any subreddit (still rejects missing subreddit).
pub fn matches_minimal(
    min: &MinimalRecord,
    targets_opt: Option<&Vec<String>>,
    q: &QuerySpec,
    kind: FileKind,
) -> bool {
    let record_kind = match kind {
        FileKind::Comment => RecordIdKind::Comment,
        FileKind::Submission => RecordIdKind::Submission,
    };
    if !q.id_filter_matches(record_kind, min.id.as_deref()) {
        return false;
    }

    if let Some(targets) = targets_opt {
        match min.subreddit.as_deref() {
            Some(s) if list_contains_ci(targets, s) => {}
            _ => return false,
        }
    } else if min.subreddit.is_none() {
        return false;
    }

    if let Some(a) = min.author.as_deref() {
        if q.filter_pseudo_users
            && (a.is_empty()
                || a.eq_ignore_ascii_case("[deleted]")
                || a.eq_ignore_ascii_case("[removed]"))
        {
            return false;
        }
        if let Some(ref deny) = q.authors_out {
            if list_contains_ci(deny, a) {
                return false;
            }
        }
        if let Some(ref allow) = q.authors_in {
            if !list_contains_ci(allow, a) {
                return false;
            }
        }
        if let Some(re) = &q.author_regex {
            if !re.is_match(a) {
                return false;
            }
        }
    } else {
        return false;
    }

    if let Some(min_s) = q.min_score {
        match min.score {
            Some(sc) if sc >= min_s => {}
            _ => return false,
        }
    }
    if let Some(max_s) = q.max_score {
        match min.score {
            Some(sc) if sc <= max_s => {}
            _ => return false,
        }
    }

    if q.timestamp_bounds.is_active() {
        match min.created_utc {
            Some(ts) if q.timestamp_bounds.contains(ts) => {}
            // Exact timestamp filters reject missing/non-integer created_utc.
            _ => return false,
        }
    }

    // domains_in can be matched from MinimalRecord now (submissions only).
    if let Some(ref domains) = q.domains_in {
        match min.domain.as_deref() {
            Some(dom) if list_contains_ci(domains, dom) => {}
            // when domain filter is set, comments (no domain) are rejected:
            _ => return false,
        }
    }

    // Keyword filters search the same MinimalRecord text fields: comment
    // `body`, submission `selftext`, and submission `title`. All-ASCII
    // keywords + all-ASCII haystacks stay on the pre-built Aho-Corasick
    // raw-byte path. Non-ASCII keywords/text get a lowercase fallback so the
    // documented case-insensitive behavior covers Unicode (e.g. `café`
    // matching `CAFÉ`).
    if let Some(ac) = q.keywords_any_automaton() {
        if !keyword_any_matches_record(min, ac, q.keywords_any_all_ascii()) {
            return false;
        }
    }
    if let Some(ac) = q.keywords_all_automaton() {
        let keyword_count = q.keywords_all.as_ref().map_or(0, Vec::len);
        if !keyword_all_matches_record(min, ac, keyword_count, q.keywords_all_all_ascii()) {
            return false;
        }
    }
    if let Some(ac) = q.keywords_exclude_automaton() {
        if keyword_any_matches_record(min, ac, q.keywords_exclude_all_ascii()) {
            return false;
        }
    }
    if let Some(re) = &q.text_regex {
        if !record_text_regex_matches(min, re) {
            return false;
        }
    }
    if q.contains_url == Some(true) && !record_contains_url(min) {
        return false;
    }
    if q.no_url && record_contains_url(min) {
        return false;
    }

    true
}

/// Full-parse checks for arbitrary JSON-pointer predicates.
pub fn matches_full(val: &Value, kind: FileKind, q: &QuerySpec) -> bool {
    if let Some(ref domains) = q.domains_in {
        if let FileKind::Submission = kind {
            let d = val.get("domain").and_then(|v| v.as_str());
            match d {
                Some(dom) if list_contains_ci(domains, dom) => {}
                _ => return false,
            }
        } else {
            return false;
        }
    }

    q.json_predicates
        .iter()
        .all(|predicate| predicate.matches(val))
}

/// Resolve target subreddits from both the deprecated ETL single-subreddit
/// default and the query-level multi-subreddit selector. When both are set,
/// the selections are merged rather than letting one silently override the
/// other. Returns None to indicate "all subreddits".
pub fn resolve_target_subs_from(
    etl_subreddit: &Option<String>,
    q_subreddits: &Option<Vec<String>>,
) -> Option<Vec<String>> {
    let mut v = Vec::new();
    if let Some(ref single) = etl_subreddit {
        v.push(single.clone().to_lowercase());
    }
    if let Some(ref subs) = q_subreddits {
        v.extend(subs.iter().cloned());
    }
    if v.is_empty() {
        return None;
    }
    v.sort();
    v.dedup();
    Some(v)
}

/// Independent record-level YYYY-MM bounds.
///
/// A missing endpoint leaves that side open, but the other endpoint still
/// applies. When either endpoint is present, records without `created_utc` are
/// rejected because they cannot be proven to fall inside the requested range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DateBounds {
    pub start: Option<YearMonth>,
    pub end: Option<YearMonth>,
}

impl DateBounds {
    #[inline]
    pub fn is_active(self) -> bool {
        self.start.is_some() || self.end.is_some()
    }
}

/// Bounds helper (record-level YYYY-MM gate).
///
/// Kept under its historical name for compatibility with existing internal
/// callers/tests; unlike the old tuple shape, one-sided ranges now return
/// `Some(DateBounds)` and enforce the present endpoint.
pub fn bounds_tuple(start: Option<YearMonth>, end: Option<YearMonth>) -> Option<DateBounds> {
    let bounds = DateBounds { start, end };
    bounds.is_active().then_some(bounds)
}

pub fn within_bounds(min: &MinimalRecord, bounds: Option<DateBounds>) -> bool {
    let Some(bounds) = bounds else {
        return true;
    };

    let Some(ts) = min.created_utc else {
        return false;
    };

    let ym = ym_from_epoch(ts);
    if let Some(lo) = bounds.start {
        if ym < lo {
            return false;
        }
    }
    if let Some(hi) = bounds.end {
        if ym > hi {
            return false;
        }
    }
    true
}
pub fn ym_from_epoch(ts: i64) -> YearMonth {
    let dt = OffsetDateTime::from_unix_timestamp(ts).unwrap_or_else(|_| OffsetDateTime::UNIX_EPOCH);
    let date: Date = dt.date();
    let year = date.year().clamp(0, u16::MAX as i32) as u16;
    let month = date.month() as u8;
    YearMonth { year, month }
}
