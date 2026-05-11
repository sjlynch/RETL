//! Filtering helpers and query logic that work on MinimalRecord fast-path fields,
//! plus optional full-parse checks and record-level date bounds.

use crate::date::YearMonth;
use crate::paths::FileKind;
use crate::query::QuerySpec;
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

/// Case-insensitive byte search for the literal "http" (https is a superset).
#[inline]
fn ascii_ci_contains_http(haystack: &[u8]) -> bool {
    if haystack.len() < 4 {
        return false;
    }
    haystack.windows(4).any(|w| {
        w[0].eq_ignore_ascii_case(&b'h')
            && w[1].eq_ignore_ascii_case(&b't')
            && w[2].eq_ignore_ascii_case(&b't')
            && w[3].eq_ignore_ascii_case(&b'p')
    })
}

/// Decide using only fields in MinimalRecord (fast path).
/// If `targets_opt` is None, accept any subreddit (still rejects missing subreddit).
pub fn matches_minimal(
    min: &MinimalRecord,
    targets_opt: Option<&Vec<String>>,
    q: &QuerySpec,
) -> bool {
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

    // domains_in can be matched from MinimalRecord now (submissions only).
    if let Some(ref domains) = q.domains_in {
        match min.domain.as_deref() {
            Some(dom) if list_contains_ci(domains, dom) => {}
            // when domain filter is set, comments (no domain) are rejected:
            _ => return false,
        }
    }

    // keywords_any: run the pre-built ASCII-case-insensitive automaton over each
    // text field's raw bytes — no per-record allocation or to_lowercase pass.
    if let Some(ac) = q.keywords_automaton() {
        let matched = min
            .body
            .as_deref()
            .map_or(false, |s| ac.is_match(s.as_bytes()))
            || min
                .selftext
                .as_deref()
                .map_or(false, |s| ac.is_match(s.as_bytes()))
            || min
                .title
                .as_deref()
                .map_or(false, |s| ac.is_match(s.as_bytes()));
        if !matched {
            return false;
        }
    }
    if q.contains_url == Some(true) {
        let matched = min
            .body
            .as_deref()
            .map_or(false, |s| ascii_ci_contains_http(s.as_bytes()))
            || min
                .selftext
                .as_deref()
                .map_or(false, |s| ascii_ci_contains_http(s.as_bytes()))
            || min
                .title
                .as_deref()
                .map_or(false, |s| ascii_ci_contains_http(s.as_bytes()));
        if !matched {
            return false;
        }
    }

    true
}

/// Full-parse checks (kept for back-compat; not used when domain is on fast path).
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
    true
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

/// Bounds helpers (record-level YYYY-MM gate)
pub fn bounds_tuple(
    start: Option<YearMonth>,
    end: Option<YearMonth>,
) -> Option<(YearMonth, YearMonth)> {
    match (start, end) {
        (Some(s), Some(e)) => Some((s, e)),
        _ => None,
    }
}
pub fn within_bounds(min: &MinimalRecord, bounds: Option<(YearMonth, YearMonth)>) -> bool {
    if let Some((lo, hi)) = bounds {
        if let Some(ts) = min.created_utc {
            let ym = ym_from_epoch(ts);
            return ym >= lo && ym <= hi;
        } else {
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
