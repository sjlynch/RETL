//! Filtering helpers and query logic that work on MinimalRecord fast-path fields,
//! plus optional full-parse checks and record-level date bounds.

use crate::date::YearMonth;
use crate::paths::FileKind;
use crate::query::QuerySpec;
use crate::zstd_jsonl::MinimalRecord;
use serde_json::Value;
use time::{Date, OffsetDateTime};

pub fn matches_subreddit_basic(min: &MinimalRecord, sub: &str) -> bool {
    if let Some(s) = &min.subreddit { s.eq_ignore_ascii_case(sub) } else { false }
}

/// Decide using only fields in MinimalRecord (fast path).
/// If `targets_opt` is None, accept any subreddit (still rejects missing subreddit).
pub fn matches_minimal(min: &MinimalRecord, targets_opt: Option<&Vec<String>>, q: &QuerySpec) -> bool {
    if let Some(targets) = targets_opt {
        match min.subreddit.as_deref().map(|s| s.to_lowercase()) {
            Some(s) if targets.binary_search(&s).is_ok() => {}
            _ => return false,
        }
    } else if min.subreddit.is_none() {
        return false;
    }

    if let Some(a) = min.author.as_deref() {
        let a_low = a.to_lowercase();
        if q.filter_pseudo_users && (a_low == "[deleted]" || a_low == "[removed]" || a_low.is_empty()) {
            return false;
        }
        if let Some(ref deny) = q.authors_out {
            if deny.binary_search(&a_low).is_ok() { return false; }
        }
        if let Some(ref allow) = q.authors_in {
            if !allow.binary_search(&a_low).is_ok() { return false; }
        }
        if let Some(re) = &q.author_regex {
            if !re.is_match(a) { return false; }
        }
    } else {
        return false;
    }

    if let Some(min_s) = q.min_score {
        match min.score { Some(sc) if sc >= min_s => {}, _ => return false }
    }
    if let Some(max_s) = q.max_score {
        match min.score { Some(sc) if sc <= max_s => {}, _ => return false }
    }

    // domains_in can be matched from MinimalRecord now (submissions only).
    if let Some(ref domains) = q.domains_in {
        match min.domain.as_deref().map(|d| d.to_lowercase()) {
            Some(dom) if domains.binary_search(&dom).is_ok() => {}
            // when domain filter is set, comments (no domain) are rejected:
            _ => return false,
        }
    }

    // keywords/URL checks on MinimalRecord text (no full parse)
    if let Some(ref kws) = q.keywords_any {
        let mut hay = String::new();
        if let Some(body) = min.body.as_deref() { hay.push_str(&body.to_lowercase()); hay.push(' '); }
        if let Some(selftext) = min.selftext.as_deref() { hay.push_str(&selftext.to_lowercase()); hay.push(' '); }
        if let Some(title) = min.title.as_deref() { hay.push_str(&title.to_lowercase()); hay.push(' '); }
        if !kws.iter().any(|kw| hay.contains(kw)) { return false; }
    }
    if q.contains_url == Some(true) {
        let mut hay = String::new();
        if let Some(body) = min.body.as_deref() { hay.push_str(body); hay.push(' '); }
        if let Some(selftext) = min.selftext.as_deref() { hay.push_str(selftext); hay.push(' '); }
        if let Some(title) = min.title.as_deref() { hay.push_str(title); hay.push(' '); }
        let hay = hay.to_lowercase();
        if !(hay.contains("http://") || hay.contains("https://")) { return false; }
    }

    true
}

/// Full-parse checks (kept for back-compat; not used when domain is on fast path).
pub fn matches_full(val: &Value, kind: FileKind, q: &QuerySpec) -> bool {
    if let Some(ref domains) = q.domains_in {
        if let FileKind::Submission = kind {
            let d = val.get("domain").and_then(|v| v.as_str()).map(|s| s.to_lowercase());
            match d {
                Some(dom) if domains.binary_search(&dom).is_ok() => {}
                _ => return false,
            }
        } else {
            return false;
        }
    }
    true
}

/// Resolve target subreddits from either a Query override or ETL default.
/// Returns None to indicate "all subreddits".
pub fn resolve_target_subs_from(
    etl_subreddit: &Option<String>,
    q_subreddits: &Option<Vec<String>>,
) -> Option<Vec<String>> {
    let mut v = if let Some(ref subs) = q_subreddits {
        subs.clone()
    } else if let Some(ref single) = etl_subreddit {
        vec![single.clone().to_lowercase()]
    } else {
        return None;
    };
    v.sort();
    v.dedup();
    Some(v)
}

/// Bounds helpers (record-level YYYY-MM gate)
pub fn bounds_tuple(start: Option<YearMonth>, end: Option<YearMonth>) -> Option<(YearMonth, YearMonth)> {
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
