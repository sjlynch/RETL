use crate::paths::FileKind;
use crate::query::{QuerySpec, RecordIdKind};
use crate::zstd_jsonl::MinimalRecord;

use super::ci::list_contains_ci;
use super::text::{
    keyword_all_matches_record, keyword_any_matches_record, record_text_regex_matches,
};
use super::url::record_contains_url;

pub fn matches_subreddit_basic(min: &MinimalRecord, sub: &str) -> bool {
    if let Some(s) = &min.subreddit {
        s.eq_ignore_ascii_case(sub)
    } else {
        false
    }
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
            // Exact timestamp filters reject created_utc that is missing or
            // not a number (string- and float-encoded numbers are coerced).
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
