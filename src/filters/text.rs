use crate::zstd_jsonl::MinimalRecord;

use super::any_text_field_matches;

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
pub(super) fn keyword_any_matches_record(
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

pub(super) fn keyword_all_matches_record(
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
pub(super) fn record_text_regex_matches(min: &MinimalRecord, re: &regex::Regex) -> bool {
    any_text_field_matches(min, |s| re.is_match(s))
}
