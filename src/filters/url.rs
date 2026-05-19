use crate::zstd_jsonl::MinimalRecord;

use super::any_text_field_matches;
use super::ci::ascii_ci_starts_with;

const HTTP_PREFIX: &[u8] = b"http";

#[inline]
fn ascii_ci_is_http_prefix(bytes: &[u8]) -> bool {
    ascii_ci_starts_with(bytes, HTTP_PREFIX)
}

/// Case-insensitive byte search for the literal "http" (https is a superset).
#[inline]
fn ascii_ci_contains_http(haystack: &[u8]) -> bool {
    if haystack.len() < HTTP_PREFIX.len() {
        return false;
    }
    haystack
        .windows(HTTP_PREFIX.len())
        .any(ascii_ci_is_http_prefix)
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
pub(super) fn record_contains_url(min: &MinimalRecord) -> bool {
    any_text_field_matches(min, |s| ascii_ci_contains_http(s.as_bytes()))
        || submission_url_matches_url_filter(min)
}
