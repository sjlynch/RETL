/// Linear scan of a small (<10) list of pre-lowercased targets, comparing to
/// `needle` case-insensitively. ASCII fast path uses byte-level case folding;
/// non-ASCII needles fall back to a single Unicode lowercase + equality check.
#[inline]
pub(super) fn list_contains_ci(list: &[String], needle: &str) -> bool {
    if needle.is_ascii() {
        list.iter().any(|s| s.eq_ignore_ascii_case(needle))
    } else {
        let lower = needle.to_lowercase();
        list.iter().any(|s| s == &lower)
    }
}

#[inline]
pub(super) fn ascii_ci_starts_with(bytes: &[u8], prefix: &[u8]) -> bool {
    bytes
        .get(..prefix.len())
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(prefix))
}
