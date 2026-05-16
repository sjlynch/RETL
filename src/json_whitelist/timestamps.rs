use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

/// Top-level keys whose integer values are rewritten to RFC3339 strings by
/// `WhitelistTokenizer::tokenize_and_rewrite_timestamps_into`. Mirrors the
/// list in `streaming::rewrite_human_timestamps_bytes`.
pub(super) const TIMESTAMP_KEYS: &[&[u8]] = &[b"created_utc", b"retrieved_on", b"edited"];

#[inline]
pub(super) fn is_timestamp_key(key: &[u8]) -> bool {
    TIMESTAMP_KEYS.iter().any(|k| *k == key)
}

pub(super) fn emit_timestamp_rewritten_value(key: &[u8], raw: &[u8], out: &mut String) {
    if is_timestamp_key(key) {
        // Plain ASCII slice; from_utf8 is essentially a length check.
        if let Ok(s) = std::str::from_utf8(raw) {
            if let Ok(n) = s.parse::<i64>() {
                if let Ok(dt) = OffsetDateTime::from_unix_timestamp(n) {
                    if let Ok(formatted) = dt.format(&Rfc3339) {
                        out.push('"');
                        out.push_str(&formatted);
                        out.push('"');
                        return;
                    }
                }
            }
        }
    }
    // SAFETY: bytes are a sub-slice of the original `&str`, valid UTF-8.
    out.push_str(unsafe { std::str::from_utf8_unchecked(raw) });
}
