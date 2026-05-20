
#[doc(hidden)]
pub fn apply_human_timestamps(val: &mut Value) {
    if let Some(obj) = val.as_object_mut() {
        // Convert common timestamp fields if they are numeric. "edited" can
        // be bool or number, so only numeric forms are rewritten.
        for key in ["created_utc", "retrieved_on", "edited"] {
            let Some(v) = obj.get_mut(key) else { continue };
            let Some(n) = v.as_i64() else { continue };
            let Ok(dt) = OffsetDateTime::from_unix_timestamp(n) else {
                continue;
            };
            let Ok(s) = dt.format(&Rfc3339) else { continue };
            *v = Value::String(s);
        }
    }
}

/// Test whether the JSON string token at `bytes[key_start]` (which must be a
/// `"`) begins one of `"created_utc":`, `"retrieved_on":`, `"edited":` with an
/// integer-literal value, and if so return `(value_start, value_end)`:
///
/// - `value_start` is the byte position immediately after the key's `":` —
///   so `bytes[..value_start]` preserves the key and colon verbatim. Any
///   whitespace and optional `-` sign between the colon and the digits sit
///   inside `[value_start..value_end]` and get replaced on rewrite.
/// - `value_end` is one past the last digit.
///
/// Returns `None` when `key_start` does not begin a timestamp key, or its
/// value is not an integer literal (`null`, `false`, a float in `1.5` / `1e3`
/// form) — those are left untouched. The caller is responsible for restricting
/// matches to the top level (see `rewrite_human_timestamps_bytes`).
fn match_timestamp_field(bytes: &[u8], key_start: usize) -> Option<(usize, usize)> {
    let len = bytes.len();
    let matched_len = TIMESTAMP_KEY_PATTERNS
        .iter()
        .find(|p| key_start + p.len() <= len && &bytes[key_start..key_start + p.len()] == **p)
        .map(|p| p.len())?;
    let value_start = key_start + matched_len;

    // Walk past optional whitespace (compact serde never emits any), an
    // optional `-`, then the digit run. Mirrors what `as_i64` accepts.
    let mut j = value_start;
    while j < len && (bytes[j] == b' ' || bytes[j] == b'\t') {
        j += 1;
    }
    if j < len && bytes[j] == b'-' {
        j += 1;
    }
    let digits_start = j;
    while j < len && bytes[j].is_ascii_digit() {
        j += 1;
    }

    // No digits → not an integer (e.g. `false`, `null`).
    // Trailing `.`/`e`/`E` → float; `as_i64` would reject too.
    let is_integer =
        j > digits_start && !matches!(bytes.get(j), Some(b'.') | Some(b'e') | Some(b'E'));
    if !is_integer {
        return None;
    }
    Some((value_start, j))
}

/// Return the index one past the closing quote of the JSON string whose
/// opening quote is at `bytes[open]`. Backslash escapes are honored, so an
/// escaped `\"` does not terminate the string. On an unterminated string
/// (malformed input) the end-of-input index is returned.
fn end_of_string(bytes: &[u8], open: usize) -> usize {
    let len = bytes.len();
    let mut i = open + 1;
    while i < len {
        match bytes[i] {
            // Skip the escaped byte. `\uXXXX` only needs the `u` skipped here:
            // the four hex digits contain neither `"` nor `\`, so the plain
            // walk consumes them safely.
            b'\\' => i += 2,
            b'"' => return i + 1,
            _ => i += 1,
        }
    }
    len
}

/// Parse a Unix-timestamp `i64` from a slice produced by `match_timestamp_field`.
/// The slice may begin with optional space/tab whitespace and an optional `-`
/// sign followed by ASCII digits. Returns `None` only on i64 overflow — the
/// slice is otherwise guaranteed well-formed by the caller.
///
/// (Spec'd as `u64` in the original task brief, but `i64` is required to keep
/// the negative-epoch test in `tests/human_timestamps_edge_cases.rs` passing.)
fn parse_unix_digits(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok().and_then(|s| {
        s.trim_start_matches(|c: char| c == ' ' || c == '\t')
            .parse::<i64>()
            .ok()
    })
}

/// Byte-level rewrite of the three timestamp fields directly from the raw JSONL
/// line into `buf`, without going through `serde_json::Value`.
///
/// Looks for the literal byte patterns `"created_utc":`, `"retrieved_on":`,
/// `"edited":` followed by an optional space and an integer, and replaces the
/// integer with an RFC3339 string. Non-integer values (`true`/`false`/`null`/
/// floats) are left untouched.
///
/// Only **top-level** keys — the direct members of the outermost JSON object,
/// at nesting depth 1 — are rewritten. This matches `apply_human_timestamps`
/// (which only walks `val.as_object_mut()`) and the whitelist tokenizer (which
/// only walks the top level). Reddit RS records nest full submission objects
/// carrying their own `created_utc` (e.g. inside the `crosspost_parent_list`
/// array); those nested timestamps must survive every export path identically,
/// so they are left as integers here too. Depth is tracked by counting
/// `{`/`[` against `}`/`]`, and string bodies are skipped wholesale so braces
/// appearing inside string values never perturb the count.
///
/// Safety on substring matching: the JSON spec requires `"` inside string
/// values to be escaped, so the literal byte sequence `"<key>":` cannot appear
/// inside a string value. That makes the anchored key match safe.
#[doc(hidden)]
pub fn rewrite_human_timestamps_bytes(line: &str, buf: &mut String) {
    buf.clear();
    buf.reserve(line.len() + 64);
    let bytes = line.as_bytes();
    let len = bytes.len();
    let mut last = 0usize;
    let mut i = 0usize;
    // Container nesting depth: 0 outside any container, 1 inside the outermost
    // object. `i64` cannot overflow — depth is bounded by `line.len()`.
    let mut depth: i64 = 0;

    while i < len {
        match bytes[i] {
            b'{' | b'[' => {
                depth += 1;
                i += 1;
            }
            b'}' | b']' => {
                depth -= 1;
                i += 1;
            }
            b'"' => {
                // A JSON string token — an object key or a string value. At
                // the top level it may be a timestamp key whose integer value
                // we rewrite; otherwise skip its body so braces/brackets
                // inside the string never reach the depth counter above.
                if depth == 1 {
                    if let Some((value_start, value_end)) = match_timestamp_field(bytes, i) {
                        // RFC3339 output contains only characters that are
                        // JSON-safe without escaping.
                        let formatted = parse_unix_digits(&bytes[value_start..value_end])
                            .and_then(|n| OffsetDateTime::from_unix_timestamp(n).ok())
                            .and_then(|dt| dt.format(&Rfc3339).ok());
                        if let Some(s) = formatted {
                            buf.push_str(&line[last..value_start]);
                            buf.push('"');
                            buf.push_str(&s);
                            buf.push('"');
                            last = value_end;
                        }
                        // Advance past the integer either way; nothing inside
                        // a digit run can start a new `"<key>":` match, so this
                        // is byte-equivalent on parse/format failure.
                        i = value_end;
                        continue;
                    }
                }
                i = end_of_string(bytes, i);
            }
            _ => i += 1,
        }
    }

    if last < bytes.len() {
        buf.push_str(&line[last..]);
    }
}
