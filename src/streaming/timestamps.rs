
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

/// Find the next occurrence of one of `"created_utc":`, `"retrieved_on":`,
/// `"edited":` in `bytes[start..]` whose value is an integer literal, and
/// return `(value_start, value_end)`:
///
/// - `value_start` is the byte position immediately after the key's `":` —
///   so `bytes[..value_start]` preserves the key and colon verbatim. Any
///   whitespace and optional `-` sign between the colon and the digits sit
///   inside `[value_start..value_end]` and get replaced on rewrite.
/// - `value_end` is one past the last digit.
///
/// Keys whose value is not an integer literal (`null`, `false`, a float in
/// `1.5` / `1e3` form) are skipped and the search continues. Returns `None`
/// when no remaining match exists.
fn find_timestamp_field(bytes: &[u8], start: usize) -> Option<(usize, usize)> {
    let len = bytes.len();
    let mut i = start;
    while i < len {
        if bytes[i] != b'"' {
            i += 1;
            continue;
        }
        let matched_len = TIMESTAMP_KEY_PATTERNS
            .iter()
            .find(|p| i + p.len() <= len && &bytes[i..i + p.len()] == **p)
            .map(|p| p.len())
            .unwrap_or(0);
        if matched_len == 0 {
            i += 1;
            continue;
        }
        let value_start = i + matched_len;

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
            i = value_start;
            continue;
        }
        return Some((value_start, j));
    }
    None
}

/// Parse a Unix-timestamp `i64` from a slice produced by `find_timestamp_field`.
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

/// Byte-level rewrite of the three timestamp fields directly from the raw JSONL line
/// into `buf`, without going through `serde_json::Value`.
///
/// Looks for the literal byte patterns `"created_utc":`, `"retrieved_on":`, `"edited":`
/// followed by an optional space and an integer, and replaces the integer with an
/// RFC3339 string. Non-integer values (`true`/`false`/`null`/floats) are left untouched.
///
/// Safety on substring matching: the JSON spec requires `"` inside string values to be
/// escaped, so the literal byte sequence `"<key>":` cannot appear inside a string value.
/// That makes a flat byte search safe for the keys we care about.
#[doc(hidden)]
pub fn rewrite_human_timestamps_bytes(line: &str, buf: &mut String) {
    buf.clear();
    buf.reserve(line.len() + 64);
    let bytes = line.as_bytes();
    let mut last = 0usize;
    let mut i = 0usize;

    while let Some((value_start, value_end)) = find_timestamp_field(bytes, i) {
        // RFC3339 output contains only characters that are JSON-safe without escaping.
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
        // Advance past the integer either way; nothing inside a digit run can
        // start a new `"<key>":` match, so this is byte-equivalent to the
        // original `i = value_start` on parse/format failure.
        i = value_end;
    }

    if last < bytes.len() {
        buf.push_str(&line[last..]);
    }
}
