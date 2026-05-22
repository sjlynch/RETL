#[inline]
pub(super) fn skip_ws(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() {
        match bytes[i] {
            b' ' | b'\t' | b'\n' | b'\r' => i += 1,
            _ => break,
        }
    }
    i
}

/// Scan a JSON string body starting at `i` (one past the opening `"`).
/// Returns `(index of closing quote, has_escape)` on success, or `None` on
/// malformed input.
pub(super) fn scan_string_body(bytes: &[u8], mut i: usize) -> Option<(usize, bool)> {
    let mut has_escape = false;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => return Some((i, has_escape)),
            b'\\' => {
                has_escape = true;
                if i + 1 >= bytes.len() {
                    return None;
                }
                if bytes[i + 1] == b'u' {
                    if i + 5 >= bytes.len() {
                        return None;
                    }
                    i += 6;
                } else {
                    i += 2;
                }
            }
            _ => i += 1,
        }
    }
    None
}

/// Skip exactly one JSON value starting at `i`. Returns the index just past
/// the value's last byte, or `None` if the input is malformed at this point.
pub(super) fn skip_value(bytes: &[u8], i: usize) -> Option<usize> {
    if i >= bytes.len() {
        return None;
    }
    match bytes[i] {
        b'"' => scan_string_body(bytes, i + 1).map(|(end, _)| end + 1),
        b'{' | b'[' => skip_container(bytes, i),
        b't' => skip_literal(bytes, i, b"true"),
        b'f' => skip_literal(bytes, i, b"false"),
        b'n' => skip_literal(bytes, i, b"null"),
        b'-' | b'0'..=b'9' => skip_number(bytes, i),
        _ => None,
    }
}

/// Walk past one container value (`{...}` or `[...]`), tracking total nesting
/// depth across both kinds. We don't enforce that `{` is paired with `}` and
/// `[` with `]`; serde_json would catch a true mismatch on the fallback slow
/// path. The contract here is "skip exactly one well-formed value", and any
/// structural surprise should bubble up as a tokenizer error so the caller
/// can fall back.
pub(super) fn skip_container(bytes: &[u8], mut i: usize) -> Option<usize> {
    let mut depth: u32 = 1;
    i += 1;
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b'"' => {
                let (end, _) = scan_string_body(bytes, i + 1)?;
                i = end + 1;
            }
            b'{' | b'[' => {
                depth += 1;
                i += 1;
            }
            b'}' | b']' => {
                depth -= 1;
                i += 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => i += 1,
        }
    }
    None
}

pub(super) fn skip_literal(bytes: &[u8], i: usize, lit: &[u8]) -> Option<usize> {
    if i + lit.len() <= bytes.len() && &bytes[i..i + lit.len()] == lit {
        Some(i + lit.len())
    } else {
        None
    }
}

pub(super) fn skip_number(bytes: &[u8], mut i: usize) -> Option<usize> {
    let start = i;
    if bytes[i] == b'-' {
        i += 1;
    }
    let int_start = i;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i == int_start {
        return None;
    }
    // JSON forbids a leading zero followed by another digit (`007`, `01`).
    // serde_json rejects the whole line on the slow path; reject here too so
    // the caller falls back rather than copying a non-JSON number verbatim.
    if i - int_start > 1 && bytes[int_start] == b'0' {
        return None;
    }
    if i < bytes.len() && bytes[i] == b'.' {
        i += 1;
        let frac_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == frac_start {
            return None;
        }
    }
    if i < bytes.len() && (bytes[i] == b'e' || bytes[i] == b'E') {
        i += 1;
        if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
            i += 1;
        }
        let exp_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == exp_start {
            return None;
        }
    }
    if i == start {
        None
    } else {
        Some(i)
    }
}
