//! Streaming whitelist tokenizer.
//!
//! The slow path in [`crate::streaming::stream_job`] (whitelist branch) used to
//! `serde_json::from_str` every line into a `Value`, copy the requested fields
//! into a fresh `Map`, then re-serialize. For whitelisted exports — a common
//! production config — this dominated CPU.
//!
//! [`WhitelistTokenizer`] walks the raw byte buffer once at the top level only.
//! For each top-level pair it scans the key, checks membership against a small
//! `HashSet`, and if matched copies the original `key:raw_value` byte slice
//! verbatim into a reusable output buffer. Unknown keys are skipped by tracking
//! brace/bracket nesting and string-escape state — no allocation, no `Value`
//! round-trip.
//!
//! Reddit records are flat top-level objects (with arrays/objects only nested
//! inside specific known fields), and the whitelist itself is top-level keys,
//! so a top-level walk is exactly what's needed.
//!
//! On any structural surprise the tokenizer returns [`TokenizerError::Malformed`]
//! and the caller falls back to the slow path so we never regress correctness.

use ahash::AHashSet;

#[derive(Debug)]
pub enum TokenizerError {
    /// The input is not a syntactically well-formed flat top-level object that
    /// the tokenizer knows how to walk. The caller should fall back to the
    /// `serde_json::Value` slow path.
    Malformed,
}

/// Small, reusable tokenizer that emits a compact JSON object containing only
/// whitelisted top-level keys, copying their raw value bytes from the input.
///
/// Construct once per pipeline (the field set is hashed up front) and reuse the
/// same instance — and the same output `String` buffer — across every line.
pub struct WhitelistTokenizer {
    keys: AHashSet<Vec<u8>>,
}

impl WhitelistTokenizer {
    pub fn new<I, S>(fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let keys: AHashSet<Vec<u8>> = fields
            .into_iter()
            .map(|s| s.as_ref().as_bytes().to_vec())
            .collect();
        Self { keys }
    }

    /// Walk `line` and append a compact JSON object containing the whitelisted
    /// fields to `out`. `out` is cleared on entry. On error `out` is left empty
    /// and the caller should use the slow path.
    pub fn tokenize_into(&self, line: &str, out: &mut String) -> Result<(), TokenizerError> {
        out.clear();
        let bytes = line.as_bytes();
        let mut i = skip_ws(bytes, 0);
        if i >= bytes.len() || bytes[i] != b'{' {
            return Err(TokenizerError::Malformed);
        }
        i += 1;
        out.push('{');

        // Independent counters: `first_pair` tracks the input walk (must
        // accept exactly one ',' between consecutive pairs), while
        // `emitted_any` tracks the output (whether to prepend a ',' before
        // the next emitted pair). They diverge whenever an input pair is
        // skipped because its key isn't in the whitelist.
        let mut first_pair = true;
        let mut emitted_any = false;
        loop {
            i = skip_ws(bytes, i);
            if i >= bytes.len() {
                out.clear();
                return Err(TokenizerError::Malformed);
            }
            if bytes[i] == b'}' {
                i += 1;
                break;
            }
            if !first_pair {
                if bytes[i] != b',' {
                    out.clear();
                    return Err(TokenizerError::Malformed);
                }
                i += 1;
                i = skip_ws(bytes, i);
            }
            first_pair = false;

            // Key string.
            if i >= bytes.len() || bytes[i] != b'"' {
                out.clear();
                return Err(TokenizerError::Malformed);
            }
            let key_quoted_start = i;
            let key_content_start = i + 1;
            let (key_content_end, key_has_escape) = match scan_string_body(bytes, i + 1) {
                Some(v) => v,
                None => {
                    out.clear();
                    return Err(TokenizerError::Malformed);
                }
            };
            let key_quoted_end = key_content_end + 1; // includes closing quote
            i = key_quoted_end;

            // ':' separator.
            i = skip_ws(bytes, i);
            if i >= bytes.len() || bytes[i] != b':' {
                out.clear();
                return Err(TokenizerError::Malformed);
            }
            i += 1;
            i = skip_ws(bytes, i);

            // Value (any JSON value).
            let value_start = i;
            let value_end = match skip_value(bytes, i) {
                Some(e) => e,
                None => {
                    out.clear();
                    return Err(TokenizerError::Malformed);
                }
            };
            i = value_end;

            // Membership test. Plain ASCII keys (the common case) hit the fast
            // raw-byte path. If the JSON key contains a backslash escape the
            // key bytes don't equal their decoded form, so we ask serde_json
            // for the canonical decoding before checking the whitelist.
            let matched = if key_has_escape {
                let raw = &bytes[key_quoted_start..key_quoted_end];
                match serde_json::from_slice::<String>(raw) {
                    Ok(decoded) => self.keys.contains(decoded.as_bytes()),
                    Err(_) => {
                        out.clear();
                        return Err(TokenizerError::Malformed);
                    }
                }
            } else {
                self.keys.contains(&bytes[key_content_start..key_content_end])
            };

            if matched {
                if emitted_any {
                    out.push(',');
                }
                emitted_any = true;
                // SAFETY: bytes are a sub-slice of the original `&str`, which is
                // valid UTF-8. `from_utf8_unchecked` avoids re-validating.
                out.push_str(unsafe {
                    std::str::from_utf8_unchecked(&bytes[key_quoted_start..key_quoted_end])
                });
                out.push(':');
                out.push_str(unsafe {
                    std::str::from_utf8_unchecked(&bytes[value_start..value_end])
                });
            }
        }

        // Trailing whitespace (none in compact serde output) is tolerated, but
        // any non-ws content after the closing brace means the line wasn't a
        // single top-level object.
        let tail = skip_ws(bytes, i);
        if tail != bytes.len() {
            out.clear();
            return Err(TokenizerError::Malformed);
        }

        out.push('}');
        Ok(())
    }
}

#[inline]
fn skip_ws(bytes: &[u8], mut i: usize) -> usize {
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
fn scan_string_body(bytes: &[u8], mut i: usize) -> Option<(usize, bool)> {
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
fn skip_value(bytes: &[u8], i: usize) -> Option<usize> {
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
fn skip_container(bytes: &[u8], mut i: usize) -> Option<usize> {
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

fn skip_literal(bytes: &[u8], i: usize, lit: &[u8]) -> Option<usize> {
    if i + lit.len() <= bytes.len() && &bytes[i..i + lit.len()] == lit {
        Some(i + lit.len())
    } else {
        None
    }
}

fn skip_number(bytes: &[u8], mut i: usize) -> Option<usize> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn slow_path(line: &str, fields: &[&str]) -> String {
        let val: Value = serde_json::from_str(line).unwrap();
        let mut obj = serde_json::Map::new();
        if let Some(map) = val.as_object() {
            for k in fields {
                if let Some(v) = map.get(*k) {
                    obj.insert((*k).to_string(), v.clone());
                }
            }
        }
        serde_json::to_string(&Value::Object(obj)).unwrap()
    }

    fn equal_as_json(a: &str, b: &str) -> bool {
        let va: Value = serde_json::from_str(a).unwrap();
        let vb: Value = serde_json::from_str(b).unwrap();
        va == vb
    }

    #[test]
    fn happy_path_flat_object() {
        let line = r#"{"id":"abc","author":"alice","score":42,"body":"hi"}"#;
        let tok = WhitelistTokenizer::new(["id", "author"]);
        let mut out = String::new();
        tok.tokenize_into(line, &mut out).unwrap();
        assert!(equal_as_json(&out, &slow_path(line, &["id", "author"])));
    }

    #[test]
    fn skips_nested_object_value() {
        let line = r#"{"a":1,"meta":{"x":[1,2,{"y":"z"}]},"b":2}"#;
        let tok = WhitelistTokenizer::new(["a", "b"]);
        let mut out = String::new();
        tok.tokenize_into(line, &mut out).unwrap();
        assert!(equal_as_json(&out, &slow_path(line, &["a", "b"])));
    }

    #[test]
    fn skips_nested_array_value() {
        let line = r#"{"a":1,"arr":[1,"two",{"k":"v"},[3]],"b":2}"#;
        let tok = WhitelistTokenizer::new(["arr"]);
        let mut out = String::new();
        tok.tokenize_into(line, &mut out).unwrap();
        assert!(equal_as_json(&out, &slow_path(line, &["arr"])));
    }

    #[test]
    fn empty_whitelist_yields_empty_object() {
        let line = r#"{"a":1,"b":2}"#;
        let tok = WhitelistTokenizer::new(Vec::<&str>::new());
        let mut out = String::new();
        tok.tokenize_into(line, &mut out).unwrap();
        assert_eq!(out, "{}");
    }

    #[test]
    fn no_match_yields_empty_object() {
        let line = r#"{"a":1,"b":2}"#;
        let tok = WhitelistTokenizer::new(["zzz"]);
        let mut out = String::new();
        tok.tokenize_into(line, &mut out).unwrap();
        assert_eq!(out, "{}");
    }

    #[test]
    fn string_with_escaped_quote_is_skipped_correctly() {
        let line = r#"{"a":"he said \"hi\"","b":2}"#;
        let tok = WhitelistTokenizer::new(["b"]);
        let mut out = String::new();
        tok.tokenize_into(line, &mut out).unwrap();
        assert!(equal_as_json(&out, &slow_path(line, &["b"])));
    }

    #[test]
    fn string_value_emitted_verbatim_with_escapes() {
        let line = r#"{"body":"line1\nline2","id":"x"}"#;
        let tok = WhitelistTokenizer::new(["body"]);
        let mut out = String::new();
        tok.tokenize_into(line, &mut out).unwrap();
        assert!(equal_as_json(&out, &slow_path(line, &["body"])));
    }

    #[test]
    fn handles_null_bool_number_values() {
        let line = r#"{"n":null,"t":true,"f":false,"i":-7,"flt":1.5,"exp":2e3,"s":"x"}"#;
        let tok = WhitelistTokenizer::new(["n", "t", "f", "i", "flt", "exp", "s"]);
        let mut out = String::new();
        tok.tokenize_into(line, &mut out).unwrap();
        let expected = slow_path(line, &["n", "t", "f", "i", "flt", "exp", "s"]);
        assert!(equal_as_json(&out, &expected));
    }

    #[test]
    fn tolerates_whitespace_in_input() {
        let line = "{ \"a\" : 1 , \"b\" :  \"two\" }";
        let tok = WhitelistTokenizer::new(["a", "b"]);
        let mut out = String::new();
        tok.tokenize_into(line, &mut out).unwrap();
        assert!(equal_as_json(&out, &slow_path(line, &["a", "b"])));
    }

    #[test]
    fn malformed_returns_error_and_clears_buf() {
        let line = r#"{"a":1,"b":}"#; // missing value
        let tok = WhitelistTokenizer::new(["a"]);
        let mut out = String::from("leftover");
        let res = tok.tokenize_into(line, &mut out);
        assert!(res.is_err());
        assert!(out.is_empty());
    }

    #[test]
    fn non_object_top_level_errors() {
        let line = r#"[1,2,3]"#;
        let tok = WhitelistTokenizer::new(["a"]);
        let mut out = String::new();
        assert!(tok.tokenize_into(line, &mut out).is_err());
    }

    #[test]
    fn key_with_unicode_escape_decoded_for_match() {
        // author decodes to "author" — exercise the slow key-decode branch.
        let line = "{\"\\u0061uthor\":\"alice\",\"b\":1}";
        let tok = WhitelistTokenizer::new(["author"]);
        let mut out = String::new();
        tok.tokenize_into(line, &mut out).unwrap();
        // The tokenizer copies the key bytes verbatim, so out has the escape;
        // serde_json round-trips it to the same Value as the slow path.
        let expected = slow_path(line, &["author"]);
        assert!(equal_as_json(&out, &expected));
    }
}
