
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
fn leading_zero_number_falls_back_to_slow_path() {
    // `007` is not valid JSON; serde_json rejects the whole line. The
    // tokenizer must reject too so the caller falls back rather than copying
    // a non-JSON number verbatim into projected output.
    let line = r#"{"id":007,"author":"alice"}"#;
    assert!(serde_json::from_str::<Value>(line).is_err());
    let tok = WhitelistTokenizer::new(["id"]);
    let mut out = String::new();
    assert!(tok.tokenize_into(line, &mut out).is_err());
}

#[test]
fn plain_zero_and_zero_fraction_still_accepted() {
    let line = r#"{"a":0,"b":-0,"c":0.5,"d":10}"#;
    let tok = WhitelistTokenizer::new(["a", "b", "c", "d"]);
    let mut out = String::new();
    tok.tokenize_into(line, &mut out).unwrap();
    assert!(equal_as_json(&out, &slow_path(line, &["a", "b", "c", "d"])));
}

#[test]
fn non_object_top_level_errors() {
    let line = r#"[1,2,3]"#;
    let tok = WhitelistTokenizer::new(["a"]);
    let mut out = String::new();
    assert!(tok.tokenize_into(line, &mut out).is_err());
}

// ---------------------------------------------------------------------
// Duplicate-key handling. A repeated *whitelisted* top-level key would let
// the byte-copying fast path emit a duplicate-keyed object, while the slow
// `serde_json::Value` path collapses duplicates last-wins. The tokenizer
// rejects such lines so the caller falls back and both paths agree.
// ---------------------------------------------------------------------

#[test]
fn duplicate_whitelisted_key_is_rejected_for_slow_path_parity() {
    let line = r#"{"id":"a","id":"b"}"#;
    let tok = WhitelistTokenizer::new(["id"]);
    let mut out = String::from("leftover");
    let res = tok.tokenize_into(line, &mut out);
    assert!(res.is_err(), "duplicate whitelisted key must be rejected");
    assert!(out.is_empty(), "rejection must clear the output buffer");
    // The slow path the caller falls back to keeps the last value, so the
    // export bytes are well-defined and duplicate-free.
    assert_eq!(slow_path(line, &["id"]), r#"{"id":"b"}"#);
}

#[test]
fn duplicate_whitelisted_key_separated_by_other_keys_is_rejected() {
    // The two `id` occurrences straddle a non-whitelisted key and a second
    // whitelisted key — the duplicate must still be caught.
    let line = r#"{"id":"a","x":1,"author":"alice","id":"b"}"#;
    let tok = WhitelistTokenizer::new(["id", "author"]);
    let mut out = String::new();
    assert!(tok.tokenize_into(line, &mut out).is_err());
    assert!(out.is_empty());
}

#[test]
fn duplicate_non_whitelisted_key_is_accepted() {
    // Only whitelisted-key duplicates can diverge — a non-whitelisted key is
    // skipped on both paths however many times it appears, so the fast path
    // must NOT fall back here.
    let line = r#"{"x":"a","x":"b","id":"c"}"#;
    let tok = WhitelistTokenizer::new(["id"]);
    let mut out = String::new();
    tok.tokenize_into(line, &mut out).unwrap();
    assert!(equal_as_json(&out, &slow_path(line, &["id"])));
    assert_eq!(out, r#"{"id":"c"}"#);
}

#[test]
fn duplicate_whitelisted_key_via_unicode_escape_is_rejected() {
    // One occurrence is written with a `\u` escape that decodes to `id`. The
    // tokenizer decodes the key before the whitelist check, so the duplicate
    // is detected against the canonical form.
    let line = "{\"\\u0069d\":\"a\",\"id\":\"b\"}";
    let tok = WhitelistTokenizer::new(["id"]);
    let mut out = String::new();
    assert!(tok.tokenize_into(line, &mut out).is_err());
    assert!(out.is_empty());
}

#[test]
fn duplicate_key_rejection_clears_matched_indices() {
    let line = r#"{"id":"a","id":"b"}"#;
    let tok = WhitelistTokenizer::new(["id", "author"]);
    let mut out = String::new();
    let mut matched = vec![7, 9];
    assert!(tok
        .tokenize_into_with_matches(line, &mut out, &mut matched)
        .is_err());
    assert!(out.is_empty());
    assert!(matched.is_empty(), "rejection must clear matched_indices");
}

#[test]
fn fused_duplicate_timestamp_key_is_rejected() {
    let line = r#"{"created_utc":1136074600,"created_utc":1136074800}"#;
    let tok = WhitelistTokenizer::new(["created_utc"]);
    let mut out = String::new();
    assert!(tok
        .tokenize_and_rewrite_timestamps_into(line, &mut out)
        .is_err());
    assert!(out.is_empty());
}

// ---------------------------------------------------------------------
// Fused tokenize + rewrite-timestamps tests. The fused path must agree
// with the two-pass composition (tokenize_into → rewrite_human_timestamps_bytes)
// at the JSON-value level on every input the streaming pipeline could see.
// ---------------------------------------------------------------------

/// Reference: existing two-pass composition. Mirrors stream_job's
/// whitelist + human_timestamps branch.
fn two_pass(line: &str, fields: &[&str]) -> String {
    let tok = WhitelistTokenizer::new(fields.iter().copied());
    let mut a = String::new();
    tok.tokenize_into(line, &mut a).unwrap();
    let mut b = String::new();
    crate::streaming::rewrite_human_timestamps_bytes(&a, &mut b);
    b
}

#[test]
fn fused_rewrites_all_three_timestamp_keys() {
    let line =
        r#"{"created_utc":1136074600,"retrieved_on":1234567890,"edited":1136074800,"id":"x"}"#;
    let fields = ["created_utc", "retrieved_on", "edited", "id"];
    let tok = WhitelistTokenizer::new(fields);
    let mut out = String::new();
    tok.tokenize_and_rewrite_timestamps_into(line, &mut out)
        .unwrap();
    let expected = two_pass(line, &fields);
    assert!(
        equal_as_json(&out, &expected),
        "out={} expected={}",
        out,
        expected
    );
    // And explicitly: the integer must have become a quoted RFC3339 string.
    let got: Value = serde_json::from_str(&out).unwrap();
    assert!(got.get("created_utc").unwrap().is_string());
    assert!(got.get("retrieved_on").unwrap().is_string());
    assert!(got.get("edited").unwrap().is_string());
    assert!(got.get("id").unwrap().is_string());
}

#[test]
fn fused_leaves_timestamp_key_untouched_when_not_whitelisted() {
    // created_utc not in the whitelist — must be dropped, not rewritten.
    let line = r#"{"created_utc":1136074600,"id":"abc"}"#;
    let tok = WhitelistTokenizer::new(["id"]);
    let mut out = String::new();
    tok.tokenize_and_rewrite_timestamps_into(line, &mut out)
        .unwrap();
    assert!(equal_as_json(&out, &two_pass(line, &["id"])));
    assert_eq!(out, r#"{"id":"abc"}"#);
}

#[test]
fn fused_leaves_non_integer_timestamp_values_alone() {
    // null, bool, string, float — all must round-trip verbatim.
    let line = r#"{"edited":null,"created_utc":false,"retrieved_on":"2006-01-01T00:00:00Z"}"#;
    let fields = ["edited", "created_utc", "retrieved_on"];
    let tok = WhitelistTokenizer::new(fields);
    let mut out = String::new();
    tok.tokenize_and_rewrite_timestamps_into(line, &mut out)
        .unwrap();
    assert!(equal_as_json(&out, &two_pass(line, &fields)));
    let got: Value = serde_json::from_str(&out).unwrap();
    assert!(got.get("edited").unwrap().is_null());
    assert_eq!(got.get("created_utc").unwrap().as_bool(), Some(false));
    assert_eq!(
        got.get("retrieved_on").unwrap().as_str(),
        Some("2006-01-01T00:00:00Z")
    );
}

#[test]
fn fused_leaves_float_timestamp_alone() {
    // A fractional `created_utc` is not a JSON integer and must not be
    // rewritten. Two-pass composition does the same.
    let line = r#"{"created_utc":1.5}"#;
    let tok = WhitelistTokenizer::new(["created_utc"]);
    let mut out = String::new();
    tok.tokenize_and_rewrite_timestamps_into(line, &mut out)
        .unwrap();
    assert!(equal_as_json(&out, &two_pass(line, &["created_utc"])));
    assert_eq!(out, r#"{"created_utc":1.5}"#);
}

#[test]
fn fused_handles_negative_epoch() {
    let line = r#"{"created_utc":-17280000}"#;
    let tok = WhitelistTokenizer::new(["created_utc"]);
    let mut out = String::new();
    tok.tokenize_and_rewrite_timestamps_into(line, &mut out)
        .unwrap();
    let got: Value = serde_json::from_str(&out).unwrap();
    let s = got.get("created_utc").unwrap().as_str().unwrap();
    assert!(s.starts_with("1969"), "expected 1969 RFC3339, got {}", s);
}

#[test]
fn fused_rewrites_timestamp_keys_only() {
    // A non-timestamp integer field must be emitted verbatim, even though
    // it's whitelisted.
    let line = r#"{"score":42,"created_utc":1136074600}"#;
    let fields = ["score", "created_utc"];
    let tok = WhitelistTokenizer::new(fields);
    let mut out = String::new();
    tok.tokenize_and_rewrite_timestamps_into(line, &mut out)
        .unwrap();
    assert!(equal_as_json(&out, &two_pass(line, &fields)));
    let got: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(got.get("score").unwrap().as_i64(), Some(42));
    assert!(got.get("created_utc").unwrap().is_string());
}

#[test]
fn fused_preserves_nested_value_bytes() {
    // Nested object/array values are emitted verbatim as raw bytes.
    let line = r#"{"created_utc":1136074600,"meta":{"a":[1,2,3]}}"#;
    let fields = ["created_utc", "meta"];
    let tok = WhitelistTokenizer::new(fields);
    let mut out = String::new();
    tok.tokenize_and_rewrite_timestamps_into(line, &mut out)
        .unwrap();
    assert!(equal_as_json(&out, &two_pass(line, &fields)));
}

#[test]
fn fused_malformed_returns_error_and_clears_buf() {
    let line = r#"{"created_utc":}"#;
    let tok = WhitelistTokenizer::new(["created_utc"]);
    let mut out = String::from("leftover");
    assert!(tok
        .tokenize_and_rewrite_timestamps_into(line, &mut out)
        .is_err());
    assert!(out.is_empty());
}

#[test]
fn fused_handles_unicode_escaped_timestamp_key() {
    // JSON-encoded "created_utc" with the leading 'c' as a \u escape. The
    // tokenizer must decode the key, recognize it as a timestamp key, and
    // rewrite the integer.
    let line = "{\"\\u0063reated_utc\":1136074600}";
    let tok = WhitelistTokenizer::new(["created_utc"]);
    let mut out = String::new();
    tok.tokenize_and_rewrite_timestamps_into(line, &mut out)
        .unwrap();
    let got: Value = serde_json::from_str(&out).unwrap();
    let s = got.get("created_utc").unwrap().as_str().unwrap();
    assert!(s.contains('T'), "expected RFC3339, got {}", s);
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
