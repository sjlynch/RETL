//! Edge cases for the byte-level human-timestamp rewriter
//! (`streaming::rewrite_human_timestamps_bytes`).
//!
//! The existing `extract_with_human_timestamps_only` test covers the happy
//! path on the synthetic corpus. Here we feed crafted .zst inputs that hit
//! branches not exercised by the basic corpus:
//!   - `edited` carrying a numeric timestamp (must convert)
//!   - `edited` set to `null` (must stay literal)
//!   - `retrieved_on` set to a string (must NOT be rewritten — only ints
//!     are converted)
//!   - `created_utc` as a negative epoch (pre-1970): must convert
//!   - Whitespace between `":` and the integer (compact serde never emits
//!     this; the rewriter is documented as tolerant of it).
//!
//! NOTE: the rewriter is invoked downstream of `parse_minimal`, which
//! deserializes `created_utc` as `Option<i64>`. Records with fractional
//! `created_utc` fail parse_minimal and are dropped before the rewriter is
//! reached, so they cannot be tested through the public API.

#[path = "common/mod.rs"]
mod common;

use common::*;
use retl::{RedditETL, Sources};
use serde_json::{json, Value};
use std::path::PathBuf;

/// Minimal "programming" corpus that lets the caller control the exact JSON
/// the rewriter sees. Only the comment file is populated.
fn build_corpus_with_lines(lines: Vec<String>) -> PathBuf {
    let dir = tempfile::tempdir().unwrap().into_path();
    let rc = dir.join("comments").join("RC_2006-01.zst");
    write_zst_lines(&rc, &lines);
    // empty submissions dir so plan_files for Both still works
    std::fs::create_dir_all(dir.join("submissions")).unwrap();
    dir
}

fn extract_human_only(base: &std::path::Path) -> Vec<Value> {
    let out = base.join("human_only.jsonl");
    // Intentionally NO date_range: bounds_tuple(None,None) returns None so
    // within_bounds is permissive — that lets us exercise edge cases like
    // negative epochs without the planner-level month gate dropping them.
    RedditETL::new()
        .base_dir(base)
        .timestamps_human_readable(true)
        .sources(Sources::Comments)
        .progress(false)
        .scan()
        .subreddit("programming")
        .extract_to_jsonl(&out)
        .unwrap();
    read_jsonl_values(&out)
}

#[test]
fn human_timestamps_convert_edited_when_integer() {
    // edited: 1136074800 (an int) must become an RFC3339 string.
    let lines = vec![json!({
        "subreddit":"programming","author":"alice","id":"c1",
        "body":"x","parent_id":"t3_s1",
        "created_utc": 1136074600_i64,
        "edited": 1136074800_i64
    })
    .to_string()];
    let base = build_corpus_with_lines(lines);
    let out = extract_human_only(&base);
    assert_eq!(out.len(), 1);
    let edited = out[0].get("edited").expect("edited should be present");
    let s = edited.as_str().expect("edited must be RFC3339 string when numeric");
    assert!(s.contains('T'), "edited not RFC3339-shaped: {}", s);
}

#[test]
fn human_timestamps_leave_edited_null_untouched() {
    // edited: null must stay literal `null` after rewrite.
    let lines = vec![json!({
        "subreddit":"programming","author":"alice","id":"c1",
        "body":"x","parent_id":"t3_s1",
        "created_utc": 1136074600_i64,
        "edited": serde_json::Value::Null
    })
    .to_string()];
    let base = build_corpus_with_lines(lines);
    let out = extract_human_only(&base);
    assert_eq!(out.len(), 1);
    let edited = out[0].get("edited").expect("edited must be present");
    assert!(edited.is_null(), "edited:null must remain null, got {:?}", edited);
}

#[test]
fn human_timestamps_leave_retrieved_on_string_untouched() {
    // retrieved_on as a STRING must not be rewritten.
    let s_in = "2006-01-01T00:00:00Z";
    let lines = vec![json!({
        "subreddit":"programming","author":"alice","id":"c1",
        "body":"x","parent_id":"t3_s1",
        "created_utc": 1136074600_i64,
        "retrieved_on": s_in
    })
    .to_string()];
    let base = build_corpus_with_lines(lines);
    let out = extract_human_only(&base);
    assert_eq!(out.len(), 1);
    let v = out[0].get("retrieved_on").unwrap();
    assert_eq!(
        v.as_str(),
        Some(s_in),
        "retrieved_on string must round-trip unchanged"
    );
}

#[test]
fn human_timestamps_handle_negative_epoch_created_utc() {
    // 1969-06-15 ~ -17_280_000s. Negative timestamps are valid Unix time;
    // the rewriter should handle them.
    let lines = vec![json!({
        "subreddit":"programming","author":"alice","id":"c1",
        "body":"x","parent_id":"t3_s1",
        "created_utc": -17_280_000_i64
    })
    .to_string()];
    let base = build_corpus_with_lines(lines);
    let out = extract_human_only(&base);
    assert_eq!(out.len(), 1);
    let v = out[0].get("created_utc").unwrap();
    let s = v.as_str().expect("negative epoch must convert to RFC3339");
    assert!(s.starts_with("1969"), "expected 1969 RFC3339, got {}", s);
}

#[test]
fn human_timestamps_handle_whitespace_between_colon_and_integer() {
    // Hand-written line with a space after `:`. The rewriter is documented as
    // tolerant of optional whitespace.
    let raw = r#"{"subreddit":"programming","author":"alice","id":"c1","body":"x","parent_id":"t3_s1","created_utc": 1136074600}"#;
    let lines = vec![raw.to_string()];
    let base = build_corpus_with_lines(lines);
    let out = extract_human_only(&base);
    assert_eq!(out.len(), 1);
    let v = out[0].get("created_utc").unwrap();
    let s = v.as_str().expect("whitespace before int must still parse and convert");
    assert!(s.contains('T'));
}

#[test]
fn non_target_keys_not_misidentified_when_string_contains_keyword_substring() {
    // A body containing a literal substring "created_utc" (inside a string
    // value) must NOT trigger a substring rewrite, because the rewriter
    // anchors on `"<key>":` which cannot occur inside a JSON string (the
    // inner `"` would be escaped). Verify that body content survives intact
    // and the real created_utc is still rewritten.
    let lines = vec![json!({
        "subreddit":"programming","author":"alice","id":"c1",
        "body":"talk about \"created_utc\" semantics",
        "parent_id":"t3_s1",
        "created_utc": 1136074600_i64
    })
    .to_string()];
    let base = build_corpus_with_lines(lines);
    let out = extract_human_only(&base);
    assert_eq!(out.len(), 1);
    let body = out[0].get("body").and_then(|v| v.as_str()).unwrap();
    assert!(
        body.contains("created_utc"),
        "body string must round-trip, got: {}",
        body
    );
    let cu = out[0].get("created_utc").and_then(|v| v.as_str()).unwrap();
    assert!(cu.contains('T'), "real created_utc must still be RFC3339");
}
