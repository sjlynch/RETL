//! Tests for the WhitelistTokenizer fallback contract.
//!
//! `json_whitelist`'s module doc promises: when `tokenize_into` returns
//! `TokenizerError::Malformed`, the caller (`streaming::stream_job`'s whitelist
//! branch) falls back to the `serde_json::Value` slow path so we never regress
//! correctness on an odd record. Perf changes to the tokenizer must not turn a
//! "fall back" case into a hard error at the top level.
//!
//! These tests lock that contract from two directions:
//!
//! 1. **Direct tokenizer rejection** — pin the exact inputs the strict
//!    top-level walker refuses, so a future tokenizer that becomes more
//!    lenient (or stricter in the wrong way) trips the test.
//!
//! 2. **End-to-end through `stream_job`** — drive the same line shapes through
//!    a real `RedditETL::scan().whitelist_fields().export_partitioned()`,
//!    which is the only public path that touches `streaming::stream_job`.
//!    The export must not surface a `Malformed` upstream, and the bytes it
//!    writes must equal the slow-path projection
//!    (`serde_json::from_str(line)` → keep whitelisted keys → re-serialize)
//!    regardless of which internal branch each line takes.

#[path = "common/mod.rs"]
mod common;

use common::{decompress_zst_lines, read_jsonl_values, write_zst_lines};
use retl::{ExportFormat, RedditETL, Sources, WhitelistTokenizer, YearMonth};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

const WHITELIST: &[&str] = &["id", "author", "subreddit", "created_utc", "score", "body"];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Mirror of `streaming::stream_job`'s slow path: parse the line as a Value
/// and keep only the whitelisted top-level keys. This is the reference the
/// fallback must match.
fn slow_path_projection(line: &str, fields: &[&str]) -> String {
    let val: Value = serde_json::from_str(line)
        .expect("slow path requires serde_json::from_str to succeed");
    let mut obj = Map::new();
    if let Some(map) = val.as_object() {
        for k in fields {
            if let Some(v) = map.get(*k) {
                obj.insert((*k).to_string(), v.clone());
            }
        }
    }
    serde_json::to_string(&Value::Object(obj)).unwrap()
}

/// Assert that `WhitelistTokenizer::tokenize_into` rejects `line` with
/// `Malformed` and that the output buffer is left empty (the documented
/// post-condition the slow-path caller relies on).
fn assert_tokenizer_rejects(line: &str) {
    let tok = WhitelistTokenizer::new(WHITELIST.iter().copied());
    let mut buf = String::from("must-be-cleared-on-error");
    let res = tok.tokenize_into(line, &mut buf);
    assert!(
        res.is_err(),
        "tokenizer should have rejected the line but accepted: {}",
        line
    );
    assert!(
        buf.is_empty(),
        "tokenizer must clear `out` on Malformed; line was: {}",
        line
    );
}

/// Sanity guard: the slow path can only handle inputs serde_json itself
/// parses. If this assertion ever fails for a line we drive through
/// `stream_job`, the export would error out instead of falling back.
fn assert_serde_json_accepts(line: &str) {
    let v: Result<Value, _> = serde_json::from_str(line);
    assert!(
        v.is_ok(),
        "slow path requires serde_json::from_str to succeed on: {} (err: {:?})",
        line,
        v.err()
    );
}

/// Build a one-month, comments-only corpus from `lines`. All lines must be
/// JSONL records (no embedded `\n`). Returns the corpus base dir.
fn make_corpus_from_lines(lines: &[String]) -> PathBuf {
    let dir = tempfile::tempdir().unwrap().keep();
    let rc_path = dir.join("comments").join("RC_2006-01.zst");
    write_zst_lines(&rc_path, lines);
    // The pipeline's discover_all walks both comments_dir and submissions_dir;
    // create an empty submissions dir so discovery returns cleanly.
    std::fs::create_dir_all(dir.join("submissions")).unwrap();
    dir
}

// ---------------------------------------------------------------------------
// Section 1: direct tokenizer-rejection contract
// ---------------------------------------------------------------------------

/// Top-level array: the tokenizer's strict walker requires `{` at top-level
/// (after JSON whitespace). serde_json itself accepts `[...]` as a valid
/// `Value::Array`, so the rejection here is a tokenizer-only constraint —
/// exactly the kind of "structurally surprising" input the module doc
/// promises will fall back rather than error.
#[test]
fn tokenizer_rejects_top_level_array_even_though_serde_accepts_it() {
    let line = r#"[{"id":"x","author":"a"},{"id":"y","author":"b"}]"#;
    assert_tokenizer_rejects(line);
    assert_serde_json_accepts(line);
}

/// Top-level scalars are valid JSON values but not `flat top-level objects`
/// the tokenizer walks. Rejection here keeps the contract crisp: anything
/// that isn't `{ ... }` falls through to the slow path.
#[test]
fn tokenizer_rejects_top_level_scalar_even_though_serde_accepts_it() {
    let line = r#""just a string""#;
    assert_tokenizer_rejects(line);
    assert_serde_json_accepts(line);
}

/// Trailing-comma object: the tokenizer's strict walker, after consuming
/// `,`, requires another `"`-quoted key, so `{"a":1,}` is `Malformed`.
/// serde_json itself also rejects trailing commas by default, so this
/// case can't reach the slow path through `stream_job` — but pinning the
/// rejection here defends against a future tokenizer that quietly lets
/// trailing commas through (which would emit invalid JSON downstream
/// because the tokenizer copies raw bytes verbatim).
#[test]
fn tokenizer_rejects_trailing_comma_object() {
    assert_tokenizer_rejects(r#"{"a":1,"b":2,}"#);
    assert_tokenizer_rejects(r#"{"id":"x",}"#);
}

/// Truly malformed input (missing value). Both paths reject — a `stream_job`
/// fed this line would error upstream, not fall back. Included to document
/// the boundary between "fall back" and "give up".
#[test]
fn tokenizer_rejects_missing_value() {
    let line = r#"{"a":1,"b":}"#;
    assert_tokenizer_rejects(line);
    assert!(serde_json::from_str::<Value>(line).is_err());
}

// ---------------------------------------------------------------------------
// Section 2: end-to-end through `stream_job` (via `export_partitioned`)
// ---------------------------------------------------------------------------

/// Build a comments-only corpus where every line is:
///   - a syntactically valid JSON object (so `serde_json::from_str` succeeds),
///   - parseable as `MinimalRecord` (so `parse_minimal` lets it through into
///     the whitelist branch),
///   - structurally unusual at the byte level (deep nesting, JSON whitespace
///     in non-canonical positions, escaped strings, mixed nested containers).
///
/// On these lines the tokenizer's success path and slow path must agree.
/// If a future tokenizer regresses and rejects one of these as Malformed,
/// the slow path still serves it — and the assertions below still hold.
fn build_quirky_valid_corpus() -> (PathBuf, Vec<String>) {
    let lines = vec![
        // Canonical record (sanity baseline).
        r#"{"id":"c1","author":"alice","subreddit":"programming","score":1,"created_utc":1136073700,"body":"hi"}"#.to_string(),

        // Deeply nested arrays inside a non-whitelisted top-level field —
        // exercises depth tracking inside skip_container.
        r#"{"id":"c2","author":"bob","subreddit":"programming","score":2,"created_utc":1136073800,"body":"deep","extras":[[[[[[[[1,2,3]]]]]]]]}"#.to_string(),

        // JSON whitespace at every legal position (around `:` and `,`,
        // before and after `{` / `}`). All should be transparent to both
        // the tokenizer's skip_ws and to serde_json.
        "{ \"id\" : \"c3\" , \"author\" : \"charlie\" , \"subreddit\" : \"programming\" , \"score\" : 3 , \"created_utc\" : 1136073900 , \"body\" : \"ws\" }".to_string(),

        // Nested objects/arrays containing escaped quotes and backslashes —
        // skip_container must walk past these without treating an escaped
        // quote as a top-level structural boundary.
        r#"{"id":"c4","author":"dana","subreddit":"programming","score":4,"created_utc":1136074000,"body":"esc","meta":{"u":"a\"b","arr":[{"k":"v\\n"}]}}"#.to_string(),

        // Non-whitelisted keys at multiple positions (head, middle, tail) —
        // verifies the tokenizer's pair-skipping book-keeping (first_pair vs
        // emitted_any) interacts correctly with the slow path's projection.
        r#"{"_garbage":["junk","fields"],"id":"c5","_more":{"ignored":true},"author":"eve","subreddit":"programming","score":5,"created_utc":1136074100,"_trailing":null,"body":"mid"}"#.to_string(),
    ];

    let base = make_corpus_from_lines(&lines);
    (base, lines)
}

/// Run an actual export over the quirky-but-valid corpus and verify that
/// every output record equals the slow-path projection of the corresponding
/// input line. This is the end-to-end fallback assertion: regardless of
/// whether each line went through the tokenizer or the slow path, the bytes
/// on disk must match what the slow path alone would produce.
#[test]
fn stream_job_jsonl_export_matches_slow_path_projection_on_quirky_objects() {
    let (base, original_lines) = build_quirky_valid_corpus();
    let out_dir = base.join("out_jsonl");

    // Pre-flight: every input must be serde-parseable so the slow path can
    // serve it. If this fails the tested contract isn't well-defined.
    for line in &original_lines {
        assert_serde_json_accepts(line);
    }

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .whitelist_fields(WHITELIST.iter().copied())
        .export_partitioned(&out_dir, ExportFormat::Jsonl)
        .expect("export must not surface a tokenizer Malformed upstream");

    let exported = out_dir.join("comments").join("RC_2006-01.jsonl");
    assert!(
        exported.exists(),
        "expected exported jsonl at {}",
        exported.display()
    );

    let exported_values: Vec<Value> = read_jsonl_values(&exported);
    let mut by_id: BTreeMap<String, Value> = BTreeMap::new();
    for v in exported_values {
        let id = v
            .get("id")
            .and_then(|s| s.as_str())
            .expect("exported record without id")
            .to_string();
        by_id.insert(id, v);
    }
    assert_eq!(
        by_id.len(),
        original_lines.len(),
        "every input record should be exported exactly once"
    );

    for line in &original_lines {
        let expected_str = slow_path_projection(line, WHITELIST);
        let expected: Value = serde_json::from_str(&expected_str).unwrap();
        let id = expected
            .get("id")
            .and_then(|s| s.as_str())
            .unwrap()
            .to_string();
        let got = by_id
            .get(&id)
            .unwrap_or_else(|| panic!("missing exported id={}", id));
        assert_eq!(
            got, &expected,
            "exported record for id={} must equal the slow-path projection of: {}",
            id, line
        );
    }
}

/// Same end-to-end assertion against the Zst export path. The two formats
/// share the `stream_job` core but route writes differently — running both
/// guards against a format-specific regression in the fallback path.
#[test]
fn stream_job_zst_export_matches_slow_path_projection_on_quirky_objects() {
    let (base, original_lines) = build_quirky_valid_corpus();
    let out_dir = base.join("out_zst");

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .whitelist_fields(WHITELIST.iter().copied())
        .export_partitioned(&out_dir, ExportFormat::Zst)
        .expect("zst export must not surface a tokenizer Malformed upstream");

    let exported = out_dir.join("comments").join("RC_2006-01.zst");
    assert!(exported.exists());

    let raw_lines = decompress_zst_lines(&exported);
    let allowed: BTreeSet<&str> = WHITELIST.iter().copied().collect();

    let mut by_id: BTreeMap<String, Value> = BTreeMap::new();
    for raw in &raw_lines {
        let v: Value = serde_json::from_str(raw).unwrap_or_else(|e| {
            panic!("exported line not valid JSON: {} (err: {})", raw, e)
        });
        // No non-whitelisted key may leak through either path.
        for k in v
            .as_object()
            .expect("exported record must be a JSON object")
            .keys()
        {
            assert!(
                allowed.contains(k.as_str()),
                "non-whitelisted key `{}` leaked into export of: {}",
                k,
                raw
            );
        }
        let id = v.get("id").and_then(|s| s.as_str()).unwrap().to_string();
        by_id.insert(id, v);
    }

    for line in &original_lines {
        let expected: Value = serde_json::from_str(&slow_path_projection(line, WHITELIST)).unwrap();
        let id = expected.get("id").and_then(|s| s.as_str()).unwrap();
        assert_eq!(
            by_id.get(id),
            Some(&expected),
            "Zst-exported record for id={} must equal the slow-path projection",
            id
        );
    }
}

/// Mix a tokenizer-rejected, *non-object* line (top-level array) into a
/// corpus alongside normal records. `parse_minimal` rejects the array line
/// so it never reaches the whitelist branch — but the contract is the same
/// from the caller's perspective: no panic, no error surfaced upstream, the
/// good records still export verbatim per the slow-path projection.
///
/// This locks the "no Malformed surfaced upstream" half of the fallback
/// contract end-to-end: even when stream_job sees a line the tokenizer
/// would reject, the export completes successfully.
#[test]
fn stream_job_drops_tokenizer_rejected_non_objects_without_error() {
    let good = r#"{"id":"g1","author":"alice","subreddit":"programming","score":1,"created_utc":1136073700,"body":"hi"}"#;
    let bad_array = r#"[{"id":"bad","author":"never","subreddit":"programming"}]"#;

    // Pre-conditions for this test's premise:
    assert_tokenizer_rejects(bad_array);
    assert_serde_json_accepts(bad_array); // serde sees a Value::Array
    let lines = vec![good.to_string(), bad_array.to_string()];
    let base = make_corpus_from_lines(&lines);
    let out_dir = base.join("out_mixed");

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .whitelist_fields(WHITELIST.iter().copied())
        .export_partitioned(&out_dir, ExportFormat::Jsonl)
        .expect("export must complete even with tokenizer-rejected non-object lines");

    let exported = out_dir.join("comments").join("RC_2006-01.jsonl");
    let values = read_jsonl_values(&exported);
    assert_eq!(values.len(), 1, "only the valid object should be exported");

    let expected: Value = serde_json::from_str(&slow_path_projection(good, WHITELIST)).unwrap();
    assert_eq!(values[0], expected);
}

/// A JSON object whose `author` key is written with a `\u`-escape that
/// decodes to `author`. This pushes the tokenizer through its slow
/// key-decode branch (`serde_json::from_slice::<String>(raw)` inside
/// `tokenize_into`) — the escaped key bytes don't equal the whitelist's
/// raw-byte entry, so matching only succeeds via canonical decoding.
///
/// On the tokenizer's success path the emitted bytes still carry the
/// escape; on the slow path serde re-serializes the canonical form. Both
/// decode to the same `Value`, which is the equivalence the fallback
/// contract is really about — if the escaped-key branch ever started
/// returning Malformed for inputs serde accepts, the slow path must
/// still produce identical content.
#[test]
fn stream_job_handles_escaped_keys_via_canonical_decode() {
    // Build the line at runtime so the escape sequence in the JSON source
    // can't be misread by the test author. The `author` form is what
    // ends up on disk in the corpus.
    let escaped_author_key = "\\u0061uthor";
    let line = format!(
        r#"{{"id":"k1","{key}":"alice","subreddit":"programming","score":1,"created_utc":1136073700,"body":"esc-key"}}"#,
        key = escaped_author_key
    );
    assert_serde_json_accepts(&line);

    // Sanity: the literal bytes of the key on disk are NOT `author`,
    // but serde_json decodes them to `author` so the slow path projects
    // correctly. The tokenizer must reach the same answer through its
    // escaped-key branch.
    let parsed: Value = serde_json::from_str(&line).unwrap();
    assert!(
        parsed.get("author").is_some(),
        "line's escaped key must decode to `author`"
    );

    let lines = vec![line.clone()];
    let base = make_corpus_from_lines(&lines);
    let out_dir = base.join("out_escaped_key");

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .whitelist_fields(WHITELIST.iter().copied())
        .export_partitioned(&out_dir, ExportFormat::Jsonl)
        .expect("export must succeed for escaped-key records");

    let exported = out_dir.join("comments").join("RC_2006-01.jsonl");
    let values = read_jsonl_values(&exported);
    assert_eq!(values.len(), 1);

    let expected: Value = serde_json::from_str(&slow_path_projection(&line, WHITELIST)).unwrap();
    // `Value` equality compares object contents (key set + values), so it
    // doesn't matter whether the export carried the escaped or decoded form
    // of the `author` key — both decode to the same JSON document.
    assert_eq!(values[0], expected);
}
