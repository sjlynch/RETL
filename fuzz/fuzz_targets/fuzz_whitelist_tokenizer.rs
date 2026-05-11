#![no_main]
//! Fuzz target for `retl::WhitelistTokenizer::tokenize_into`.
//!
//! The whitelist tokenizer is a hand-rolled byte scanner over a JSONL line's
//! top-level object. It recognizes string keys (including escaped keys), skips
//! arbitrary JSON values, and copies only selected `key:raw_value` slices into a
//! compact output object. Production falls back to `serde_json::Value` when the
//! tokenizer reports malformed input, so malformed lines may return `Err(_)` —
//! but they must never panic or abort.
//!
//! This target validates three contracts:
//!
//! 1. The tokenizer always returns normally for arbitrary UTF-8 input.
//! 2. Any successful output is a valid UTF-8 `String` and, when the input was
//!    valid JSON, parses back through `serde_json::from_str`.
//! 3. For valid top-level JSON objects, successful tokenizer output matches the
//!    slow `serde_json::Value` projection for a fixed set of common Reddit
//!    fields.

use libfuzzer_sys::fuzz_target;
use serde_json::Value;

const WHITELIST: &[&str] = &[
    "id",
    "name",
    "subreddit",
    "subreddit_id",
    "author",
    "author_fullname",
    "created_utc",
    "retrieved_on",
    "edited",
    "score",
    "body",
    "selftext",
    "title",
    "parent_id",
    "link_id",
    "domain",
    "url",
    "permalink",
    "distinguished",
    "stickied",
    "is_self",
    "over_18",
];

fn projected_value(input: &serde_json::Map<String, Value>) -> Value {
    let mut out = serde_json::Map::new();
    for &field in WHITELIST {
        if let Some(value) = input.get(field) {
            out.insert(field.to_owned(), value.clone());
        }
    }
    Value::Object(out)
}

fuzz_target!(|data: &[u8]| {
    // Production receives decoded JSONL lines as `&str`, so only valid UTF-8
    // reaches this entry point. Invalid JSON over valid UTF-8 is still in scope.
    let Ok(line) = std::str::from_utf8(data) else {
        return;
    };

    let input = serde_json::from_str::<Value>(line).ok();
    let tokenizer = retl::WhitelistTokenizer::new(WHITELIST.iter().copied());
    let mut out = String::from("stale output must be cleared");

    match tokenizer.tokenize_into(line, &mut out) {
        Ok(()) => {
            // Explicitly exercise the UTF-8 contract even though `String`
            // already enforces it at the type level.
            std::str::from_utf8(out.as_bytes()).expect("tokenizer output must be valid UTF-8");

            if let Some(input_value) = input {
                let output_value: Value = serde_json::from_str(&out)
                    .expect("tokenizer output for valid JSON input must parse as JSON");

                if let Value::Object(input_object) = input_value {
                    assert_eq!(
                        output_value,
                        projected_value(&input_object),
                        "tokenizer projection diverged from serde_json slow path\n\
                         input:  {line}\n\
                         output: {out}"
                    );
                }
            }
        }
        Err(_) => {
            // Public contract: on error the reusable output buffer is left
            // empty so callers cannot accidentally consume stale bytes.
            assert!(
                out.is_empty(),
                "tokenizer errors must clear the output buffer"
            );

            // A valid top-level object is the production happy path; if that
            // shape is rejected, fallback preserves correctness but the fast
            // path has regressed.
            if matches!(input, Some(Value::Object(_))) {
                panic!("tokenizer rejected a valid top-level JSON object: {line}");
            }
        }
    }
});
