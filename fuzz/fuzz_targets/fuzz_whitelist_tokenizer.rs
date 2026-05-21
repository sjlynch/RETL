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
//!
//! The one valid object shape the tokenizer is *expected* to reject is a line
//! that repeats a whitelisted top-level key: `serde_json::Value` collapses
//! duplicate keys last-wins, so the byte-copying fast path must defer to the
//! slow path rather than emit a duplicate-keyed object. `has_duplicate_-
//! whitelisted_key` lets contract 3's regression check exempt that case.

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

/// Reports whether `line`'s top-level JSON object repeats any whitelisted key.
///
/// `serde_json::Value` silently collapses duplicate object keys (last value
/// wins), so the byte-copying tokenizer is *expected* to reject such a line
/// and defer to the slow path — otherwise the two paths would emit different
/// bytes for the same input. This lets contract 3's regression check tell a
/// sanctioned duplicate-key rejection apart from a genuine fast-path
/// regression. Only meaningful for inputs that are themselves JSON objects;
/// any parse hiccup conservatively reports `false`.
fn has_duplicate_whitelisted_key(line: &str) -> bool {
    use serde::de::{Deserializer, IgnoredAny, MapAccess, Visitor};
    use std::collections::HashSet;
    use std::fmt;

    struct DupVisitor;

    impl<'de> Visitor<'de> for DupVisitor {
        type Value = bool;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a JSON object")
        }

        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<bool, A::Error> {
            let mut seen: HashSet<String> = HashSet::new();
            let mut duplicate = false;
            while let Some(key) = map.next_key::<String>()? {
                map.next_value::<IgnoredAny>()?;
                let whitelisted = WHITELIST.contains(&key.as_str());
                if whitelisted && !seen.insert(key) {
                    duplicate = true;
                }
            }
            Ok(duplicate)
        }
    }

    let mut de = serde_json::Deserializer::from_str(line);
    de.deserialize_map(DupVisitor).unwrap_or(false)
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
            // path has regressed. The one sanctioned exception is a line that
            // repeats a whitelisted top-level key: the tokenizer deliberately
            // rejects it so the slow path — the single source of truth for
            // duplicate-key last-wins semantics — produces the output.
            if matches!(input, Some(Value::Object(_))) && !has_duplicate_whitelisted_key(line) {
                panic!("tokenizer rejected a valid top-level JSON object: {line}");
            }
        }
    }
});
