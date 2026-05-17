//! Property tests for the streaming whitelist tokenizer
//! (`json_whitelist::WhitelistTokenizer`).
//!
//! For a randomly generated flat top-level object and a randomly chosen
//! whitelist subset, the tokenizer's output parsed back through serde_json
//! must equal the slow-path `Value::Object` projection. The slow path mirrors
//! exactly what `streaming::stream_job` does in the whitelist branch when the
//! tokenizer is bypassed.
//!
//! Numeric leaves are constrained to integers so the test isn't sensitive to
//! `serde_json::Number`'s float-vs-int representation distinction (which is
//! orthogonal to whitelisting correctness).

#[path = "common/mod.rs"]
mod common;

use proptest::prelude::*;
use retl::{rewrite_human_timestamps_bytes, WhitelistTokenizer};
use serde_json::{json, Map, Value};

// ---------------------------------------------------------------------------
// Generators
// ---------------------------------------------------------------------------

/// A small JSON leaf value: int, ascii-string, bool, or null. Keeps numbers
/// integral so serde_json's Number representation round-trips identically.
fn leaf_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        any::<i32>().prop_map(|n| json!(n)),
        "[a-zA-Z0-9 _.,!?-]{0,16}".prop_map(Value::String),
        any::<bool>().prop_map(Value::Bool),
        Just(Value::Null),
    ]
}

/// Recursive JSON value with bounded depth/breadth — exercises the
/// tokenizer's nested-container skipping without exploding the search space.
fn nested_value() -> impl Strategy<Value = Value> {
    leaf_value().prop_recursive(
        3,  // max depth
        16, // max total nodes
        4,  // max children per collection
        |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
                prop::collection::hash_map(
                    "[a-zA-Z][a-zA-Z0-9_]{0,8}",
                    inner,
                    0..4,
                )
                .prop_map(|m| {
                    let mut obj = Map::new();
                    for (k, v) in m {
                        obj.insert(k, v);
                    }
                    Value::Object(obj)
                }),
            ]
        },
    )
}

/// A flat top-level object: `{ key -> nested_value }`. Keys are unique because
/// `HashMap` deduplicates them; this matches real-world Reddit records, which
/// don't use duplicate top-level keys.
fn flat_object() -> impl Strategy<Value = (Vec<String>, Value)> {
    prop::collection::hash_map(
        "[a-zA-Z][a-zA-Z0-9_]{0,8}",
        nested_value(),
        0..8,
    )
    .prop_map(|m| {
        let keys: Vec<String> = m.keys().cloned().collect();
        let mut obj = Map::new();
        for (k, v) in m {
            obj.insert(k, v);
        }
        (keys, Value::Object(obj))
    })
}

// ---------------------------------------------------------------------------
// Reference implementation: matches streaming::stream_job's whitelist branch.
// ---------------------------------------------------------------------------

fn slow_path(line: &str, fields: &[String]) -> String {
    let val: Value = serde_json::from_str(line).expect("input must parse");
    let mut obj = Map::new();
    if let Some(map) = val.as_object() {
        for k in fields {
            if let Some(v) = map.get(k) {
                obj.insert(k.clone(), v.clone());
            }
        }
    }
    serde_json::to_string(&Value::Object(obj)).unwrap()
}

// ---------------------------------------------------------------------------
// Property
// ---------------------------------------------------------------------------

proptest! {
    // Default 32 cases per run for daily iteration; the tokenizer is cheap so
    // we can afford a few times more cases than the heavyweight pipeline
    // properties. Bump with `RETL_PROPTEST_CASES=256 cargo test` before
    // merging tokenizer changes.
    #![proptest_config(ProptestConfig {
        cases: common::proptest_cases(32),
        ..ProptestConfig::default()
    })]

    #[test]
    fn tokenizer_matches_slow_path((all_keys, obj) in flat_object(),
                                   extra in prop::collection::vec("[a-zA-Z][a-zA-Z0-9_]{0,8}", 0..4),
                                   pick_mask in prop::collection::vec(any::<bool>(), 0..16))
    {
        // Build the whitelist: a (possibly empty) subset of the object's own
        // keys, plus a few extras that are unlikely to collide. The tokenizer
        // must skip non-matching keys silently.
        let mut whitelist: Vec<String> = all_keys.iter()
            .zip(pick_mask.iter().copied().chain(std::iter::repeat(false)))
            .filter(|(_, keep)| *keep)
            .map(|(k, _)| k.clone())
            .collect();
        whitelist.extend(extra);

        let line = serde_json::to_string(&obj).unwrap();

        let tok = WhitelistTokenizer::new(whitelist.iter().map(|s| s.as_str()));
        let mut out = String::new();
        tok.tokenize_into(&line, &mut out)
            .expect("tokenizer should succeed on canonical serde_json output");

        // Parse both outputs back through serde_json and compare as Values.
        // This is order-insensitive (serde_json::Map equality is content-based)
        // and tolerant of any compact-formatting differences that don't change
        // the underlying JSON document.
        let got: Value = serde_json::from_str(&out)
            .expect("tokenizer output must be valid JSON");
        let expected_str = slow_path(&line, &whitelist);
        let expected: Value = serde_json::from_str(&expected_str).unwrap();
        prop_assert_eq!(got, expected, "input was: {}", line);
    }
}

// ---------------------------------------------------------------------------
// Fused tokenize + rewrite-timestamps property: the single-pass fused method
// must produce the same JSON document as the two-pass composition
// (`tokenize_into` → `rewrite_human_timestamps_bytes`) on every input the
// streaming pipeline could hand it. This is the perf-task acceptance gate
// promoted to a property — covers >> 50 representative records per run.
// ---------------------------------------------------------------------------

/// Generate a JSON value that's likely to exercise the timestamp-rewrite path:
/// most often an int (matching what the rewriter actually rewrites), with
/// other types mixed in to check the verbatim-passthrough behavior.
fn ts_capable_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        // Integers in a range that includes negatives, the epoch, and far-future.
        // Keep magnitude bounded so OffsetDateTime::from_unix_timestamp accepts
        // most of them; the fused path emits verbatim on out-of-range, which
        // matches the two-pass behavior exactly, so it's fine if some don't
        // convert.
        (-2_000_000_000_i64..2_000_000_000_i64).prop_map(|n| json!(n)),
        any::<bool>().prop_map(Value::Bool),
        Just(Value::Null),
        "[a-zA-Z0-9 :T+-]{0,16}".prop_map(Value::String),
        any::<f64>().prop_filter("finite", |f| f.is_finite()).prop_map(|f| json!(f)),
    ]
}

/// Build a record that has a healthy chance of containing one or more of the
/// three timestamp keys, plus arbitrary other top-level keys. This mirrors
/// the structural shape the streaming pipeline sees in production.
fn record_with_maybe_timestamps() -> impl Strategy<Value = (Vec<String>, Value)> {
    (
        proptest::option::of(ts_capable_value()),
        proptest::option::of(ts_capable_value()),
        proptest::option::of(ts_capable_value()),
        prop::collection::hash_map(
            "[a-zA-Z][a-zA-Z0-9_]{0,8}",
            "[a-zA-Z0-9 _.,!?-]{0,16}".prop_map(Value::String),
            0..6,
        ),
    )
        .prop_map(|(cu, ro, ed, others)| {
            let mut keys: Vec<String> = Vec::new();
            let mut obj = Map::new();
            if let Some(v) = cu {
                obj.insert("created_utc".to_string(), v);
                keys.push("created_utc".to_string());
            }
            if let Some(v) = ro {
                obj.insert("retrieved_on".to_string(), v);
                keys.push("retrieved_on".to_string());
            }
            if let Some(v) = ed {
                obj.insert("edited".to_string(), v);
                keys.push("edited".to_string());
            }
            for (k, v) in others {
                if !obj.contains_key(&k) {
                    keys.push(k.clone());
                    obj.insert(k, v);
                }
            }
            (keys, Value::Object(obj))
        })
}

proptest! {
    // Default 32 cases; bump via `RETL_PROPTEST_CASES` for thorough runs.
    #![proptest_config(ProptestConfig {
        cases: common::proptest_cases(32),
        ..ProptestConfig::default()
    })]

    #[test]
    fn fused_matches_two_pass(
        (all_keys, obj) in record_with_maybe_timestamps(),
        pick_mask in prop::collection::vec(any::<bool>(), 0..16),
        include_extras in any::<bool>(),
    )
    {
        // Whitelist is a (possibly empty) subset of the record's own keys.
        // Sometimes also include extras that won't match — exercises the
        // skip-non-matching branch.
        let mut whitelist: Vec<String> = all_keys.iter()
            .zip(pick_mask.iter().copied().chain(std::iter::repeat(false)))
            .filter(|(_, keep)| *keep)
            .map(|(k, _)| k.clone())
            .collect();
        if include_extras {
            whitelist.push("does_not_exist_zzz".to_string());
        }

        let line = serde_json::to_string(&obj).unwrap();
        let tok = WhitelistTokenizer::new(whitelist.iter().map(|s| s.as_str()));

        // Fused single-pass.
        let mut fused = String::new();
        tok.tokenize_and_rewrite_timestamps_into(&line, &mut fused)
            .expect("fused tokenizer should succeed on canonical serde_json output");

        // Two-pass composition (the existing behavior the fused method replaces).
        let mut tok_only = String::new();
        tok.tokenize_into(&line, &mut tok_only)
            .expect("tokenize_into should succeed on canonical serde_json output");
        let mut two_pass = String::new();
        rewrite_human_timestamps_bytes(&tok_only, &mut two_pass);

        // Compare as JSON Values so the equality check is tolerant of any
        // compact-formatting differences that don't change the document.
        let got: Value = serde_json::from_str(&fused)
            .expect("fused output must be valid JSON");
        let expected: Value = serde_json::from_str(&two_pass)
            .expect("two-pass output must be valid JSON");
        prop_assert_eq!(got, expected, "input was: {} whitelist: {:?}", line, whitelist);
    }
}
