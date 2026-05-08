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

use proptest::prelude::*;
use retl::WhitelistTokenizer;
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
    #![proptest_config(ProptestConfig {
        // Modest case count so test time stays reasonable; the generator covers
        // the structural variants (leaves / nested arrays / nested objects /
        // empty whitelist / whitelist that doesn't intersect the keys).
        cases: 256,
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
