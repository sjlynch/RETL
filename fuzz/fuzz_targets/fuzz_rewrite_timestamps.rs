#![no_main]
//! Fuzz target for `retl::rewrite_human_timestamps_bytes`.
//!
//! The byte-level rewriter scans a JSONL line for the literal byte sequences
//! `"created_utc":`, `"retrieved_on":`, `"edited":` and replaces an integer
//! value in place with an RFC3339 timestamp string. It is a hand-rolled byte
//! scanner with several edge cases (negative numbers, fractional rejection,
//! key-overlap, escaped quotes inside string values) that are easy to get
//! subtly wrong.
//!
//! This target validates three contracts:
//!
//! 1. It does not panic on any input.
//! 2. The output is valid UTF-8 (enforced by the `String` return type) AND,
//!    if the input was valid JSON, the output is also valid JSON. A regression
//!    that emits truncated/garbled JSON for a parsable input is a finding.
//! 3. For inputs that parse as a "shallow" top-level JSON object (no nested
//!    objects/arrays in the values), the byte-rewrite output must equal the
//!    slow path — `apply_human_timestamps` applied to the parsed Value. The
//!    two paths are run side-by-side in production; semantic divergence on
//!    that shape is a regression to file separately.
//!
//! The shallow-object guard is deliberate: `apply_human_timestamps` only
//! touches the top-level object, while the byte rewriter walks every byte
//! and would (correctly, per its docs) rewrite nested-object timestamp keys
//! too. We do not assert equivalence on shapes where the two paths are
//! known to diverge by design — only on the flat Reddit-line shape that
//! matches the production payload.

use libfuzzer_sys::fuzz_target;
use serde_json::Value;

/// True when `v` is a JSON object whose direct children are all scalars
/// (no nested objects or arrays). Mirrors the production Reddit-line shape.
fn is_shallow_object(v: &Value) -> bool {
    match v {
        Value::Object(map) => map
            .values()
            .all(|x| !matches!(x, Value::Object(_) | Value::Array(_))),
        _ => false,
    }
}

fuzz_target!(|data: &[u8]| {
    // Production calls the rewriter on a `&str` (a decoded JSONL line), so
    // restrict to valid UTF-8 to mirror the real entry point.
    let Ok(line) = std::str::from_utf8(data) else { return };

    // Contract 1: must not panic on arbitrary UTF-8 input.
    let mut buf = String::new();
    retl::rewrite_human_timestamps_bytes(line, &mut buf);

    // Contract 2: if input parses as JSON, the rewritten output must too.
    let Ok(input_val) = serde_json::from_str::<Value>(line) else {
        // Non-JSON input — we already proved no-panic above; nothing more to check.
        return;
    };
    let rewritten_val: Value = serde_json::from_str(&buf)
        .expect("byte-rewrite of a valid-JSON line must itself be valid JSON");

    // Contract 3: on the shallow-object shape used in production, the byte
    // path must match the slow path exactly.
    if is_shallow_object(&input_val) {
        let mut slow = input_val;
        retl::apply_human_timestamps(&mut slow);
        assert_eq!(
            rewritten_val, slow,
            "byte-rewrite diverges from apply_human_timestamps on shallow object\n\
             input:     {line}\n\
             rewritten: {buf}"
        );
    }
});
