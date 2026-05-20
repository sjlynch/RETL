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
//! 3. For inputs that parse as a top-level JSON object, the byte-rewrite
//!    output must equal the slow path — `apply_human_timestamps` applied to
//!    the parsed Value. The two paths are run side-by-side in production;
//!    semantic divergence on that shape is a regression to file separately.
//!
//! The object guard restricts the side-by-side check to the production
//! payload shape — a top-level JSON object. Both the byte rewriter and
//! `apply_human_timestamps` rewrite only the *top-level* object's timestamp
//! keys, leaving nested ones (e.g. inside `crosspost_parent_list`) untouched,
//! so the two paths must agree on every JSON object, shallow or deeply
//! nested. Non-object top-level values (a bare array or scalar) are never fed
//! through this rewriter in production, so they are not asserted.

use libfuzzer_sys::fuzz_target;
use serde_json::Value;

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

    // Contract 3: on any top-level JSON object — nested values included —
    // the byte path must match the slow path exactly.
    if input_val.is_object() {
        let mut slow = input_val;
        retl::apply_human_timestamps(&mut slow);
        assert_eq!(
            rewritten_val, slow,
            "byte-rewrite diverges from apply_human_timestamps on object\n\
             input:     {line}\n\
             rewritten: {buf}"
        );
    }
});
