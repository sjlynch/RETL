#![no_main]
//! Fuzz target for `retl::parse_minimal`.
//!
//! `parse_minimal` is a thin wrapper around `serde_json::from_str` that maps
//! a JSON line into the `MinimalRecord` struct. It is the first byte-handling
//! step every Reddit JSONL line passes through, so panics or aborts here are
//! treated as findings.
//!
//! Contract under fuzz: the function MUST always return — `Ok(_)` for valid
//! Reddit-shaped lines, `Err(_)` for everything else. A panic, abort, or any
//! non-`Result` exit is a bug to file separately.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // `parse_minimal` is `&str`-typed, so feed it only valid UTF-8. Mirrors how
    // the production reader hands the function lines from a `BufRead::lines()`
    // pipeline (which itself yields `String`).
    if let Ok(s) = std::str::from_utf8(data) {
        // Result is intentionally discarded: we care about panic/abort behavior,
        // not the parse outcome. `Err(_)` is a perfectly valid result.
        let _ = retl::parse_minimal(s);
    }
});
