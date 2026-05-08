//! Tests for the cooperative memory throttle in `src/mem.rs`.
//!
//! `mem`'s sysinfo reading is process-globally cached, so the throttle paths
//! in `dedupe`/`bucketing`/`zstd_jsonl` were previously untestable: there was
//! no way to inject a "low-memory" reading without actually starving the host.
//! `set_available_memory_fraction_for_tests` (gated behind the `test-utils`
//! feature) overrides the cached fraction AND stamps `LAST_CHECK_MS` to "now"
//! so the next read uses the override instead of clobbering it with a fresh
//! sysinfo refresh.
//!
//! Run with: `cargo test --features test-utils`
//!
//! These tests use `serial_test::serial` because they mutate
//! process-global state and would race other tests in the same binary.

use std::time::{Duration, Instant};

use retl::{
    is_low_memory, maybe_throttle_low_memory, set_available_memory_fraction_for_tests,
};
use serial_test::serial;

#[test]
#[serial]
fn injected_low_fraction_makes_is_low_memory_true() {
    // Inject a 5% available reading; threshold of 20% must report "low".
    set_available_memory_fraction_for_tests(0.05);
    assert!(
        is_low_memory(0.20),
        "is_low_memory(0.20) must be true after injecting fraction=0.05"
    );

    // Restore "plenty" so subsequent tests in the same process aren't poisoned.
    set_available_memory_fraction_for_tests(1.0);
    assert!(
        !is_low_memory(0.20),
        "is_low_memory(0.20) must be false after restoring fraction=1.0"
    );
}

#[test]
#[serial]
fn maybe_throttle_low_memory_actually_sleeps_when_low() {
    // Baseline: with plenty of memory, the throttle is a near-instant no-op.
    set_available_memory_fraction_for_tests(1.0);
    let t0 = Instant::now();
    maybe_throttle_low_memory(0.20);
    let no_op_elapsed = t0.elapsed();
    assert!(
        no_op_elapsed < Duration::from_millis(10),
        "throttle should be a no-op when memory is plentiful, took {:?}",
        no_op_elapsed
    );

    // Inject low memory; throttle must actually sleep (~25ms in the impl).
    set_available_memory_fraction_for_tests(0.05);
    let t0 = Instant::now();
    maybe_throttle_low_memory(0.20);
    let throttled_elapsed = t0.elapsed();
    assert!(
        throttled_elapsed >= Duration::from_millis(20),
        "throttle should sleep when memory is low, took {:?}",
        throttled_elapsed
    );

    // Reset so the global cache doesn't leak into other tests/binaries.
    set_available_memory_fraction_for_tests(1.0);
}
