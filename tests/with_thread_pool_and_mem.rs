//! Direct tests for the publicly-exported helpers `with_thread_pool` and the
//! memory introspection in `mem` (`available_memory_fraction` /
//! `is_low_memory`). The pipeline uses these but never asserts their basic
//! invariants directly.

use rayon::prelude::*;
use retl::{available_memory_fraction, is_low_memory, with_thread_pool};
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn with_thread_pool_runs_closure_and_returns_value() {
    // A scoped pool of 2 threads still computes the right value.
    let n = with_thread_pool(Some(2), || {
        (0..1000u64).into_par_iter().sum::<u64>()
    });
    assert_eq!(n, 499_500);
}

#[test]
fn with_thread_pool_zero_falls_back_to_global_pool() {
    // Some(0) is documented as "global default pool". Closure must still run.
    let n = with_thread_pool(Some(0), || {
        (1..=100u64).into_par_iter().sum::<u64>()
    });
    assert_eq!(n, 5050);
}

#[test]
fn with_thread_pool_none_runs_on_global_pool() {
    let n = with_thread_pool(None, || {
        (0..50u64).into_par_iter().map(|x| x * 2).sum::<u64>()
    });
    let expected: u64 = (0..50u64).map(|x| x * 2).sum();
    assert_eq!(n, expected);
}

#[test]
fn with_thread_pool_distributes_work_across_workers_when_n_set() {
    // We can't pin specific threads from the test, but with a bounded pool of
    // 4 threads, an embarrassingly-parallel iteration should get done.
    let counter = AtomicUsize::new(0);
    with_thread_pool(Some(4), || {
        (0..200).into_par_iter().for_each(|_| {
            counter.fetch_add(1, Ordering::Relaxed);
        });
    });
    assert_eq!(counter.load(Ordering::Relaxed), 200);
}

#[test]
fn available_memory_fraction_is_in_unit_interval() {
    let f = available_memory_fraction();
    assert!(
        (0.0..=1.0).contains(&f),
        "available_memory_fraction must be in [0,1], got {}",
        f
    );
}

#[test]
fn is_low_memory_is_false_at_threshold_zero() {
    // With threshold 0.0 nothing can be "below" it (since fraction >= 0.0
    // is the actual range — strict less-than rules out equality).
    assert!(
        !is_low_memory(0.0),
        "is_low_memory(0.0) must be false (no fraction is strictly < 0.0)"
    );
}

#[test]
fn is_low_memory_is_true_above_one() {
    // Fraction is clamped to [0,1], so any threshold > 1.0 must report true.
    assert!(
        is_low_memory(1.1),
        "is_low_memory(1.1) must be true (fraction <= 1.0 < 1.1)"
    );
}
