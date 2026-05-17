//! Crate-internal utilities split by concern. The submodules are private
//! implementation detail; everything that callers reach for via
//! `crate::util::*` is re-exported here so the public/internal paths stay
//! stable across the split.
//!
//! Submodule map:
//! - [`backoff`] — Windows-friendly `*_with_backoff` I/O retries and
//!   `replace_file_atomic_backoff`.
//! - [`exclusions`] — default bot author list and env/file merging.
//! - [`scratch`] — process-unique scratch directory naming.
//! - [`thread_pool`] — `with_thread_pool` scoped Rayon helper.
//! - [`tracing`] — binary-only tracing subscriber init.

mod backoff;
mod exclusions;
mod scratch;
mod thread_pool;
mod tracing;

pub use backoff::{
    create_dir_all_with_backoff, create_dir_all_with_default_backoff, create_new_with_backoff,
    create_new_with_default_backoff, create_with_backoff, create_with_default_backoff,
    open_with_backoff, open_with_default_backoff, read_dir_with_backoff,
    read_dir_with_default_backoff, remove_dir_all_with_backoff,
    remove_dir_all_with_default_backoff, remove_dir_all_with_short_backoff, remove_with_backoff,
    remove_with_default_backoff, remove_with_short_backoff, replace_file_atomic_backoff,
    DEFAULT_BACKOFF_DELAY_MS, DEFAULT_BACKOFF_TRIES, SHORT_BACKOFF_TRIES,
};
// `create_dir_with_backoff` has no current caller, but the original `util.rs`
// exposed it as `pub(crate)` alongside its `_with_default_backoff` wrapper;
// keep the re-export so the symbol's visibility is preserved across the split.
#[allow(unused_imports)]
pub(crate) use backoff::create_dir_with_backoff;
pub(crate) use backoff::create_dir_with_default_backoff;
// `TestIoFailureGuard` is returned by the injectors below and only ever bound
// as `_guard` by callers; the re-export is required so its type stays nameable
// at `crate::util::TestIoFailureGuard`.
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use backoff::TestIoFailureGuard;
#[cfg(test)]
pub(crate) use backoff::{
    inject_retriable_io_errors_for_file_name_tests, inject_retriable_io_errors_for_tests, TestIoOp,
};

pub use exclusions::default_bot_authors;
pub(crate) use exclusions::try_merge_extra_exclusions;

pub(crate) use scratch::unique_scratch_dir;

pub use thread_pool::with_thread_pool;

pub use tracing::init_tracing_for_binary;

// -------- Grab-bag helpers with no clearer home --------

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const FNV1A_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV1A_PRIME: u64 = 0x00000100000001B3;

pub(crate) fn fnv1a_offset_basis() -> u64 {
    FNV1A_OFFSET_BASIS
}

pub(crate) fn fnv1a_update(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(FNV1A_PRIME);
    }
}

pub(crate) fn stable_fnv1a_hex(bytes: &[u8]) -> String {
    let mut hash = FNV1A_OFFSET_BASIS;
    fnv1a_update(&mut hash, bytes);
    format!("fnv1a64:{hash:016x}")
}

pub(crate) fn system_time_parts(t: SystemTime) -> (i64, u32) {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => (
            i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
            d.subsec_nanos(),
        ),
        Err(e) => {
            let d = e.duration();
            (
                -i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
                d.subsec_nanos(),
            )
        }
    }
}

pub fn output_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

/// Smoothstep curve mapping a measured free-memory fraction onto `[0.0, 1.0]`,
/// used by adaptive buffer sizing in dedupe and bucketing.
///
/// - At or below `soft_low`, returns `0.0` (favor smaller buffers).
/// - At or above `high`, returns `1.0` (favor larger buffers).
/// - Between the two, applies the cubic smoothstep `s*s*(3 - 2*s)` to soften
///   the transition.
///
/// `(high - soft_low)` is floored at `0.05` to keep the divisor non-degenerate
/// when callers are configured with overlapping thresholds.
pub(crate) fn smoothstep_memory_fraction(free: f64, soft_low: f64, high: f64) -> f64 {
    let span = (high - soft_low).max(0.05);
    let scale = ((free - soft_low) / span).clamp(0.0, 1.0);
    scale * scale * (3.0 - 2.0 * scale)
}
