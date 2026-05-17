use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static SCRATCH_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Return a process-unique scratch directory path under `work_dir`.
///
/// The directory name carries the logical `prefix`, scratch `kind`, PID,
/// process-local counter, and current nanoseconds so separate `retl` processes
/// sharing the same work dir do not reuse fixed shard roots.
pub(crate) fn unique_scratch_dir(work_dir: &Path, prefix: &str, kind: &str) -> PathBuf {
    let counter = SCRATCH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    work_dir.join(format!(
        "{prefix}_{kind}_{}_{}_{}",
        std::process::id(),
        counter,
        nanos
    ))
}
