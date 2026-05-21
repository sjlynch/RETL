use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static SCRATCH_COUNTER: AtomicU64 = AtomicU64::new(0);

/// RAII guard that reclaims scratch paths when it goes out of scope.
///
/// The reduce/parents stages stage shard and run files under per-run scratch
/// paths. That scratch must be removed on **every** exit path — a normal
/// return, an early `return Err(...)`, *and* an unwinding panic — or repeated
/// runs silently accrete orphaned `run_*` directories and shard files under a
/// shared parent. Hanging cleanup off an `if outcome.is_ok()` check misses the
/// panic case entirely: the panic unwinds straight past it.
///
/// A guard is **armed** on construction. Its [`Drop`] removes each guarded
/// path best-effort — a directory tree via `remove_dir_all`, anything else via
/// `remove_file` — logging (never failing) on a cleanup hiccup. Call
/// [`ScratchGuard::disarm`] for the single intentional case where leftover
/// scratch is wanted, e.g. leaving a *failed* aggregate's shard directory in
/// place for post-mortem inspection. `disarm` is never reached on a panic, so
/// panic-unwind always cleans up.
pub(crate) struct ScratchGuard {
    paths: Vec<PathBuf>,
    armed: bool,
}

impl ScratchGuard {
    /// Arm a guard over a single scratch path (file or directory).
    pub(crate) fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            paths: vec![path.into()],
            armed: true,
        }
    }

    /// Arm a guard over a set of scratch paths (files or directories).
    pub(crate) fn for_paths(paths: Vec<PathBuf>) -> Self {
        Self { paths, armed: true }
    }

    /// Add another scratch path to remove on drop.
    pub(crate) fn guard_also(&mut self, path: impl Into<PathBuf>) {
        self.paths.push(path.into());
    }

    /// Cancel removal: the guarded paths are left in place when the guard
    /// drops. Use this only for the deliberate post-mortem case.
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ScratchGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        for path in &self.paths {
            let outcome = if path.is_dir() {
                crate::util::remove_dir_all_with_short_backoff(path)
            } else {
                // `remove_*_with_short_backoff` both treat `NotFound` as
                // success, so a never-created or already-removed path is a
                // no-op here.
                crate::util::remove_with_short_backoff(path)
            };
            if let Err(e) = outcome {
                tracing::warn!(
                    scratch_path = %path.display(),
                    error = %e,
                    "failed to remove scratch path; leftover scratch may accrete"
                );
            }
        }
    }
}

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
