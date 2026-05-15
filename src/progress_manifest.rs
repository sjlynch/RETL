//! Sidecar progress manifest for resumable spool runs.
//!
//! Written next to spool outputs as `<out_dir>/_progress.json`. After each
//! per-month atomic publish, an entry is recorded keyed by `<prefix>_<YYYY-MM>`
//! (e.g. `RC_2018-03`) so a re-run can skip months already completed by a
//! previous, possibly crashed, invocation.
//!
//! Entries store the on-disk size of the published file. On re-entry we
//! cross-check by stat'ing the final destination and comparing sizes — if the
//! file is missing, smaller, or larger than recorded, the entry is dropped and
//! that month is re-run. The manifest also records a fingerprint of the query,
//! output-affecting options, and (for current scan/export callers) selected
//! corpus file identities; a mismatch invalidates the checkpoint set. The
//! optional `sha256` field is reserved for v2.

use anyhow::{Context, Result};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::atomic_write::ensure_staging_dir;
use crate::atomic_write::write_jsonl_atomic;

pub const MANIFEST_FILE_NAME: &str = "_progress.json";
const MANIFEST_VERSION: u32 = 1;

#[cfg(test)]
mod test_failures {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Debug)]
    struct TestSaveFailure {
        id: u64,
        out_dir: PathBuf,
        skip_attempts: usize,
        remaining_failures: usize,
    }

    static NEXT_ID: AtomicU64 = AtomicU64::new(0);
    static SAVE_FAILURES: std::sync::Mutex<Vec<TestSaveFailure>> =
        std::sync::Mutex::new(Vec::new());

    pub(crate) struct TestSaveFailureGuard {
        id: u64,
    }

    impl Drop for TestSaveFailureGuard {
        fn drop(&mut self) {
            let mut failures = SAVE_FAILURES
                .lock()
                .expect("progress-manifest test failure mutex poisoned");
            failures.retain(|failure| failure.id != self.id);
        }
    }

    pub(crate) fn fail_saves_after_attempts_for_tests(
        out_dir: impl Into<PathBuf>,
        skip_attempts: usize,
        failures: usize,
    ) -> TestSaveFailureGuard {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        SAVE_FAILURES
            .lock()
            .expect("progress-manifest test failure mutex poisoned")
            .push(TestSaveFailure {
                id,
                out_dir: out_dir.into(),
                skip_attempts,
                remaining_failures: failures,
            });
        TestSaveFailureGuard { id }
    }

    pub(super) fn maybe_fail_save_for_tests(out_dir: &Path) -> Result<()> {
        let mut failures = SAVE_FAILURES
            .lock()
            .expect("progress-manifest test failure mutex poisoned");
        let Some(failure) = failures.iter_mut().find(|failure| {
            failure.out_dir == out_dir
                && (failure.skip_attempts > 0 || failure.remaining_failures > 0)
        }) else {
            return Ok(());
        };

        if failure.skip_attempts > 0 {
            failure.skip_attempts -= 1;
            return Ok(());
        }

        failure.remaining_failures -= 1;
        Err(anyhow::anyhow!(
            "injected progress manifest save failure for {}",
            out_dir.display()
        ))
    }
}

#[cfg(test)]
pub(crate) mod testing {
    pub(crate) use super::test_failures::fail_saves_after_attempts_for_tests;
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MonthEntry {
    pub size: u64,
    pub lines: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProgressManifest {
    pub version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
    pub months: HashMap<String, MonthEntry>,
}

impl Default for ProgressManifest {
    fn default() -> Self {
        Self {
            version: MANIFEST_VERSION,
            fingerprint: None,
            months: HashMap::new(),
        }
    }
}

/// Build the manifest path for a given output directory.
pub fn manifest_path(out_dir: &Path) -> PathBuf {
    out_dir.join(MANIFEST_FILE_NAME)
}

/// Compose the canonical month key used in the manifest, e.g. `RC_2018-03`.
pub fn month_key(prefix: &str, ym: impl std::fmt::Display) -> String {
    format!("{}_{}", prefix, ym)
}

/// Load the manifest if present. Missing file → empty manifest. A manifest
/// that fails to parse, or has an unknown version, is treated as absent
/// (warned, not fatal — we'd rather re-run than corrupt the user's data).
pub fn load(out_dir: &Path) -> ProgressManifest {
    let path = manifest_path(out_dir);
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return ProgressManifest::default(),
        Err(e) => {
            tracing::warn!(path=%path.display(), error=%e, "could not read progress manifest; treating as empty");
            return ProgressManifest::default();
        }
    };
    match serde_json::from_slice::<ProgressManifest>(&bytes) {
        Ok(m) if m.version == MANIFEST_VERSION => m,
        Ok(m) => {
            tracing::warn!(path=%path.display(), version=m.version, "progress manifest has unsupported version; ignoring");
            ProgressManifest::default()
        }
        Err(e) => {
            tracing::warn!(path=%path.display(), error=%e, "progress manifest is malformed; ignoring");
            ProgressManifest::default()
        }
    }
}

/// Atomically rewrite the manifest from the current accumulator snapshot.
/// Uses the same staging dir as the spool outputs so a crash never publishes
/// a half-written manifest.
pub fn save(
    out_dir: &Path,
    months: &HashMap<String, MonthEntry>,
    fingerprint: Option<&str>,
) -> Result<()> {
    #[cfg(test)]
    test_failures::maybe_fail_save_for_tests(out_dir)?;

    let staging_dir = ensure_staging_dir(out_dir)?;
    let dest = manifest_path(out_dir);
    let manifest = ProgressManifest {
        version: MANIFEST_VERSION,
        fingerprint: fingerprint.map(str::to_owned),
        months: months.clone(),
    };
    let bytes = serde_json::to_vec_pretty(&manifest).context("serialize progress manifest")?;
    write_jsonl_atomic(&staging_dir, &dest, 64 * 1024, |w| {
        w.write_all(&bytes)?;
        Ok(())
    })?;
    Ok(())
}

/// Shared accumulator passed into the per-month closure. Holds the manifest
/// state in-memory plus enough context to rewrite the on-disk manifest after
/// each successful per-month commit.
pub struct ManifestAccumulator {
    out_dir: PathBuf,
    fingerprint: Option<String>,
    months: Mutex<HashMap<String, MonthEntry>>,
    save_error: Mutex<Option<String>>,
}

impl ManifestAccumulator {
    pub fn new(
        out_dir: &Path,
        initial: HashMap<String, MonthEntry>,
        fingerprint: Option<String>,
    ) -> Self {
        Self {
            out_dir: out_dir.to_path_buf(),
            fingerprint,
            months: Mutex::new(initial),
            save_error: Mutex::new(None),
        }
    }

    /// Insert or update an entry for `key`, then atomically rewrite the manifest.
    ///
    /// **Durability contract:** when this returns `Ok`, the in-memory map and
    /// the on-disk `_progress.json` both contain the new entry. When this
    /// returns `Err`, the tentative insert is rolled back so the in-memory
    /// state stays in sync with the last manifest that was successfully
    /// written — a subsequent successful commit then publishes only entries
    /// whose prior saves landed. Resume-enabled callers should treat `Err` as
    /// a run-level failure: the data file may already be published, but the
    /// checkpoint is not durable. If a caller chooses to collect failures
    /// instead of failing immediately, [`Self::last_save_error`] is latched
    /// once any commit in this accumulator fails.
    ///
    /// The in-memory insert, snapshot construction, staged write, atomic
    /// rename, and rollback-on-failure all happen under the same mutex. That
    /// serializes concurrent workers with `file_concurrency > 1`, preventing
    /// lost updates and races between the rollback and other commits. The
    /// atomic writer also gives each staged `_progress.json` rewrite a unique
    /// `.inprogress` path so concurrent attempts cannot truncate each other.
    pub fn commit(&self, key: String, entry: MonthEntry) -> Result<()> {
        let mut guard = self.months.lock();
        let prev = guard.insert(key.clone(), entry);
        match save(&self.out_dir, &*guard, self.fingerprint.as_deref()) {
            Ok(()) => {
                drop(guard);
                Ok(())
            }
            Err(e) => {
                match prev {
                    Some(p) => {
                        guard.insert(key, p);
                    }
                    None => {
                        guard.remove(&key);
                    }
                }
                drop(guard);
                let mut save_error = self.save_error.lock();
                if save_error.is_none() {
                    *save_error = Some(format!("{:#}", e));
                }
                Err(e)
            }
        }
    }

    /// Returns the first manifest save error observed by this accumulator, if
    /// any. This is latched for the lifetime of the accumulator and is not
    /// cleared by later successful commits, so it answers "did any commit in
    /// this run fail to reach disk?". A new accumulator (i.e. a subsequent run)
    /// starts with no failure state.
    pub fn last_save_error(&self) -> Option<String> {
        self.save_error.lock().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::atomic_write::STAGING_DIR_NAME;

    #[test]
    fn commit_rolls_back_in_memory_state_on_save_failure() {
        let tmp = tempfile::tempdir().unwrap();
        let out_dir = tmp.path();
        // Block staging-dir creation by placing a regular file at <out>/_staging.
        // `ensure_staging_dir` will fail fast (NotADirectory / AlreadyExists is
        // non-retriable), forcing `save()` to error.
        std::fs::write(out_dir.join(STAGING_DIR_NAME), b"not a directory").unwrap();

        let acc = ManifestAccumulator::new(out_dir, HashMap::new(), None);
        let entry = MonthEntry {
            size: 100,
            lines: 50,
            sha256: None,
        };
        let result = acc.commit("RC_2024-01".to_string(), entry);

        assert!(
            result.is_err(),
            "commit should fail when staging dir is blocked"
        );
        assert!(
            acc.last_save_error().is_some(),
            "last_save_error should be populated after a failed commit",
        );
        assert!(
            acc.months.lock().is_empty(),
            "in-memory map should be empty after rollback (commit failed)",
        );
        assert!(
            !manifest_path(out_dir).exists(),
            "no manifest should have been written when save failed",
        );
    }

    #[test]
    fn save_error_is_latched_for_run_but_new_accumulator_starts_clear() {
        let tmp = tempfile::tempdir().unwrap();
        let out_dir = tmp.path();

        // First commit: blocked.
        std::fs::write(out_dir.join(STAGING_DIR_NAME), b"not a directory").unwrap();
        let acc = ManifestAccumulator::new(out_dir, HashMap::new(), None);
        let entry_a = MonthEntry {
            size: 10,
            lines: 1,
            sha256: None,
        };
        let _ = acc.commit("RC_2024-01".to_string(), entry_a);
        assert!(acc.last_save_error().is_some());

        // Unblock staging dir, then commit succeeds. The same accumulator still
        // remembers that this run had a non-durable checkpoint attempt.
        std::fs::remove_file(out_dir.join(STAGING_DIR_NAME)).unwrap();
        let entry_b = MonthEntry {
            size: 20,
            lines: 2,
            sha256: None,
        };
        acc.commit("RC_2024-02".to_string(), entry_b)
            .expect("commit should succeed");

        assert!(
            acc.last_save_error().is_some(),
            "same-run save error should remain latched after a later success",
        );
        // Only the successful entry should be present — the rolled-back one is gone.
        let map = acc.months.lock();
        assert_eq!(map.len(), 1);
        assert!(map.contains_key("RC_2024-02"));
        assert!(!map.contains_key("RC_2024-01"));
        drop(map);

        // A subsequent run constructs a fresh accumulator; successful commits
        // start from a clear failure state.
        let acc2 = ManifestAccumulator::new(out_dir, HashMap::new(), None);
        let entry_c = MonthEntry {
            size: 30,
            lines: 3,
            sha256: None,
        };
        acc2.commit("RC_2024-03".to_string(), entry_c)
            .expect("commit should succeed");
        assert!(
            acc2.last_save_error().is_none(),
            "new accumulator should not inherit the prior run's failure state",
        );
    }
}
