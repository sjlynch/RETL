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
//! that month is re-run. The manifest also records a fingerprint of the query
//! and output-affecting options; a mismatch invalidates the checkpoint set.
//! The optional `sha256` field is reserved for v2.

use anyhow::{Context, Result};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::atomic_write::ensure_staging_dir;
use crate::atomic_write::write_jsonl_atomic;

pub const MANIFEST_FILE_NAME: &str = "_progress.json";
const MANIFEST_VERSION: u32 = 1;

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
    last_save_error: Mutex<Option<String>>,
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
            last_save_error: Mutex::new(None),
        }
    }

    /// Insert or update an entry for `key`, then atomically rewrite the manifest.
    ///
    /// **Durability contract:** when this returns `Ok`, the in-memory map and
    /// the on-disk `_progress.json` both contain the new entry. When this
    /// returns `Err`, the tentative insert is rolled back so the in-memory
    /// state stays in sync with the last manifest that was successfully
    /// written — a subsequent successful commit then publishes only entries
    /// whose prior saves landed. Callers that warn-and-continue on commit
    /// errors (e.g. the spool pipeline) can poll [`Self::last_save_error`] to
    /// query "is the manifest durable up to here?" at end-of-run.
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
                *self.last_save_error.lock() = None;
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
                *self.last_save_error.lock() = Some(format!("{:#}", e));
                Err(e)
            }
        }
    }

    /// Returns the error message from the most recent failed [`Self::commit`],
    /// if any. Cleared on the next successful commit. Use this to query
    /// whether all prior `commit()` calls landed durably — callers that
    /// only `warn!` on commit errors can surface unflushed state at end-of-run.
    pub fn last_save_error(&self) -> Option<String> {
        self.last_save_error.lock().clone()
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

        assert!(result.is_err(), "commit should fail when staging dir is blocked");
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
    fn commit_clears_last_save_error_on_success() {
        let tmp = tempfile::tempdir().unwrap();
        let out_dir = tmp.path();

        // First commit: blocked.
        std::fs::write(out_dir.join(STAGING_DIR_NAME), b"not a directory").unwrap();
        let acc = ManifestAccumulator::new(out_dir, HashMap::new(), None);
        let entry_a = MonthEntry { size: 10, lines: 1, sha256: None };
        let _ = acc.commit("RC_2024-01".to_string(), entry_a);
        assert!(acc.last_save_error().is_some());

        // Unblock staging dir, then commit succeeds.
        std::fs::remove_file(out_dir.join(STAGING_DIR_NAME)).unwrap();
        let entry_b = MonthEntry { size: 20, lines: 2, sha256: None };
        acc.commit("RC_2024-02".to_string(), entry_b).expect("commit should succeed");

        assert!(
            acc.last_save_error().is_none(),
            "last_save_error should clear on a successful commit",
        );
        // Only the successful entry should be present — the rolled-back one is gone.
        let map = acc.months.lock();
        assert_eq!(map.len(), 1);
        assert!(map.contains_key("RC_2024-02"));
        assert!(!map.contains_key("RC_2024-01"));
    }
}
