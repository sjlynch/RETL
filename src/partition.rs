use ahash::RandomState;
use anyhow::{Context, Result};
use parking_lot::Mutex;
use std::fs::File;
use std::hash::{BuildHasher, Hash, Hasher};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::atomic_write::{ensure_staging_dir, unique_inprogress_path};
use crate::util::replace_file_atomic_backoff;

/// Upper bound on the number of partitions a single `PartitionWriters` will
/// open at once. Each partition owns a live `BufWriter<File>` for the lifetime
/// of the writer, so an unbounded `parts` value can exhaust the process
/// file-descriptor limit (Windows stdio ~512, Linux soft `ulimit -n` ~1024)
/// and pin a `parts * write_buf` block of RAM before any data lands.
///
/// `PartitionWriters::new` silently clamps `parts` to this value. Callers
/// that want to dynamic-cap their own configuration can compare against this
/// constant directly.
pub const MAX_PARTITIONS: usize = 10_000;

/// Partitioned writers that route each user aggregate to a stable partition file.
/// Writes are user-keyed: the same `user` always goes to the same partition.
/// You provide the bytes to write via a lambda (closure) that gets a `&mut dyn Write`.
///
/// File layout:
///   `<dir>/_staging/<stem>_part_XXXXXX.ndjson.retl-<pid>-<nonce>.inprogress`
///   `<dir>/<stem>_part_XXXXXX.ndjson` (final, after finalize())
///
/// The part number is zero-padded to 6 digits so that final paths sort
/// lexicographically for any supported partition count.
///
/// Bounds on `parts`:
///  - `parts` is clamped to `1` on the low end (a zero partition count
///    collapses to a single output file).
///  - `parts` is clamped to [`MAX_PARTITIONS`] on the high end so a
///    misconfigured caller cannot exhaust the process file-descriptor limit
///    or commit hundreds of GiB of writer buffers up front.
///
/// Notes:
///  - You are responsible for writing line terminators (`\n`) inside the lambda.
///  - `write_with()` and `flush_all()` take `&self`, so a single
///    `PartitionWriters` can be shared across threads (e.g. inside a
///    `rayon::scope`). Each call only locks the partition it routes to,
///    so writes to distinct partitions proceed in parallel; concurrent
///    writes that hash to the same partition serialize on its mutex.
pub struct PartitionWriters {
    writers: Vec<parking_lot::Mutex<BufWriter<File>>>,
    tmp_paths: Vec<PathBuf>,
    final_paths: Vec<PathBuf>,
    state: RandomState,
}

impl PartitionWriters {
    /// Create `parts` writers under `dir` with the given file `stem`.
    /// Writes go into a staging directory and are atomically promoted on `finalize()`.
    ///
    /// `parts` is clamped to the inclusive range `[1, MAX_PARTITIONS]`; a
    /// value above [`MAX_PARTITIONS`] is reduced to that ceiling (with a
    /// `tracing::warn!`) rather than allowed to exhaust file descriptors
    /// or writer-buffer RAM.
    pub fn new(dir: &Path, stem: &str, parts: usize, write_buf: usize) -> Result<Self> {
        let requested = parts;
        let parts = parts.max(1).min(MAX_PARTITIONS);
        if requested > MAX_PARTITIONS {
            tracing::warn!(
                requested = requested,
                max = MAX_PARTITIONS,
                "PartitionWriters: clamping parts to MAX_PARTITIONS to bound file-descriptor and buffer use"
            );
        }
        crate::util::create_dir_all_with_default_backoff(dir)
            .with_context(|| format!("create partition dir {}", dir.display()))?;
        let staging = ensure_staging_dir(dir)?;

        let mut writers = Vec::with_capacity(parts);
        let mut tmp_paths = Vec::with_capacity(parts);
        let mut final_paths = Vec::with_capacity(parts);

        for i in 0..parts {
            let final_p = dir.join(format!("{}_part_{:06}.ndjson", stem, i));
            let tmp = unique_inprogress_path(&staging, &final_p)?;
            let f = crate::util::create_new_with_default_backoff(&tmp)
                .with_context(|| format!("create {}", tmp.display()))?;
            writers.push(Mutex::new(BufWriter::with_capacity(write_buf, f)));
            tmp_paths.push(tmp);
            final_paths.push(final_p);
        }

        // Deterministic random state for stable sharding
        let state = RandomState::with_seeds(
            0x1357_9bdf_acce_55ed,
            0x2468_ace0_fdb9_8642,
            0xfeed_face_dead_beef,
            0x0bad_f00d_c0de_cafe,
        );

        Ok(Self {
            writers,
            tmp_paths,
            final_paths,
            state,
        })
    }

    #[inline]
    fn shard_index(&self, user: &str) -> usize {
        let mut h = self.state.build_hasher();
        user.hash(&mut h);
        (h.finish() as usize) % self.writers.len()
    }

    /// Route this user to a stable partition and write bytes using the provided closure.
    /// The closure receives a `&mut dyn Write`. You must write a full NDJSON line (incl. `\n`).
    pub fn write_with<F>(&self, user: &str, f: F) -> Result<()>
    where
        F: FnOnce(&mut dyn Write) -> Result<()>,
    {
        let idx = self.shard_index(user);
        let mut guard = self.writers[idx].lock();
        let w: &mut dyn Write = &mut *guard;
        f(w)
    }

    /// Flush all partitions.
    pub fn flush_all(&self) -> Result<()> {
        for w in &self.writers {
            w.lock().flush()?;
        }
        Ok(())
    }

    /// Flush, close, and promote all `.inprogress` files to final `.ndjson`
    /// files. Returns the list of final file paths in stable partition order.
    ///
    /// ## Atomicity scope — read before relying on the return value
    ///
    /// Each partition is promoted with its own atomic
    /// [`replace_file_atomic_backoff`](crate::util::replace_file_atomic_backoff)
    /// rename, but `finalize` is **not** a transaction across the partition
    /// set: no filesystem primitive renames N files as one unit. Partitions
    /// are promoted in index order.
    ///
    /// If a mid-loop rename fails (a transient error outlives the backoff
    /// budget, or a destination is locked), `finalize` does **not** silently
    /// leave a partial `*_part_*.ndjson` set on disk:
    ///
    /// 1. It rolls every already-promoted partition back into staging with a
    ///    best-effort reverse rename, so the common outcome of a failure is
    ///    still "all partitions published" or "none published".
    /// 2. It returns `Err` wrapping a [`PartitionFinalizeError`] that names
    ///    exactly which partitions are published vs. still staged, and whether
    ///    the rollback fully succeeded.
    ///
    /// A caller that gets an `Err` can therefore tell a cleanly rolled-back
    /// run (retryable, nothing on disk) apart from a genuinely partial export,
    /// instead of enumerating `*_part_*.ndjson` and mistaking a partial set
    /// for a complete one. On `Ok`, every partition is published and the
    /// returned paths are safe to enumerate as a complete export.
    pub fn finalize(mut self) -> Result<Vec<PathBuf>> {
        self.flush_all()?;
        // Ensure files are closed before rename/copy
        let writers = std::mem::take(&mut self.writers);
        drop(writers);

        let tmp_paths = std::mem::take(&mut self.tmp_paths);
        let final_paths = std::mem::take(&mut self.final_paths);

        // Promote each partition in index order, tracking how many reached
        // their final path so a mid-loop failure can be reported — and rolled
        // back — precisely.
        let mut promoted = 0usize;
        let mut promote_err: Option<anyhow::Error> = None;
        for (tmp, final_p) in tmp_paths.iter().zip(final_paths.iter()) {
            match replace_file_atomic_backoff(tmp, final_p) {
                Ok(()) => promoted += 1,
                Err(e) => {
                    promote_err = Some(e);
                    break;
                }
            }
        }

        let Some(source) = promote_err else {
            return Ok(final_paths);
        };
        // `promoted` is now the index of the partition that could not be
        // promoted: partitions [0, promoted) are published, the rest staged.
        let failed_index = promoted;

        // `at_final[i]` tracks whether partition `i`'s data currently sits at
        // its final path. Roll the already-published partitions back into
        // staging so the output directory does not present a partial
        // `*_part_*.ndjson` set that a reader could mistake for a complete
        // export. Each reverse rename is itself atomic; a partition whose
        // rollback fails simply stays published (recorded as a partial state).
        let parts = final_paths.len();
        let mut at_final = vec![false; parts];
        for slot in at_final.iter_mut().take(promoted) {
            *slot = true;
        }
        for i in 0..promoted {
            match replace_file_atomic_backoff(&final_paths[i], &tmp_paths[i]) {
                Ok(()) => at_final[i] = false,
                Err(rollback_err) => {
                    tracing::warn!(
                        partition = i,
                        final_path = %final_paths[i].display(),
                        error = %rollback_err,
                        "PartitionWriters::finalize: could not roll a published \
                         partition back to staging after a mid-loop promote failure; \
                         output directory holds a partial export"
                    );
                }
            }
        }

        let published: Vec<PathBuf> = (0..parts)
            .filter(|&i| at_final[i])
            .map(|i| final_paths[i].clone())
            .collect();
        let pending: Vec<PathBuf> = (0..parts)
            .filter(|&i| !at_final[i])
            .map(|i| tmp_paths[i].clone())
            .collect();
        let rolled_back = published.is_empty();

        Err(anyhow::Error::new(PartitionFinalizeError {
            published,
            pending,
            failed_index,
            rolled_back,
            source,
        }))
    }
}

/// Error returned by [`PartitionWriters::finalize`] when the staged partition
/// files could not all be promoted to their final paths.
///
/// `finalize` promotes partitions one at a time — it is *not* atomic across
/// the partition set (see [`PartitionWriters::finalize`]). When a promote
/// fails partway through, `finalize` rolls the already-promoted partitions
/// back into staging and returns this error so the caller can tell a cleanly
/// rolled-back run apart from a genuinely partial export, instead of mistaking
/// a partial `*_part_*.ndjson` set for a complete one.
#[derive(Debug)]
pub struct PartitionFinalizeError {
    /// Final paths of partitions still published at their final location.
    /// Empty when the rollback fully succeeded ([`Self::rolled_back`] is
    /// `true`); non-empty only when rolling one or more partitions back
    /// failed, in which case the output directory holds a *partial* export
    /// and these are the files a reader must not treat as complete.
    pub published: Vec<PathBuf>,
    /// Staging (`.inprogress`) paths of partitions not currently published —
    /// the partitions never promoted plus any that were rolled back into
    /// staging.
    pub pending: Vec<PathBuf>,
    /// Index of the partition whose promotion first failed.
    pub failed_index: usize,
    /// `true` when every previously promoted partition was rolled back into
    /// staging, so the output directory holds *none* of this export's final
    /// files — a clean, retryable "nothing published" state. `false` means at
    /// least one partition could not be rolled back and a partial export
    /// remains on disk (see [`Self::published`]).
    pub rolled_back: bool,
    /// The underlying rename failure that aborted promotion.
    source: anyhow::Error,
}

impl PartitionFinalizeError {
    /// Total number of partitions the writer was finalizing.
    pub fn total_partitions(&self) -> usize {
        self.published.len() + self.pending.len()
    }

    /// Whether a partial export remains on disk — some partitions published
    /// while others are not. Always `false` after a fully successful
    /// rollback; equivalent to `!self.rolled_back`.
    pub fn is_partial_export(&self) -> bool {
        !self.published.is_empty()
    }
}

impl std::fmt::Display for PartitionFinalizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PartitionWriters::finalize: promoting partition {} of {} failed; \
             {} partition(s) published, {} staged in _staging/. {}",
            self.failed_index,
            self.total_partitions(),
            self.published.len(),
            self.pending.len(),
            if self.rolled_back {
                "All promoted partitions were rolled back to staging — no partial \
                 export remains on disk and finalize can be safely retried."
            } else {
                "Rolling promoted partitions back to staging did not fully succeed — \
                 a partial export remains on disk; see `published` for the partition \
                 files a reader must not treat as a complete export."
            },
        )
    }
}

impl std::error::Error for PartitionFinalizeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A mid-loop rename failure must not silently leave a partial
    /// `*_part_*.ndjson` set. With a blocked destination, `finalize` rolls the
    /// already-promoted partitions back into staging and returns a
    /// `PartitionFinalizeError` naming the published/pending split.
    #[test]
    fn finalize_mid_loop_failure_rolls_back_and_reports_partition_set() {
        let dir = tempfile::tempdir().unwrap();
        let pw = PartitionWriters::new(dir.path(), "blocked", 3, 64 * 1024).unwrap();
        for user in ["alice", "bob", "carol", "dave", "erin", "frank"] {
            pw.write_with(user, |w| {
                w.write_all(format!("{{\"u\":\"{user}\"}}\n").as_bytes())?;
                Ok(())
            })
            .unwrap();
        }

        // Block partition 1's final path with a directory so its promote
        // rename fails after partition 0 has already been published.
        let blocker = dir.path().join("blocked_part_000001.ndjson");
        std::fs::create_dir(&blocker).unwrap();

        // Cap the retry budget so the blocked rename surfaces immediately
        // instead of burning the full production ~10–20 s backoff.
        let _cap = crate::util::cap_backoff_budget_for_test(1, 0);

        let err = pw
            .finalize()
            .expect_err("finalize must fail when a partition cannot be promoted");
        let fe = err
            .downcast_ref::<PartitionFinalizeError>()
            .expect("error must carry a PartitionFinalizeError");

        assert_eq!(fe.failed_index, 1, "partition 1 is the blocked one");
        assert_eq!(fe.total_partitions(), 3);
        assert!(
            fe.rolled_back,
            "partition 0 should roll back cleanly into staging"
        );
        assert!(!fe.is_partial_export());
        assert!(
            fe.published.is_empty(),
            "rollback should leave nothing published, got {:?}",
            fe.published
        );
        assert_eq!(
            fe.pending.len(),
            3,
            "every partition should be reported as staged after rollback"
        );

        // No final `*_part_*.ndjson` file may remain — the blocker directory
        // is the only `blocked_part_*` entry, and partition 0 was rolled back.
        assert!(
            !dir.path().join("blocked_part_000000.ndjson").exists(),
            "partition 0 must not stay published after rollback"
        );
        // Every reported pending file is a real staged file ready for retry.
        for staged in &fe.pending {
            assert!(staged.exists(), "staged file should exist: {staged:?}");
        }
    }
}
