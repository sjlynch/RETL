use ahash::RandomState;
use anyhow::{Context, Result};
use parking_lot::Mutex;
use std::fs::{self, File};
use std::hash::{BuildHasher, Hash, Hasher};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::util::{create_with_backoff, replace_file_atomic_backoff};

/// Partitioned writers that route each user aggregate to a stable partition file.
/// Writes are user-keyed: the same `user` always goes to the same partition.
/// You provide the bytes to write via a lambda (closure) that gets a `&mut dyn Write`.
///
/// File layout:
///   <dir>/_staging/<stem>_part_XXXX.inprogress  (temp)
///   <dir>/<stem>_part_XXXX.ndjson               (final, after finalize())
///
/// Notes:
///  - You are responsible for writing line terminators (`\n`) inside the lambda.
///  - `write_with()` is concurrency-friendly (internal per-part mutex).
pub struct PartitionWriters {
    writers: Vec<parking_lot::Mutex<BufWriter<File>>>,
    tmp_paths: Vec<PathBuf>,
    final_paths: Vec<PathBuf>,
    state: RandomState,
}

impl PartitionWriters {
    /// Create `parts` writers under `dir` with the given file `stem`.
    /// Writes go into a staging directory and are atomically promoted on `finalize()`.
    pub fn new(dir: &Path, stem: &str, parts: usize, write_buf: usize) -> Result<Self> {
        let parts = parts.max(1);
        let staging = dir.join("_staging");
        fs::create_dir_all(&staging)?;
        fs::create_dir_all(dir)?;

        let mut writers = Vec::with_capacity(parts);
        let mut tmp_paths = Vec::with_capacity(parts);
        let mut final_paths = Vec::with_capacity(parts);

        for i in 0..parts {
            let tmp = staging.join(format!("{}_part_{:04}.inprogress", stem, i));
            let final_p = dir.join(format!("{}_part_{:04}.ndjson", stem, i));
            let f = create_with_backoff(&tmp, 16, 50)
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

        Ok(Self { writers, tmp_paths, final_paths, state })
    }

    #[inline]
    fn shard_index(&self, user: &str) -> usize {
        let mut h = self.state.build_hasher();
        user.hash(&mut h);
        (h.finish() as usize) % self.writers.len()
    }

    /// Route this user to a stable partition and write bytes using the provided closure.
    /// The closure receives a `&mut dyn Write`. You must write a full NDJSON line (incl. `\n`).
    pub fn write_with<F>(&mut self, user: &str, f: F) -> Result<()>
    where
        F: FnOnce(&mut dyn Write) -> Result<()>,
    {
        let idx = self.shard_index(user);
        let mut guard = self.writers[idx].lock();
        let w: &mut dyn Write = &mut *guard;
        f(w)
    }

    /// Flush all partitions.
    pub fn flush_all(&mut self) -> Result<()> {
        for w in &mut self.writers {
            w.lock().flush()?;
        }
        Ok(())
    }

    /// Flush, close, and promote all `.inprogress` files to final `.ndjson` files atomically.
    /// Returns the list of final file paths in stable order.
    pub fn finalize(mut self) -> Result<Vec<PathBuf>> {
        self.flush_all()?;
        // Ensure files are closed before rename/copy
        let writers = std::mem::take(&mut self.writers);
        drop(writers);

        let tmp_paths = self.tmp_paths;
        let final_paths = self.final_paths;

        for (tmp, final_p) in tmp_paths.iter().zip(final_paths.iter()) {
            replace_file_atomic_backoff(tmp, final_p)?;
        }

        Ok(final_paths)
    }
}
