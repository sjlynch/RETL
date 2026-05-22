pub use crate::username_stream::UsernameStream;

use crate::config::clamp_shard_count;
use crate::shard_common;
use crate::util::unique_scratch_dir;
use ahash::RandomState;
use anyhow::{Context, Result};
use rayon::prelude::*;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Disk-backed sharded dedup writer: concurrent-safe.
pub struct ShardedWriter {
    run_root: PathBuf,
    base_dir: PathBuf,
    shards: shard_common::LineShardWriters,
    count: usize,
    state: RandomState, // seeded for deterministic sharding
}

impl ShardedWriter {
    pub fn create(work_dir: &Path, prefix: &str, count: usize) -> Result<Self> {
        let count = clamp_shard_count(count, "ShardedWriter::create");
        let run_root = unique_scratch_dir(work_dir, prefix, "shards");
        let shards_dir = run_root.join("shards");
        crate::util::create_dir_all_with_default_backoff(&shards_dir)
            .with_context(|| format!("create shard scratch dir {}", shards_dir.display()))?;

        let shards = shard_common::create_line_shard_writers(
            &shards_dir,
            count,
            |i| format!("shard_{i:04}.tmp"),
            "shard scratch",
        )?;

        let state = shard_common::seeded_state("usernames");

        Ok(Self {
            run_root,
            base_dir: shards_dir,
            shards,
            count,
            state,
        })
    }

    pub fn scratch_root(&self) -> &Path {
        &self.run_root
    }

    #[inline]
    fn shard_index(&self, s: &str) -> usize {
        shard_common::shard_index(&self.state, s, self.count)
    }

    pub fn write(&self, key: &str) -> Result<()> {
        let idx = self.shard_index(key);
        let mut guard = self.shards[idx].lock();
        guard.write_all(key.as_bytes())?;
        guard.write_all(b"\n")?;
        Ok(())
    }

    pub fn flush_all(&self) -> Result<()> {
        shard_common::flush_line_shard_writers(&self.shards)
    }

    /// Deduplicate each shard independently and return ordered deduped files.
    pub fn dedup(self, prefix: &str) -> Result<Vec<PathBuf>> {
        let (deduped, _scratch_root) = self.dedup_with_scratch(prefix)?;
        Ok(deduped)
    }

    /// Deduplicate each shard independently, returning the files plus the
    /// per-run scratch root that owns them. Callers that hand the files to a
    /// lazy consumer should keep this root alive until that consumer is done.
    pub fn dedup_with_scratch(self, prefix: &str) -> Result<(Vec<PathBuf>, PathBuf)> {
        self.flush_all()?;
        let ShardedWriter {
            run_root,
            base_dir,
            shards,
            count,
            state: _,
        } = self;
        drop(shards); // ensure writers are closed

        let dedup_dir = run_root.join(format!("{prefix}_dedup"));
        crate::util::create_dir_all_with_default_backoff(&dedup_dir)
            .with_context(|| format!("create dedup scratch dir {}", dedup_dir.display()))?;

        let shard_paths: Vec<PathBuf> = (0..count)
            .map(|i| base_dir.join(format!("shard_{:04}.tmp", i)))
            .collect();

        let deduped: Vec<PathBuf> = shard_paths
            .par_iter()
            .map(|shard_path| -> Result<PathBuf> {
                let out_path = dedup_dir.join(
                    shard_path
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .replace(".tmp", ".txt"),
                );
                // `sort = true`: `ShardedWriter` shards are consumed by
                // sorted-merge downstream, so the deduped output is sorted.
                // Shares `shard_common::dedup_line_shard` with the parent-id
                // dedupe stage (which passes `sort = false`).
                shard_common::dedup_line_shard(shard_path, &out_path, true)?;
                Ok(out_path)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok((deduped, run_root))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::{inject_retriable_io_errors_for_file_name_tests, open_with_backoff, TestIoOp};
    use std::io::Read;

    #[test]
    fn sharded_writer_retries_transient_create_on_shard_file() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let _guard =
            inject_retriable_io_errors_for_file_name_tests(TestIoOp::Create, "shard_0000.tmp", 1);

        let writer = ShardedWriter::create(tmp.path(), "retry", 1).expect("create sharded writer");
        writer.write("alice").expect("write key");
        writer.write("alice").expect("write duplicate key");
        writer.write("bob").expect("write key");
        let (deduped, _scratch_root) = writer.dedup_with_scratch("out").expect("dedup shards");

        assert_eq!(deduped.len(), 1);
        let mut contents = String::new();
        open_with_backoff(&deduped[0], 2, 0)
            .expect("open deduped shard")
            .read_to_string(&mut contents)
            .expect("read deduped shard");
        assert_eq!(contents, "alice\nbob\n");
    }

    #[test]
    fn sharded_writer_clamps_huge_shard_count() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let writer = ShardedWriter::create(tmp.path(), "huge", usize::MAX)
            .expect("huge shard count should be clamped before allocation");
        assert_eq!(writer.count, crate::config::MAX_SHARDS);
        assert_eq!(writer.shards.len(), crate::config::MAX_SHARDS);
    }
}
