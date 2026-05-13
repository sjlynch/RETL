pub use crate::username_stream::UsernameStream;

use crate::shard_common;
use crate::util::unique_scratch_dir;
use ahash::RandomState;
use anyhow::{Context, Result};
use parking_lot::Mutex;
use rayon::prelude::*;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

/// Disk-backed sharded dedup writer: concurrent-safe.
pub struct ShardedWriter {
    run_root: PathBuf,
    base_dir: PathBuf,
    shards: Vec<Mutex<BufWriter<File>>>,
    count: usize,
    state: RandomState, // seeded for deterministic sharding
}

impl ShardedWriter {
    pub fn create(work_dir: &Path, prefix: &str, count: usize) -> Result<Self> {
        let count = count.max(1);
        let run_root = unique_scratch_dir(work_dir, prefix, "shards");
        let shards_dir = run_root.join("shards");
        fs::create_dir_all(&shards_dir)?;

        let mut shards = Vec::with_capacity(count);
        for i in 0..count {
            let path = shards_dir.join(format!("shard_{:04}.tmp", i));
            let file = File::create(path)?;
            shards.push(Mutex::new(BufWriter::new(file)));
        }

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
        for w in &self.shards {
            w.lock().flush()?;
        }
        Ok(())
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
        fs::create_dir_all(&dedup_dir)?;

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
                dedup_single_shard(shard_path, &out_path)?;
                Ok(out_path)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok((deduped, run_root))
    }
}

fn dedup_single_shard(input: &Path, output: &Path) -> Result<()> {
    let in_file =
        File::open(input).with_context(|| format!("open shard for dedup: {}", input.display()))?;
    let mut reader = BufReader::new(in_file);

    let out_file = File::create(output)
        .with_context(|| format!("create dedup output: {}", output.display()))?;
    let mut writer = BufWriter::new(out_file);

    let mut seen: HashSet<String> = HashSet::with_capacity(64_000);

    let mut buf = String::with_capacity(8 * 1024);
    loop {
        buf.clear();
        let n = reader.read_line(&mut buf)?;
        if n == 0 {
            break;
        }
        if buf.ends_with('\n') {
            let _ = buf.pop();
            if buf.ends_with('\r') {
                let _ = buf.pop();
            }
        }
        if !buf.is_empty() {
            if seen.insert(buf.clone()) {
                writer.write_all(buf.as_bytes())?;
                writer.write_all(b"\n")?;
            }
        }
    }
    writer.flush()?;
    Ok(())
}
