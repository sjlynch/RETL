use crate::shard_common;
use crate::util::unique_scratch_dir;
use ahash::RandomState;
use anyhow::{Context, Result};
use parking_lot::Mutex;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

/// Sharded key->i64 writer for large-scale reductions (sum/min, etc.).
pub struct ShardedKVWriter {
    run_root: PathBuf,
    base_dir: PathBuf,
    shards: Vec<Mutex<BufWriter<File>>>,
    count: usize,
    state: RandomState,
}

impl ShardedKVWriter {
    pub fn create(work_dir: &Path, prefix: &str, count: usize) -> Result<Self> {
        let count = count.max(1);
        let run_root = unique_scratch_dir(work_dir, prefix, "kv_shards");
        let dir = run_root.join("shards");
        fs::create_dir_all(&dir)?;
        let mut shards = Vec::with_capacity(count);
        for i in 0..count {
            let p = dir.join(format!("kv_{:04}.tmp", i));
            shards.push(Mutex::new(BufWriter::new(File::create(p)?)));
        }
        let state = shard_common::seeded_state("kv");
        Ok(Self {
            run_root,
            base_dir: dir,
            shards,
            count,
            state,
        })
    }

    pub fn scratch_root(&self) -> &Path {
        &self.run_root
    }

    #[inline]
    fn shard_index(&self, k: &str) -> usize {
        shard_common::shard_index(&self.state, k, self.count)
    }

    pub fn write_kv(&self, key: &str, val: i64) -> Result<()> {
        let idx = self.shard_index(key);
        let mut w = self.shards[idx].lock();
        w.write_all(key.as_bytes())?;
        w.write_all(b"\t")?;
        w.write_all(val.to_string().as_bytes())?;
        w.write_all(b"\n")?;
        Ok(())
    }

    pub fn flush_all(&self) -> Result<()> {
        for w in &self.shards {
            w.lock().flush()?;
        }
        Ok(())
    }

    pub fn reduce_sum(self, prefix: &str) -> Result<Vec<PathBuf>> {
        let (outs, _scratch_root) = self.reduce_sum_with_scratch(prefix)?;
        Ok(outs)
    }

    pub fn reduce_sum_with_scratch(self, prefix: &str) -> Result<(Vec<PathBuf>, PathBuf)> {
        self.reduce(prefix, Reducer::Sum, "kv_sum")
    }

    pub fn reduce_min(self, prefix: &str) -> Result<Vec<PathBuf>> {
        let (outs, _scratch_root) = self.reduce_min_with_scratch(prefix)?;
        Ok(outs)
    }

    pub fn reduce_min_with_scratch(self, prefix: &str) -> Result<(Vec<PathBuf>, PathBuf)> {
        self.reduce(prefix, Reducer::Min, "kv_min")
    }

    fn reduce(
        self,
        prefix: &str,
        reducer: Reducer,
        suffix: &str,
    ) -> Result<(Vec<PathBuf>, PathBuf)> {
        // Ensure on-disk buffers are flushed before we move fields out of `self`.
        self.flush_all()?;

        // Move all fields out of `self` **once** to avoid partial-move borrow errors.
        let ShardedKVWriter {
            run_root,
            base_dir,
            shards,
            count,
            state: _,
        } = self;
        drop(shards);

        let out_dir = run_root.join(format!("{prefix}_{suffix}"));
        fs::create_dir_all(&out_dir)?;

        // Compute shard input paths from moved fields (no further `self` usage).
        let ins: Vec<PathBuf> = (0..count)
            .map(|i| base_dir.join(format!("kv_{:04}.tmp", i)))
            .collect();

        let outs: Vec<PathBuf> = ins
            .par_iter()
            .map(|p| -> Result<PathBuf> {
                let out = out_dir.join(
                    p.file_name()
                        .unwrap()
                        .to_string_lossy()
                        .replace(".tmp", ".tsv"),
                );
                reduce_shard(p, &out, reducer)?;
                Ok(out)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((outs, run_root))
    }
}

#[derive(Clone, Copy)]
enum Reducer {
    Sum,
    Min,
}

fn reduce_shard(input: &Path, output: &Path, reducer: Reducer) -> Result<()> {
    let mut acc: HashMap<String, i64> = HashMap::with_capacity(64_000);
    let r = BufReader::new(File::open(input).with_context(|| format!("open {}", input.display()))?);
    for line in r.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        if let Some((k, v)) = line.split_once('\t') {
            if let Ok(val) = v.parse::<i64>() {
                match reducer {
                    Reducer::Sum => *acc.entry(k.to_string()).or_insert(0) += val,
                    Reducer::Min => {
                        let e = acc.entry(k.to_string()).or_insert(i64::MAX);
                        if val < *e {
                            *e = val;
                        }
                    }
                }
            }
        }
    }
    let mut rows: Vec<(String, i64)> = acc.into_iter().collect();
    rows.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

    let mut w = BufWriter::new(File::create(output)?);
    for (k, v) in rows {
        w.write_all(k.as_bytes())?;
        w.write_all(b"\t")?;
        w.write_all(v.to_string().as_bytes())?;
        w.write_all(b"\n")?;
    }
    w.flush()?;
    Ok(())
}
