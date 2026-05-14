use crate::shard_common;
use crate::util::{
    create_dir_all_with_backoff, create_with_backoff, open_with_backoff, unique_scratch_dir,
};
use ahash::RandomState;
use anyhow::{Context, Result};
use parking_lot::Mutex;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
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
        create_dir_all_with_backoff(&dir, 16, 50)
            .with_context(|| format!("create kv shard scratch dir {}", dir.display()))?;
        let mut shards = Vec::with_capacity(count);
        for i in 0..count {
            let p = dir.join(format!("kv_{:04}.tmp", i));
            let file = create_with_backoff(&p, 16, 50)
                .with_context(|| format!("create kv shard scratch {}", p.display()))?;
            shards.push(Mutex::new(BufWriter::new(file)));
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
        create_dir_all_with_backoff(&out_dir, 16, 50)
            .with_context(|| format!("create kv reduce dir {}", out_dir.display()))?;

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
    let r = BufReader::new(
        open_with_backoff(input, 16, 50).with_context(|| format!("open {}", input.display()))?,
    );
    for (line_idx, line) in r.lines().enumerate() {
        let line_no = line_idx + 1;
        let line = line.with_context(|| format!("read {} at line {}", input.display(), line_no))?;
        if line.is_empty() {
            continue;
        }
        let (k, v) = line.split_once('\t').ok_or_else(|| {
            anyhow::anyhow!(
                "malformed K-V shard line in {} at line {}: missing tab separator",
                input.display(),
                line_no
            )
        })?;
        let val = v.parse::<i64>().with_context(|| {
            format!(
                "malformed K-V shard line in {} at line {}: value is not an i64",
                input.display(),
                line_no
            )
        })?;
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
    let mut rows: Vec<(String, i64)> = acc.into_iter().collect();
    rows.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

    let mut w = BufWriter::new(
        create_with_backoff(output, 16, 50)
            .with_context(|| format!("create {}", output.display()))?,
    );
    for (k, v) in rows {
        w.write_all(k.as_bytes())?;
        w.write_all(b"\t")?;
        w.write_all(v.to_string().as_bytes())?;
        w.write_all(b"\n")?;
    }
    w.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn reduce_shard_errors_on_missing_tab() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let input = tmp.path().join("kv_0000.tmp");
        let output = tmp.path().join("kv_0000.tsv");
        std::fs::write(&input, b"alice\t1\nmalformed\n").expect("write input");

        let err = reduce_shard(&input, &output, Reducer::Sum).expect_err("malformed shard fails");
        let msg = format!("{err:#}");

        assert!(
            msg.contains("malformed K-V shard line") && msg.contains("missing tab separator"),
            "unexpected error: {msg}"
        );
        assert!(
            !output.exists(),
            "malformed reduction must not publish output"
        );
    }

    #[test]
    fn reduce_shard_errors_on_non_i64_value() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let input = tmp.path().join("kv_0000.tmp");
        let output = tmp.path().join("kv_0000.tsv");
        let mut f = std::fs::File::create(&input).expect("create input");
        writeln!(f, "alice\tbogus").expect("write input");

        let err = reduce_shard(&input, &output, Reducer::Sum).expect_err("malformed shard fails");
        let msg = format!("{err:#}");

        assert!(
            msg.contains("malformed K-V shard line") && msg.contains("value is not an i64"),
            "unexpected error: {msg}"
        );
    }
}
