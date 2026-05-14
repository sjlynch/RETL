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

    /// Reduce all shards by summing per-key values.
    ///
    /// **Overflow contract:** per-key totals are accumulated as `i64` using
    /// [`i64::checked_add`]. On overflow the running total saturates to
    /// [`i64::MAX`] (positive addend) or [`i64::MIN`] (negative addend) and
    /// a single `tracing::warn!` is emitted per affected shard — further
    /// overflows in the same shard are suppressed to avoid log spam. Callers
    /// that need strict overflow detection should partition input so per-key
    /// totals stay within `i64`, or post-process the warning log.
    pub fn reduce_sum(self, prefix: &str) -> Result<Vec<PathBuf>> {
        let (outs, _scratch_root) = self.reduce_sum_with_scratch(prefix)?;
        Ok(outs)
    }

    /// Like [`reduce_sum`](Self::reduce_sum) but also returns the scratch
    /// directory root so the caller can clean it up.
    ///
    /// See [`reduce_sum`](Self::reduce_sum) for the overflow contract.
    pub fn reduce_sum_with_scratch(self, prefix: &str) -> Result<(Vec<PathBuf>, PathBuf)> {
        self.reduce(prefix, Reducer::Sum, "kv_sum")
    }

    /// Reduce all shards by keeping the minimum value seen per key.
    ///
    /// Every observed `i64` (including [`i64::MAX`]) is a legal value: keys
    /// that never appear in input never appear in output, and a key whose
    /// only observation is [`i64::MAX`] reduces to [`i64::MAX`].
    pub fn reduce_min(self, prefix: &str) -> Result<Vec<PathBuf>> {
        let (outs, _scratch_root) = self.reduce_min_with_scratch(prefix)?;
        Ok(outs)
    }

    /// Like [`reduce_min`](Self::reduce_min) but also returns the scratch
    /// directory root so the caller can clean it up.
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
    /// Per-key sum with saturating overflow. See [`ShardedKVWriter::reduce_sum`]
    /// for the public contract: `checked_add` saturates to `i64::{MAX,MIN}` and
    /// emits a one-time `tracing::warn!` per affected shard.
    Sum,
    /// Per-key minimum. Sentinel-free: the first observation seeds the entry,
    /// so any `i64` (including `i64::MAX`) is a legal observed value and keys
    /// never observed never appear in output.
    Min,
}

fn reduce_shard(input: &Path, output: &Path, reducer: Reducer) -> Result<()> {
    let mut acc: HashMap<String, i64> = HashMap::with_capacity(64_000);
    let mut sum_overflow_warned = false;
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
            Reducer::Sum => {
                let e = acc.entry(k.to_string()).or_insert(0i64);
                match e.checked_add(val) {
                    Some(s) => *e = s,
                    None => {
                        // Saturate to keep the run going; emit one warning per
                        // shard so the user sees the overflow without log spam.
                        let saturated = if val >= 0 { i64::MAX } else { i64::MIN };
                        if !sum_overflow_warned {
                            tracing::warn!(
                                path = %input.display(),
                                line = line_no,
                                key = %k,
                                accumulator = *e,
                                value = val,
                                saturated_to = saturated,
                                "kv_shard Sum overflow: saturating; further overflows in this shard suppressed"
                            );
                            sum_overflow_warned = true;
                        }
                        *e = saturated;
                    }
                }
            }
            Reducer::Min => {
                // Sentinel-free: seed with the first observed value so `i64::MAX`
                // is a legal observation and 'never seen' is unambiguous (the
                // key simply never enters the map).
                acc.entry(k.to_string())
                    .and_modify(|cur| {
                        if val < *cur {
                            *cur = val;
                        }
                    })
                    .or_insert(val);
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

    fn read_kv_tsv(path: &Path) -> HashMap<String, i64> {
        let mut out = HashMap::new();
        let r = BufReader::new(File::open(path).expect("open output"));
        for line in r.lines() {
            let line = line.expect("read line");
            if line.is_empty() {
                continue;
            }
            let (k, v) = line.split_once('\t').expect("tab");
            out.insert(k.to_string(), v.parse::<i64>().expect("i64"));
        }
        out
    }

    #[test]
    fn reduce_shard_sum_saturates_on_positive_overflow() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let input = tmp.path().join("kv_0000.tmp");
        let output = tmp.path().join("kv_0000.tsv");

        let mut f = std::fs::File::create(&input).expect("create input");
        writeln!(f, "alice\t{}", i64::MAX).expect("write");
        writeln!(f, "alice\t1").expect("write");
        writeln!(f, "alice\t1000").expect("write");
        writeln!(f, "bob\t42").expect("write");
        drop(f);

        reduce_shard(&input, &output, Reducer::Sum).expect("sum should saturate, not panic");
        let rows = read_kv_tsv(&output);
        assert_eq!(rows.get("alice").copied(), Some(i64::MAX));
        assert_eq!(rows.get("bob").copied(), Some(42));
    }

    #[test]
    fn reduce_shard_sum_saturates_on_negative_overflow() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let input = tmp.path().join("kv_0000.tmp");
        let output = tmp.path().join("kv_0000.tsv");

        let mut f = std::fs::File::create(&input).expect("create input");
        writeln!(f, "alice\t{}", i64::MIN).expect("write");
        writeln!(f, "alice\t-1").expect("write");
        drop(f);

        reduce_shard(&input, &output, Reducer::Sum).expect("sum should saturate, not panic");
        let rows = read_kv_tsv(&output);
        assert_eq!(rows.get("alice").copied(), Some(i64::MIN));
    }

    #[test]
    fn reduce_shard_min_accepts_i64_max_as_legal_value() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let input = tmp.path().join("kv_0000.tmp");
        let output = tmp.path().join("kv_0000.tsv");

        let mut f = std::fs::File::create(&input).expect("create input");
        // `lonely_max` is only ever observed as i64::MAX — must round-trip.
        writeln!(f, "lonely_max\t{}", i64::MAX).expect("write");
        // `pair_max` is seen twice as i64::MAX — still MAX, no sentinel collision.
        writeln!(f, "pair_max\t{}", i64::MAX).expect("write");
        writeln!(f, "pair_max\t{}", i64::MAX).expect("write");
        // `mixed` proves the min still wins when there is a smaller value.
        writeln!(f, "mixed\t{}", i64::MAX).expect("write");
        writeln!(f, "mixed\t5").expect("write");
        writeln!(f, "mixed\t{}", i64::MAX).expect("write");
        drop(f);

        reduce_shard(&input, &output, Reducer::Min).expect("min should succeed");
        let rows = read_kv_tsv(&output);
        assert_eq!(rows.get("lonely_max").copied(), Some(i64::MAX));
        assert_eq!(rows.get("pair_max").copied(), Some(i64::MAX));
        assert_eq!(rows.get("mixed").copied(), Some(5));
        // 'never seen' is unambiguous: keys that never appeared are absent.
        assert!(!rows.contains_key("ghost"));
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn reduce_shard_min_picks_smallest_value() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let input = tmp.path().join("kv_0000.tmp");
        let output = tmp.path().join("kv_0000.tsv");

        let mut f = std::fs::File::create(&input).expect("create input");
        writeln!(f, "a\t30").expect("write");
        writeln!(f, "a\t10").expect("write");
        writeln!(f, "a\t20").expect("write");
        writeln!(f, "b\t-5").expect("write");
        writeln!(f, "b\t-100").expect("write");
        drop(f);

        reduce_shard(&input, &output, Reducer::Min).expect("min should succeed");
        let rows = read_kv_tsv(&output);
        assert_eq!(rows.get("a").copied(), Some(10));
        assert_eq!(rows.get("b").copied(), Some(-100));
    }
}
