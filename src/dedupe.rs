use crate::key_extractor::KeyExtractor;
use crate::mem::{available_memory_fraction, is_low_memory};
use crate::ndjson::{NdjsonReader, NdjsonWriter};
use crate::progress::ProgressScope;
use crate::util::{open_with_backoff, remove_with_backoff, replace_file_atomic_backoff};
use anyhow::{Context, Result};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Configuration for the generic dedupe engine.
#[derive(Clone, Debug)]
pub struct DedupeCfg {
    pub min_buf_mb: usize,
    pub max_buf_mb: usize,
    pub soft_low_frac: f64,
    pub high_frac: f64,
    pub adapt_cooldown_ms: u64,
    pub read_buf_bytes: usize,
    pub write_buf_bytes: usize,
    /// Hard cap on bytes inflight between the line-reader producer and the
    /// run-writer consumer. Peak in-memory footprint of `build_runs_sorted`
    /// is bounded by this value (one map being filled + one map awaiting
    /// disk write). 0 disables the cap and falls back to `max_buf_mb` only.
    pub inflight_bytes: usize,
}
impl Default for DedupeCfg {
    fn default() -> Self {
        Self {
            min_buf_mb: 512,
            max_buf_mb: 8192,
            soft_low_frac: 0.18,
            high_frac: 0.85,
            adapt_cooldown_ms: 400,
            read_buf_bytes: 4 * 1024 * 1024,
            write_buf_bytes: 4 * 1024 * 1024,
            // Default backpressure budget: 256 MiB. With channel capacity of 1,
            // peak inflight = ~2 * (inflight_bytes / 2) = 256 MiB regardless of
            // available_memory_fraction sampling.
            inflight_bytes: 256 * 1024 * 1024,
        }
    }
}

/// Phase 1: Build **sorted runs** by key. The run files contain the **original** lines
/// grouped by key (keys are written in sorted order within each run).
/// Returns the run file paths.
///
/// Producer/consumer split: the line-reader runs on the calling thread and
/// hands completed maps to a disk-writer thread via a bounded crossbeam
/// channel (capacity 1). Per-flush map size is capped at
/// `cfg.inflight_bytes / 2`, so peak in-memory footprint is bounded by
/// `cfg.inflight_bytes` regardless of the free-RAM target.
pub fn build_runs_sorted(
    input: &Path,
    runs_dir: &Path,
    key: &KeyExtractor,
    cfg: &DedupeCfg,
) -> Result<Vec<PathBuf>> {
    fs::create_dir_all(runs_dir)?;

    let total_in_bytes = fs::metadata(input).map(|m| m.len()).unwrap_or(0);
    let pb = ProgressScope::bytes("Dedupe: build runs", total_in_bytes);

    let mut rdr = NdjsonReader::open(input, cfg.read_buf_bytes)
        .with_context(|| format!("open {}", input.display()))?;

    let mut buf = String::with_capacity(64 * 1024);
    let mut buffered_bytes: usize = 0;
    let mut target_bytes: usize = cfg.min_buf_mb * 1024 * 1024;
    let mut last_eval = Instant::now() - Duration::from_millis(cfg.adapt_cooldown_ms * 2);

    // Hard cap on per-flush bytes. With channel capacity 1, total inflight is
    // bounded by 2 * per_flush_cap = inflight_bytes.
    let per_flush_cap = if cfg.inflight_bytes > 0 {
        (cfg.inflight_bytes / 2).max(1024 * 1024)
    } else {
        usize::MAX
    };

    let mut map: ahash::AHashMap<String, Vec<String>> = ahash::AHashMap::with_capacity(64_000);
    let mut run_idx: usize = 0;

    let (tx, rx) = crossbeam_channel::bounded::<(usize, ahash::AHashMap<String, Vec<String>>)>(1);

    let runs_dir_buf = runs_dir.to_path_buf();
    let write_buf_bytes = cfg.write_buf_bytes;

    let run_paths: Vec<PathBuf> = std::thread::scope(|s| -> Result<Vec<PathBuf>> {
        let writer_handle = s.spawn(move || -> Result<Vec<(usize, PathBuf)>> {
            let mut written: Vec<(usize, PathBuf)> = Vec::new();
            while let Ok((idx, mut m)) = rx.recv() {
                let run_path = runs_dir_buf.join(format!("run_{:04}.ndjson", idx));
                write_run_sorted(&run_path, &mut m, write_buf_bytes)?;
                written.push((idx, run_path));
            }
            Ok(written)
        });

        let producer_result: Result<()> = (|| -> Result<()> {
            loop {
                let n = rdr.read_line(&mut buf)?;
                if n == 0 { break; }
                pb.inc_bytes(n as u64);

                if buf.is_empty() { continue; }
                if let Some(k) = key.key_from_line(&buf) {
                    map.entry(k).or_default().push(buf.clone());
                    buffered_bytes += buf.len() + 1;
                }

                if last_eval.elapsed() >= Duration::from_millis(cfg.adapt_cooldown_ms) {
                    let free = available_memory_fraction();
                    let span = (cfg.high_frac - cfg.soft_low_frac).max(0.05f64);
                    let mut scale = ((free - cfg.soft_low_frac) / span).clamp(0.0, 1.0);
                    scale = scale * scale * (3.0 - 2.0 * scale);
                    let adaptive = ((cfg.min_buf_mb as f64
                        + (cfg.max_buf_mb as f64 - cfg.min_buf_mb as f64) * scale)
                        .round() as usize)
                        * 1024
                        * 1024;
                    // Cap adaptive target by per_flush_cap so the bounded
                    // channel — not the RAM-fraction sampler — is the
                    // primary backpressure mechanism.
                    target_bytes = adaptive.min(per_flush_cap);
                    last_eval = Instant::now();
                }

                if buffered_bytes >= target_bytes || is_low_memory(cfg.soft_low_frac) {
                    if !map.is_empty() {
                        run_idx += 1;
                        tracing::debug!(
                            target = "retl::backpressure",
                            stage = "dedupe.build_runs_sorted",
                            buffered_bytes,
                            target_bytes,
                            run_idx,
                            "handing run to writer (bounded channel; producer blocks if full)"
                        );
                        let owned = std::mem::replace(
                            &mut map,
                            ahash::AHashMap::with_capacity(64_000),
                        );
                        // send blocks here when consumer falls behind → backpressure
                        if tx.send((run_idx, owned)).is_err() {
                            // writer thread dropped rx (errored); break and let
                            // join surface the underlying error.
                            break;
                        }
                        buffered_bytes = 0;
                    }
                }
            }

            if !map.is_empty() {
                run_idx += 1;
                let owned = std::mem::take(&mut map);
                let _ = tx.send((run_idx, owned));
            }
            Ok(())
        })();

        // Always close tx so the consumer drains and returns.
        drop(tx);
        let writer_result = writer_handle.join().expect("writer thread panicked");
        // Surface writer errors first, then producer errors.
        let written = writer_result?;
        producer_result?;
        let mut sorted = written;
        sorted.sort_by_key(|(i, _)| *i);
        Ok(sorted.into_iter().map(|(_, p)| p).collect())
    })?;

    pb.finish(format!("runs built ({})", run_paths.len()));
    Ok(run_paths)
}

fn write_run_sorted(
    run_path: &Path,
    buf_map: &mut ahash::AHashMap<String, Vec<String>>,
    write_buf: usize,
) -> Result<()> {
    let mut keys: Vec<String> = buf_map.keys().cloned().collect();
    keys.sort_unstable();

    let mut w = NdjsonWriter::create(run_path, write_buf)
        .with_context(|| format!("create {}", run_path.display()))?;

    for k in keys {
        if let Some(lines) = buf_map.remove(&k) {
            for s in lines {
                w.write_line(&s)?;
            }
        }
    }
    w.finish()?;
    Ok(())
}

/// Phase 2: K-way merge of sorted runs. For each key, gather all consecutive lines
/// from all runs and call the user-provided `merge_same_key` callback to write **one**
/// output NDJSON line for that key.
pub fn merge_runs_sorted(
    runs: &[PathBuf],
    output: &Path,
    key: &KeyExtractor,
    cfg: &DedupeCfg,
    mut merge_same_key: impl FnMut(&str, Vec<String>, &mut dyn std::io::Write) -> Result<()>,
) -> Result<()> {
    if runs.is_empty() {
        // Nothing to write.
        let tmp = output.with_extension("ndjson.inprogress");
        let mut w = NdjsonWriter::create(&tmp, cfg.write_buf_bytes)?;
        w.finish_atomic(output)?;
        return Ok(());
    }

    if runs.len() == 1 {
        // Single run already sorted — promote to final.
        replace_file_atomic_backoff(&runs[0], output)?;
        let _ = fs::remove_file(&runs[0]);
        return Ok(());
    }

    let total_merge_bytes: u64 = runs.iter().map(|p| fs::metadata(p).map(|m| m.len()).unwrap_or(0)).sum();
    let pb = ProgressScope::bytes("Dedupe: merge runs", total_merge_bytes);

    #[derive(Eq)]
    struct HeapItem { key: String, run_idx: usize, line: String }
    impl Ord for HeapItem {
        fn cmp(&self, other: &Self) -> Ordering {
            other.key.cmp(&self.key).then_with(|| other.run_idx.cmp(&self.run_idx))
        }
    }
    impl PartialOrd for HeapItem { fn partial_cmp(&self, o: &Self) -> Option<Ordering> { Some(self.cmp(o)) } }
    impl PartialEq for HeapItem { fn eq(&self, o: &Self) -> bool { self.key == o.key && self.run_idx == o.run_idx } }

    let mut readers: Vec<(BufReader<File>, u64)> = Vec::with_capacity(runs.len()); // (reader, bytes_read)
    for p in runs {
        let f = open_with_backoff(p, 16, 50).with_context(|| format!("open {}", p.display()))?;
        readers.push((BufReader::with_capacity(cfg.read_buf_bytes, f), 0));
    }

    let tmp_out = output.with_extension("ndjson.inprogress");
    let mut out = crate::util::create_with_backoff(&tmp_out, 16, 50)
        .with_context(|| format!("create {}", tmp_out.display()))?;
    let mut out_buf = std::io::BufWriter::with_capacity(cfg.write_buf_bytes, out);

    let mut heap = BinaryHeap::<HeapItem>::new();

    // Prime heap
    for i in 0..readers.len() {
        let (r, read_bytes) = &mut readers[i];
        let mut s = String::new();
        let n = r.read_line(&mut s)?;
        if n > 0 {
            *read_bytes += n as u64;
            pb.inc_bytes(n as u64);
            if s.ends_with('\n') { s.pop(); if s.ends_with('\r') { s.pop(); } }
            if let Some(k) = key.key_from_line(&s) {
                heap.push(HeapItem { key: k, run_idx: i, line: s });
            }
        }
    }

    // Merge loop
    while let Some(top) = heap.pop() {
        let current_key = top.key.clone();

        // Collect all lines for `current_key`
        let mut group_lines: Vec<String> = Vec::with_capacity(16);
        group_lines.push(top.line);

        // Pull next from the run we just popped from
        {
            let (r, read_bytes) = &mut readers[top.run_idx];
            let mut s = String::new();
            let n = r.read_line(&mut s)?;
            if n > 0 {
                *read_bytes += n as u64;
                pb.inc_bytes(n as u64);
                if s.ends_with('\n') { s.pop(); if s.ends_with('\r') { s.pop(); } }
                if let Some(k) = key.key_from_line(&s) {
                    heap.push(HeapItem { key: k, run_idx: top.run_idx, line: s });
                }
            }
        }

        // While heap top matches current key, accumulate and advance
        while heap.peek().map(|h| h.key.as_str()) == Some(current_key.as_str()) {
            let item = heap.pop().unwrap();
            group_lines.push(item.line);

            let (r, read_bytes) = &mut readers[item.run_idx];
            let mut s = String::new();
            let n = r.read_line(&mut s)?;
            if n > 0 {
                *read_bytes += n as u64;
                pb.inc_bytes(n as u64);
                if s.ends_with('\n') { s.pop(); if s.ends_with('\r') { s.pop(); } }
                if let Some(k) = key.key_from_line(&s) {
                    heap.push(HeapItem { key: k, run_idx: item.run_idx, line: s });
                }
            }
        }

        // Delegate actual merging/encoding of the group to the caller
        merge_same_key(&current_key, group_lines, &mut out_buf)?;
    }

    out_buf.flush()?;
    drop(out_buf);
    replace_file_atomic_backoff(&tmp_out, output)?;

    // Cleanup run files (caller removes the runs directory)
    for p in runs {
        let _ = remove_with_backoff(p, 10, 25);
    }

    pb.finish("merge done");
    Ok(())
}
