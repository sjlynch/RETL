use super::cfg::{DedupeCfg, BUILD_RUNS_CHANNEL_CAP, BYTES_PER_MB};
use super::note_key_extraction_failed;
use crate::key_extractor::KeyExtractor;
use crate::mem::{available_memory_fraction, is_low_memory, AdaptiveMemCfg};
use crate::ndjson::{NdjsonReader, NdjsonWriter};
use crate::progress::ProgressScope;
use crate::util::smoothstep_memory_fraction;
use crate::zstd_jsonl::malformed_json_error;
use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

const MAP_INITIAL_CAPACITY: usize = 64_000;

type RunMap = ahash::AHashMap<String, Vec<String>>;

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
    build_runs_sorted_with_key_stats(input, runs_dir, key, cfg, None)
}

pub(crate) fn build_runs_sorted_with_key_stats(
    input: &Path,
    runs_dir: &Path,
    key: &KeyExtractor,
    cfg: &DedupeCfg,
    key_extractions_failed: Option<&AtomicU64>,
) -> Result<Vec<PathBuf>> {
    crate::util::create_dir_all_with_default_backoff(runs_dir)
        .with_context(|| format!("create runs dir {}", runs_dir.display()))?;

    let total_in_bytes = fs::metadata(input).map(|m| m.len()).unwrap_or(0);
    let pb = ProgressScope::bytes("Dedupe: build runs", total_in_bytes);

    let mut rdr = NdjsonReader::open(input, cfg.read_buf_bytes)
        .with_context(|| format!("open {}", input.display()))?;

    // Hard cap on per-flush bytes. With channel capacity 1, total inflight is
    // bounded by 2 * per_flush_cap = inflight_bytes.
    let per_flush_cap = if cfg.inflight_bytes > 0 {
        (cfg.inflight_bytes / 2).max(BYTES_PER_MB)
    } else {
        usize::MAX
    };

    let (tx, rx) = crossbeam_channel::bounded::<(usize, RunMap)>(BUILD_RUNS_CHANNEL_CAP);

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

        let producer_result = run_producer(
            input,
            cfg,
            &mut rdr,
            key,
            per_flush_cap,
            tx.clone(),
            &pb,
            cfg.mem.clone(),
            key_extractions_failed,
        );

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

fn run_producer(
    input_path: &Path,
    cfg: &DedupeCfg,
    rdr: &mut NdjsonReader,
    key: &KeyExtractor,
    per_flush_cap: usize,
    tx: crossbeam_channel::Sender<(usize, RunMap)>,
    pb: &ProgressScope,
    adaptive_mem: AdaptiveMemCfg,
    key_extractions_failed: Option<&AtomicU64>,
) -> Result<()> {
    let mut buf = String::with_capacity(64 * 1024);
    let mut buffered_bytes: usize = 0;
    let mut target_bytes: usize = cfg.min_buf_mb * BYTES_PER_MB;
    let mut last_eval = Instant::now() - Duration::from_millis(adaptive_mem.adapt_cooldown_ms * 2);
    let mut map: RunMap = ahash::AHashMap::with_capacity(MAP_INITIAL_CAPACITY);
    let mut run_idx: usize = 0;
    let mut line_number: u64 = 0;

    loop {
        // Reviewed exception to the direct `read_line` audit: `NdjsonReader`
        // delegates to `read_line_capped(DEFAULT_MAX_LINE_BYTES)` and keeps
        // the path in its InvalidData error.
        let n = rdr.read_line(&mut buf)?;
        if n == 0 {
            break;
        }
        pb.inc_bytes(n as u64);
        line_number += 1;

        if buf.is_empty() {
            continue;
        }
        match key
            .key_from_line(&buf)
            .map_err(|e| malformed_json_error(input_path, line_number, e))?
        {
            Some(k) => {
                map.entry(k).or_default().push(buf.clone());
                buffered_bytes += buf.len() + 1;
            }
            None => note_key_extraction_failed(key_extractions_failed),
        }

        if last_eval.elapsed() >= Duration::from_millis(adaptive_mem.adapt_cooldown_ms) {
            let scale = smoothstep_memory_fraction(
                available_memory_fraction(),
                adaptive_mem.soft_low_frac,
                adaptive_mem.high_frac,
            );
            let adaptive = ((cfg.min_buf_mb as f64
                + (cfg.max_buf_mb as f64 - cfg.min_buf_mb as f64) * scale)
                .round() as usize)
                * BYTES_PER_MB;
            // Cap adaptive target by per_flush_cap so the bounded
            // channel — not the RAM-fraction sampler — is the
            // primary backpressure mechanism.
            target_bytes = adaptive.min(per_flush_cap);
            last_eval = Instant::now();
        }

        if buffered_bytes >= target_bytes || is_low_memory(adaptive_mem.soft_low_frac) {
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
                    ahash::AHashMap::with_capacity(MAP_INITIAL_CAPACITY),
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
}

fn write_run_sorted(run_path: &Path, buf_map: &mut RunMap, write_buf: usize) -> Result<()> {
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
