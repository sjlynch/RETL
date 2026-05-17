use super::cfg::DedupeCfg;
use super::note_key_extraction_failed;
use crate::atomic_write::{ensure_staging_dir, write_jsonl_atomic};
use crate::key_extractor::KeyExtractor;
use crate::ndjson::{read_line_capped, DEFAULT_MAX_LINE_BYTES};
use crate::progress::ProgressScope;
use crate::util::remove_with_backoff;
use crate::zstd_jsonl::malformed_json_error;
use anyhow::{Context, Result};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;

#[derive(Eq)]
struct HeapItem {
    key: String,
    run_idx: usize,
    line: String,
}
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.run_idx.cmp(&self.run_idx))
    }
}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl PartialEq for HeapItem {
    fn eq(&self, o: &Self) -> bool {
        self.key == o.key && self.run_idx == o.run_idx
    }
}

// Read until `reader` yields a line with an extractable key, stripping
// CRLF and building a HeapItem for it. Returns `Ok(None)` only at EOF.
// Lines without a key are counted and skipped; `read_bytes` and `pb` are
// updated on every successful read so progress accounting stays
// consistent across the call sites below.
fn advance_reader(
    reader: &mut BufReader<File>,
    run_path: &Path,
    run_idx: usize,
    key: &KeyExtractor,
    read_bytes: &mut u64,
    line_number: &mut u64,
    pb: &ProgressScope,
    key_extractions_failed: Option<&AtomicU64>,
) -> Result<Option<HeapItem>> {
    loop {
        let mut s = String::new();
        let n = read_line_capped(reader, &mut s, DEFAULT_MAX_LINE_BYTES, run_path).with_context(
            || {
                format!(
                    "read dedupe run {} near line {}",
                    run_path.display(),
                    *line_number + 1
                )
            },
        )?;
        if n == 0 {
            return Ok(None);
        }
        *read_bytes += n as u64;
        *line_number += 1;
        pb.inc_bytes(n as u64);
        match key
            .key_from_line(&s)
            .map_err(|e| malformed_json_error(run_path, *line_number, e))?
        {
            Some(k) => {
                return Ok(Some(HeapItem {
                    key: k,
                    run_idx,
                    line: s,
                }))
            }
            None => note_key_extraction_failed(key_extractions_failed),
        }
    }
}

/// Phase 2: K-way merge of sorted runs. For each key, gather all consecutive lines
/// from all runs and call the user-provided `merge_same_key` callback to write **one**
/// output NDJSON line for that key.
pub fn merge_runs_sorted(
    runs: &[PathBuf],
    output: &Path,
    key: &KeyExtractor,
    cfg: &DedupeCfg,
    merge_same_key: impl FnMut(&str, Vec<String>, &mut dyn std::io::Write) -> Result<()>,
) -> Result<()> {
    merge_runs_sorted_with_key_stats(runs, output, key, cfg, merge_same_key, None)
}

pub(crate) fn merge_runs_sorted_with_key_stats(
    runs: &[PathBuf],
    output: &Path,
    key: &KeyExtractor,
    cfg: &DedupeCfg,
    mut merge_same_key: impl FnMut(&str, Vec<String>, &mut dyn std::io::Write) -> Result<()>,
    key_extractions_failed: Option<&AtomicU64>,
) -> Result<()> {
    // Route through `<dest_parent>/_staging/<basename>.retl-<pid>-<nonce>.inprogress`
    // so concurrent dedupe runs targeting different outputs in the same directory
    // (e.g. `out/x.txt` and `out/x.json`) cannot collide on a shared sibling
    // temp, and so crashed runs leave staged leftovers that `sweep_stale_inprogress`
    // can recover.
    let parent_dir = output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let staging_dir = ensure_staging_dir(&parent_dir)
        .with_context(|| format!("ensure staging dir under {}", parent_dir.display()))?;

    if runs.is_empty() {
        // Nothing to write — still publish an empty file atomically.
        write_jsonl_atomic(&staging_dir, output, cfg.write_buf_bytes, |_w| Ok(()))?;
        return Ok(());
    }

    let total_merge_bytes: u64 = runs
        .iter()
        .map(|p| fs::metadata(p).map(|m| m.len()).unwrap_or(0))
        .sum();
    let pb = ProgressScope::bytes("Dedupe: merge runs", total_merge_bytes);

    let mut readers: Vec<(BufReader<File>, u64, u64)> = Vec::with_capacity(runs.len()); // (reader, bytes_read, lines_read)
    for p in runs {
        let f = crate::util::open_with_default_backoff(p)
            .with_context(|| format!("open {}", p.display()))?;
        readers.push((BufReader::with_capacity(cfg.read_buf_bytes, f), 0, 0));
    }

    write_jsonl_atomic(
        &staging_dir,
        output,
        cfg.write_buf_bytes,
        |out_buf| -> Result<()> {
            let mut heap = BinaryHeap::<HeapItem>::new();

            // Prime heap
            for i in 0..readers.len() {
                let (r, read_bytes, line_number) = &mut readers[i];
                if let Some(item) = advance_reader(
                    r,
                    &runs[i],
                    i,
                    key,
                    read_bytes,
                    line_number,
                    &pb,
                    key_extractions_failed,
                )? {
                    heap.push(item);
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
                    let (r, read_bytes, line_number) = &mut readers[top.run_idx];
                    if let Some(item) = advance_reader(
                        r,
                        &runs[top.run_idx],
                        top.run_idx,
                        key,
                        read_bytes,
                        line_number,
                        &pb,
                        key_extractions_failed,
                    )? {
                        heap.push(item);
                    }
                }

                // While heap top matches current key, accumulate and advance
                while heap.peek().map(|h| h.key.as_str()) == Some(current_key.as_str()) {
                    let item = heap.pop().unwrap();
                    let run_idx = item.run_idx;
                    group_lines.push(item.line);

                    let (r, read_bytes, line_number) = &mut readers[run_idx];
                    if let Some(item) = advance_reader(
                        r,
                        &runs[run_idx],
                        run_idx,
                        key,
                        read_bytes,
                        line_number,
                        &pb,
                        key_extractions_failed,
                    )? {
                        heap.push(item);
                    }
                }

                // Delegate actual merging/encoding of the group to the caller
                merge_same_key(&current_key, group_lines, out_buf)?;
            }

            Ok(())
        },
    )?;

    // Cleanup run files (caller removes the runs directory)
    for p in runs {
        let _ = remove_with_backoff(p, 10, 25);
    }

    pb.finish("merge done");
    Ok(())
}
