//! Generic parallel aggregation support over JSONL inputs with progress.
//! Implement `Aggregator` for your aggregation state and call `aggregate_jsonls_parallel`.

use crate::progress::make_count_progress;
use crate::pipeline::RedditETL;
use crate::util::replace_file_atomic_backoff;
use anyhow::Result;
use rayon::prelude::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

pub trait Aggregator: Send + Default + Serialize + DeserializeOwned {
    fn ingest(&mut self, record: &Value);
    fn merge(&mut self, other: Self);
}

fn shard_name_for_input(shards_dir: &Path, input: &Path) -> PathBuf {
    let stem = input.file_stem().and_then(|s| s.to_str()).unwrap_or("part");
    // Common case: part_YYYY-MM
    let stem = stem.strip_prefix("part_").unwrap_or(stem);
    shards_dir.join(format!("agg_{}.json", stem))
}

impl RedditETL {
    /// Build per-file aggregation shards in parallel, then merge into `final_out`.
    /// - Always rebuilds shards (no resume behavior).
    /// - `pretty == true` pretty-prints the final output (shards are compact).
    ///
    /// Returns `(merged_shards, shard_errors)`: the number of shards that were
    /// successfully built and folded into the final output, and the number of
    /// inputs whose shard build failed (and were therefore dropped from the
    /// aggregate). Failures are logged via `tracing::warn!`.
    pub fn aggregate_jsonls_parallel<A: Aggregator>(
        &self,
        inputs: Vec<PathBuf>,
        shards_dir: &Path,
        final_out: &Path,
        _resume: bool,
        pretty: bool,
    ) -> Result<(usize, usize)> {
        fs::create_dir_all(shards_dir)?;

        let pb_build = if self.opts.progress { Some(make_count_progress(inputs.len() as u64, "Aggregate: build shards")) } else { None };

        // Build shards in parallel (always rebuild). Per-input failures are
        // surfaced via tracing and counted, not silently swallowed.
        let shard_errors = AtomicUsize::new(0);
        inputs.par_iter().for_each(|input| {
            let out_shard = shard_name_for_input(shards_dir, input);
            let tmp_shard = out_shard.with_extension("json.inprogress");

            let result = (|| -> Result<()> {
                let mut agg = A::default();
                let f = File::open(input)?;
                let r = BufReader::new(f);
                for line in r.lines() {
                    let line = line?;
                    if line.is_empty() { continue; }
                    if let Ok(v) = serde_json::from_str::<Value>(&line) {
                        agg.ingest(&v);
                    }
                }
                let out = File::create(&tmp_shard)?;
                let mut w = BufWriter::new(out);
                serde_json::to_writer(&mut w, &agg)?;
                w.flush()?;
                drop(w);
                replace_file_atomic_backoff(&tmp_shard, &out_shard)?;
                Ok(())
            })();

            if let Err(e) = result {
                shard_errors.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(path=%out_shard.display(), error=%e, "failed building aggregate shard");
                // Best-effort cleanup of any partial temp file.
                let _ = fs::remove_file(&tmp_shard);
            }

            if let Some(pb) = &pb_build { pb.inc(1); }
        });
        let shard_errors = shard_errors.into_inner();

        if let Some(pb) = pb_build { pb.finish_with_message("Aggregate: shard build done"); }

        // Merge shards. Only consider successfully-written shard files
        // (skip any leftover `.inprogress` temps from prior failures).
        let mut shards: Vec<PathBuf> = fs::read_dir(shards_dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("json"))
            .collect();
        shards.sort();

        let pb_merge = if self.opts.progress { Some(make_count_progress(shards.len() as u64, "Aggregate: merge shards")) } else { None };

        let mut total = A::default();
        let mut merged_shards = 0usize;
        for shard in shards {
            let f = File::open(&shard)?;
            let r = BufReader::new(f);
            let part: A = serde_json::from_reader(r)?;
            total.merge(part);
            merged_shards += 1;
            if let Some(pb) = &pb_merge { pb.inc(1); }
        }

        // Atomically publish the final output via tmp + rename so an
        // interrupted run never leaves a half-written `final_out` in place.
        let tmp_final = final_out.with_extension("json.inprogress");
        {
            let out = File::create(&tmp_final)?;
            let mut w = BufWriter::new(out);
            if pretty {
                serde_json::to_writer_pretty(&mut w, &total)?;
            } else {
                serde_json::to_writer(&mut w, &total)?;
            }
            w.flush()?;
        }
        replace_file_atomic_backoff(&tmp_final, final_out)?;

        if let Some(pb) = pb_merge { pb.finish_with_message("Aggregate: final written"); }
        Ok((merged_shards, shard_errors))
    }
}
