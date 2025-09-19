//! Generic parallel aggregation support over JSONL inputs with progress.
//! Implement `Aggregator` for your aggregation state and call `aggregate_jsonls_parallel`.

use crate::progress::make_count_progress;
use crate::pipeline::RedditETL;
use anyhow::Result;
use rayon::prelude::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

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
    pub fn aggregate_jsonls_parallel<A: Aggregator>(
        &self,
        inputs: Vec<PathBuf>,
        shards_dir: &Path,
        final_out: &Path,
        _resume: bool,
        pretty: bool,
    ) -> Result<()> {
        fs::create_dir_all(shards_dir)?;

        let pb_build = if self.opts.progress { Some(make_count_progress(inputs.len() as u64, "Aggregate: build shards")) } else { None };

        // Build shards in parallel (always rebuild)
        inputs.par_iter().for_each(|input| {
            let out_shard = shard_name_for_input(shards_dir, input);

            let _ = (|| -> Result<()> {
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
                let out = File::create(&out_shard)?;
                let mut w = BufWriter::new(out);
                serde_json::to_writer(&mut w, &agg)?;
                w.flush()?;
                Ok(())
            })();

            if let Some(pb) = &pb_build { pb.inc(1); }
        });

        if let Some(pb) = pb_build { pb.finish_with_message("Aggregate: shard build done"); }

        // Merge shards
        let mut shards: Vec<PathBuf> = fs::read_dir(shards_dir)?.filter_map(|e| e.ok().map(|e| e.path())).collect();
        shards.sort();

        let pb_merge = if self.opts.progress { Some(make_count_progress(shards.len() as u64, "Aggregate: merge shards")) } else { None };

        let mut total = A::default();
        for shard in shards {
            let f = File::open(&shard)?;
            let r = BufReader::new(f);
            let part: A = serde_json::from_reader(r)?;
            total.merge(part);
            if let Some(pb) = &pb_merge { pb.inc(1); }
        }

        let out = File::create(final_out)?;
        let mut w = BufWriter::new(out);
        if pretty {
            serde_json::to_writer_pretty(&mut w, &total)?;
        } else {
            serde_json::to_writer(&mut w, &total)?;
        }
        w.flush()?;

        if let Some(pb) = pb_merge { pb.finish_with_message("Aggregate: final written"); }
        Ok(())
    }
}
