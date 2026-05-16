
struct BuildOutcome {
    shards: Vec<PathBuf>,
    report: AggregateBuildReport,
}

enum ShardBuildResult {
    Clean {
        input: PathBuf,
        shard: PathBuf,
    },
    Partial {
        input: PathBuf,
        error: String,
        shard: Option<PathBuf>,
    },
    Fatal {
        input: PathBuf,
        error: String,
    },
}

fn write_shard<A: Aggregator>(staging_dir: &Path, out_shard: &Path, agg: &A) -> Result<()> {
    write_jsonl_atomic(staging_dir, out_shard, AGGREGATE_WRITE_BUF_BYTES, |w| {
        serde_json::to_writer(w, agg)?;
        Ok(())
    })
    .with_context(|| format!("write aggregate shard {}", out_shard.display()))
}

/// Phase 1: build per-input aggregator shards in parallel.
///
/// Each input is ingested into a fresh `make_agg()` state and atomically
/// written to its precomputed shard path via the staging + rename dance.
/// Fatal per-input failures are surfaced via `tracing::warn!` and collected,
/// not propagated, so one bad input doesn't sink the entire run. Mid-file
/// read errors are collected separately; by default their partial state is
/// dropped instead of merged.
fn build_aggregate_shards_with<A, F>(
    inputs: &[PathBuf],
    shard_paths: &[PathBuf],
    staging_dir: &Path,
    progress: bool,
    make_agg: &F,
    partial_policy: AggregatePartialReadPolicy,
) -> BuildOutcome
where
    A: Aggregator,
    F: Fn() -> A + Send + Sync,
{
    let pb_build = if progress {
        Some(make_count_progress(
            inputs.len() as u64,
            "Aggregate: build shards",
        ))
    } else {
        None
    };

    let outcomes: Vec<ShardBuildResult> = inputs
        .par_iter()
        .zip(shard_paths.par_iter())
        .map(|(input, out_shard)| {
            let mut agg = make_agg();
            let mut line_no = 0_u64;
            let ingest_result = for_each_jsonl_line_cfg(input, AGGREGATE_INGEST_BUF_BYTES, |line| {
                line_no += 1;
                if !line.is_empty() {
                    match serde_json::from_str::<Value>(line) {
                        Ok(v) => agg.ingest(&v),
                        Err(e) => anyhow::bail!(
                            "malformed JSON in {} at line {}: {}",
                            input.display(),
                            line_no,
                            e
                        ),
                    }
                }
                Ok(())
            });

            let outcome = match ingest_result {
                Err(e) => {
                    tracing::warn!(
                        input=%input.display(),
                        shard=%out_shard.display(),
                        error=%e,
                        "failed building aggregate shard"
                    );
                    ShardBuildResult::Fatal {
                        input: input.clone(),
                        error: e.to_string(),
                    }
                }
                Ok(read_error) => {
                    let partial_error = read_error.as_ref().map(|e| e.to_string());
                    if partial_error.is_some()
                        && partial_policy == AggregatePartialReadPolicy::Strict
                    {
                        let error = partial_error.expect("checked is_some");
                        tracing::warn!(
                            input=%input.display(),
                            error=%error,
                            "partial aggregate input read; dropping partial shard"
                        );
                        ShardBuildResult::Partial {
                            input: input.clone(),
                            error,
                            shard: None,
                        }
                    } else {
                        match write_shard(staging_dir, out_shard, &agg) {
                            Ok(()) => {
                                if let Some(error) = partial_error {
                                    tracing::warn!(
                                        input=%input.display(),
                                        shard=%out_shard.display(),
                                        error=%error,
                                        "partial aggregate input read; merging partial shard by policy"
                                    );
                                    ShardBuildResult::Partial {
                                        input: input.clone(),
                                        error,
                                        shard: Some(out_shard.clone()),
                                    }
                                } else {
                                    ShardBuildResult::Clean {
                                        input: input.clone(),
                                        shard: out_shard.clone(),
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    input=%input.display(),
                                    shard=%out_shard.display(),
                                    error=%e,
                                    "failed building aggregate shard"
                                );
                                ShardBuildResult::Fatal {
                                    input: input.clone(),
                                    error: e.to_string(),
                                }
                            }
                        }
                    }
                }
            };

            if let Some(pb) = &pb_build {
                pb.inc(1);
            }
            outcome
        })
        .collect();

    if let Some(pb) = pb_build {
        pb.finish_with_message("Aggregate: shard build done");
    }

    let mut shards = Vec::new();
    let mut report = AggregateBuildReport::default();
    for outcome in outcomes {
        match outcome {
            ShardBuildResult::Clean { input, shard } => {
                report.ok_inputs.push(input);
                shards.push(shard);
            }
            ShardBuildResult::Partial {
                input,
                error,
                shard,
            } => {
                if let Some(shard) = shard {
                    shards.push(shard);
                }
                report
                    .partial_inputs
                    .push(AggregateInputIssue::new(&input, error));
            }
            ShardBuildResult::Fatal { input, error } => {
                report
                    .fatal_inputs
                    .push(AggregateInputIssue::new(&input, error));
            }
        }
    }
    report.merged_shards = shards.len();

    BuildOutcome { shards, report }
}
