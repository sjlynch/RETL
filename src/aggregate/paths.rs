
fn aggregate_run_token() -> String {
    let counter = AGGREGATE_RUN_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("p{}_r{counter}_t{nanos}", std::process::id())
}

fn aggregate_run_dir(shards_dir: &Path, run_token: &str) -> PathBuf {
    shards_dir.join(format!("run_{run_token}"))
}

fn shard_name_for_input(run_dir: &Path, run_token: &str, index: usize, input: &Path) -> PathBuf {
    let stem = input.file_stem().and_then(|s| s.to_str()).unwrap_or("part");
    // Common case: part_YYYY-MM. Keep the human-readable stem, but prefix it
    // with the run token and input index so same-basename inputs from different
    // directories are distinct. The enclosing per-run directory isolates live
    // artifacts from other aggregate jobs sharing the same caller-provided
    // shards_dir.
    let stem = stem.strip_prefix("part_").unwrap_or(stem);
    run_dir.join(format!("agg_{run_token}_{index:06}_{stem}.json"))
}

fn shard_names_for_inputs(run_dir: &Path, run_token: &str, inputs: &[PathBuf]) -> Vec<PathBuf> {
    inputs
        .iter()
        .enumerate()
        .map(|(index, input)| shard_name_for_input(run_dir, run_token, index, input))
        .collect()
}
