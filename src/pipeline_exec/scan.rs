
fn log_pseudo_user_filter(query: &QuerySpec) {
    if query.filter_pseudo_users {
        tracing::info!(
            "Excluding pseudo-users ([deleted], [removed], empty). Pass --include-deleted to keep them."
        );
    }
}

/// Result of a single month's atomic publish — used by the orchestration
/// shell in `extract_spool_monthly` to update both the returned `parts` list
/// and the on-disk progress manifest.
struct MonthResult {
    out_path: PathBuf,
    key: String,
    lines: u64,
}

struct ScanCheckpoint {
    parts: Vec<PathBuf>,
    matched_records: u64,
}

fn effective_plan_range(
    etl: &RedditETL,
    query: Option<&QuerySpec>,
) -> (Option<YearMonth>, Option<YearMonth>) {
    let mut start = etl.opts.start;
    let mut end = etl.opts.end;

    if let Some(bounds) = query.map(|q| q.timestamp_bounds) {
        if start.is_none() {
            start = bounds.derived_start_month();
        }
        if end.is_none() {
            end = bounds.derived_end_month();
        }
    }

    (start, end)
}

fn plan_pipeline_files(etl: &RedditETL, query: Option<&QuerySpec>) -> Result<Vec<FileJob>> {
    if let Some(err) = etl.opts.build_error.clone() {
        return Err(err.into());
    }
    let discovered = discover_sources_checked(
        &etl.opts.comments_dir,
        &etl.opts.submissions_dir,
        etl.opts.sources,
    )?;
    let (start, end) = effective_plan_range(etl, query);
    let jobs = plan_files_checked(
        &discovered,
        &etl.opts.comments_dir,
        &etl.opts.submissions_dir,
        etl.opts.sources,
        start,
        end,
    )?;
    log_missing_month_warnings(&discovered, etl.opts.sources, start, end);
    Ok(jobs)
}

fn warn_if_unfiltered_undated_query(etl: &RedditETL, query: &QuerySpec, files: &[FileJob]) {
    if etl.opts.start.is_some()
        || etl.opts.end.is_some()
        || etl.opts.subreddit.is_some()
        || query.has_selective_filters()
    {
        return;
    }

    let file_count = files.len();
    let compressed_bytes = total_compressed_size(files);
    tracing::warn!(
        files = file_count,
        compressed_bytes = compressed_bytes,
        "running an unfiltered, undated query over the full corpus (files={file_count}, compressed_bytes={compressed_bytes}); pass --subreddit and/or --start/--end to narrow the scope"
    );
}

fn manifest_counts(entries: &[(&str, u64)]) -> BTreeMap<String, u64> {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_string(), *value))
        .collect()
}

fn scan_manifest_input(
    start: RunManifestStart,
    operation: &str,
    format: &str,
    etl: &RedditETL,
    query: &QuerySpec,
    files: &[FileJob],
    limit: Option<u64>,
    counts: BTreeMap<String, u64>,
    checkpoint_fingerprint: Option<String>,
    checkpoint_path: Option<PathBuf>,
    extra_options: Value,
) -> RunManifestInput {
    let mut input = RunManifestInput::new(operation);
    input.start = start;
    input.api_operation = Some(operation.to_string());
    input.query = scan_query_value(query, limit);
    input.options = etl_options_value(&etl.opts, limit, extra_options);
    input.corpus = corpus_snapshot_from_etl(&etl.opts, files);
    input.output_format = format.to_string();
    input.counts = counts;
    input.partial_read = etl.opts.partial_read_reporter.snapshot();
    input.resume_enabled = etl.opts.resume;
    input.checkpoint_fingerprint = checkpoint_fingerprint;
    input.checkpoint_path = checkpoint_path;
    input
}

fn validate_export_whitelist(etl: &RedditETL) -> Result<()> {
    if matches!(&etl.opts.whitelist_fields, Some(fields) if fields.is_empty()) {
        anyhow::bail!("--whitelist must include at least one non-empty field");
    }
    Ok(())
}
