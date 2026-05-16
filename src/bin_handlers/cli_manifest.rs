
fn counts_map(entries: &[(&str, u64)]) -> BTreeMap<String, u64> {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_string(), *value))
        .collect()
}

fn normalize_cli_values(values: &[String], strip_subreddit_prefix: bool) -> Vec<String> {
    let mut out: Vec<String> = values
        .iter()
        .filter_map(|value| {
            let mut normalized = value.trim().to_lowercase();
            if strip_subreddit_prefix {
                if let Some(rest) = normalized.strip_prefix("r/") {
                    normalized = rest.to_string();
                }
            }
            (!normalized.is_empty()).then_some(normalized)
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

fn source_arg_manifest_label(source: SourceArg) -> &'static str {
    match source {
        SourceArg::Rc => "comments",
        SourceArg::Rs => "submissions",
        SourceArg::Both => "both",
    }
}

fn file_kind_manifest_label(kind: FileKind) -> &'static str {
    match kind {
        FileKind::Comment => "comment",
        FileKind::Submission => "submission",
    }
}

fn cli_selected_file_identities(common: &CommonOpts) -> Result<Vec<FileIdentity>> {
    let comments_dir = common.data_dir.join("comments");
    let submissions_dir = common.data_dir.join("submissions");
    let discovered = discover_sources_checked(
        &comments_dir,
        &submissions_dir,
        Sources::from(common.source),
    )?;
    let jobs = plan_files_checked(
        &discovered,
        &comments_dir,
        &submissions_dir,
        Sources::from(common.source),
        common.start,
        common.end,
    )?;
    let mut identities: Vec<FileIdentity> = jobs
        .iter()
        .map(|job| {
            let mut identity = file_identity(&job.path);
            identity.kind = Some(file_kind_manifest_label(job.kind).to_string());
            identity.month = Some(job.ym.to_string());
            identity
        })
        .collect();
    identities.sort_by(|a, b| {
        a.kind
            .cmp(&b.kind)
            .then_with(|| a.month.cmp(&b.month))
            .then_with(|| a.path.cmp(&b.path))
    });
    Ok(identities)
}

fn cli_corpus_snapshot(common: &CommonOpts) -> Result<CorpusSnapshot> {
    Ok(CorpusSnapshot {
        base_dir: Some(path_to_stable_string(&common.data_dir)),
        comments_dir: Some(path_to_stable_string(&common.data_dir.join("comments"))),
        submissions_dir: Some(path_to_stable_string(&common.data_dir.join("submissions"))),
        sources: Some(source_arg_manifest_label(common.source).to_string()),
        selected_files: cli_selected_file_identities(common)?,
    })
}

fn cli_query_value(common: &CommonOpts, query: &QueryOpts) -> Value {
    serde_json::json!({
        "subreddits": normalize_cli_values(&common.subreddits, true),
        "authors_in": normalize_cli_values(&query.authors, false),
        "authors_out": normalize_cli_values(&query.exclude_authors, false),
        "exclude_common_bots": query.exclude_common_bots,
        "author_regex": query.author_regex.as_deref(),
        "min_score": query.min_score,
        "max_score": query.max_score,
        "keywords_any": normalize_cli_values(&query.keywords, false),
        "domains_in": normalize_cli_values(&query.domains, false),
        "contains_url": query.contains_url.then_some(true),
        "json_predicates": &query.json_predicates,
        "filter_pseudo_users": !common.include_deleted,
    })
}

fn cli_common_options_value(common: &CommonOpts, extra: Value) -> Value {
    serde_json::json!({
        "data_dir": path_to_stable_string(&common.data_dir),
        "work_dir": path_to_stable_string(&common.work_dir),
        "start": common.start.map(|ym| ym.to_string()),
        "end": common.end.map(|ym| ym.to_string()),
        "source": source_arg_manifest_label(common.source),
        "parallelism": common.parallelism,
        "file_concurrency": common.file_concurrency,
        "progress": !common.no_progress,
        "allow_partial": common.allow_partial,
        "emit_manifest": !common.no_manifest,
        "extra": extra,
    })
}

#[allow(clippy::too_many_arguments)]
fn write_cli_scan_manifest_for_file(
    start: RunManifestStart,
    operation: &str,
    command: &str,
    common: &CommonOpts,
    query: &QueryOpts,
    out_path: &Path,
    output_format: &str,
    counts: BTreeMap<String, u64>,
    partial_reporter: &retl::PartialReadReporter,
    extra_options: Value,
) -> Result<()> {
    if common.no_manifest || out_path == Path::new("-") {
        return Ok(());
    }
    let mut manifest = RunManifestInput::new(operation);
    manifest.start = start;
    manifest.command = Some(command.to_string());
    manifest.query = cli_query_value(common, query);
    manifest.options = cli_common_options_value(common, extra_options);
    manifest.corpus = cli_corpus_snapshot(common)?;
    manifest.output_format = output_format.to_string();
    manifest.counts = counts;
    manifest.partial_read = partial_reporter.snapshot();
    write_run_manifest(manifest, ManifestDestination::File(out_path.to_path_buf()))?;
    Ok(())
}
