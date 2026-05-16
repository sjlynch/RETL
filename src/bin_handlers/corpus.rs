
#[derive(Serialize)]
struct CorpusPlanDocument {
    manifest_version: u32,
    manifest_name: Option<String>,
    source: String,
    start: YearMonth,
    end: YearMonth,
    dest: PathBuf,
    summary: CorpusPlanSummary,
    items: Vec<CorpusPlanItem>,
    next_steps: Vec<String>,
}

#[derive(Default, Clone, Debug, Serialize)]
struct CorpusPlanSummary {
    total_items: usize,
    available_items: usize,
    unavailable_items: usize,
    local_present: usize,
    local_missing: usize,
    local_inaccessible: usize,
    size_mismatches: usize,
    checksum_mismatches: usize,
    known_expected_compressed_bytes: u64,
}

impl CorpusPlanSummary {
    fn from_items(items: &[CorpusPlanItem]) -> Self {
        let mut summary = Self {
            total_items: items.len(),
            ..Self::default()
        };
        for item in items {
            match item.availability {
                CorpusAvailability::Available => summary.available_items += 1,
                CorpusAvailability::Unavailable => summary.unavailable_items += 1,
            }
            if item.availability == CorpusAvailability::Available {
                if let Some(bytes) = item.compressed_bytes {
                    summary.known_expected_compressed_bytes = summary
                        .known_expected_compressed_bytes
                        .saturating_add(bytes);
                }
            }
            match &item.local {
                CorpusLocalStatus::Missing => summary.local_missing += 1,
                CorpusLocalStatus::Inaccessible { .. } => summary.local_inaccessible += 1,
                CorpusLocalStatus::Present {
                    size_matches,
                    sha256_matches,
                    ..
                } => {
                    summary.local_present += 1;
                    if matches!(size_matches, Some(false)) {
                        summary.size_mismatches += 1;
                    }
                    if matches!(sha256_matches, Some(false)) {
                        summary.checksum_mismatches += 1;
                    }
                }
            }
        }
        summary
    }
}

pub(crate) fn run_corpus(args: CorpusArgs) -> Result<()> {
    match args.command {
        CorpusCommand::Plan(plan) => run_corpus_plan(plan),
        CorpusCommand::Manifest(manifest) => run_corpus_manifest(manifest),
    }
}

fn run_corpus_plan(args: CorpusPlanArgs) -> Result<()> {
    let manifest = load_corpus_manifest(args.manifest.as_deref())?;
    let sources = Sources::from(args.source);
    let mut items = manifest
        .plan(
            sources,
            args.start,
            args.end,
            &args.dest,
            args.verify_checksums,
        )
        .with_context(|| "building corpus acquisition plan")?;
    if args.only_missing {
        items.retain(CorpusPlanItem::needs_download);
    }
    let summary = CorpusPlanSummary::from_items(&items);

    match args.format {
        CorpusPlanFmt::Json => {
            let doc = CorpusPlanDocument {
                manifest_version: manifest.version,
                manifest_name: manifest.name.clone(),
                source: args.source.label().to_string(),
                start: args.start,
                end: args.end,
                dest: args.dest.clone(),
                summary,
                items,
                next_steps: corpus_plan_next_steps(&args),
            };
            write_corpus_plan_json(&args.out, &doc)?;
        }
        CorpusPlanFmt::Tsv => write_corpus_plan_tsv(&args.out, &items)?,
    }
    Ok(())
}

fn run_corpus_manifest(args: CorpusManifestArgs) -> Result<()> {
    write_text_or_stdout(&args.out, |w| {
        w.write_all(CorpusManifest::builtin_json().as_bytes())?;
        Ok(())
    })
}

fn load_corpus_manifest(path: Option<&Path>) -> Result<CorpusManifest> {
    match path {
        Some(path) => {
            let file = retl::open_with_default_backoff(path)
                .with_context(|| format!("opening corpus manifest {}", path.display()))?;
            let reader = BufReader::new(file);
            CorpusManifest::from_reader(reader)
                .with_context(|| format!("parsing corpus manifest {}", path.display()))
        }
        None => CorpusManifest::builtin().with_context(|| "parsing built-in corpus manifest"),
    }
}

fn write_text_or_stdout<T, F>(out: &Path, body: F) -> Result<T>
where
    F: FnOnce(&mut dyn Write) -> Result<T>,
{
    if out == Path::new("-") {
        let stdout = io::stdout();
        let mut w = BufWriter::new(stdout.lock());
        let result = body(&mut w)?;
        w.flush()?;
        Ok(result)
    } else {
        write_text_file_atomic(out, body)
    }
}

fn write_corpus_plan_json(out: &Path, doc: &CorpusPlanDocument) -> Result<()> {
    write_text_or_stdout(out, |w| {
        serde_json::to_writer_pretty(&mut *w, doc)?;
        writeln!(w)?;
        Ok(())
    })
}

fn write_corpus_plan_tsv(out: &Path, items: &[CorpusPlanItem]) -> Result<()> {
    write_text_or_stdout(out, |w| {
        writeln!(
            w,
            "source\tmonth\tavailability\tlocal_status\tfile_name\texpected_path\tcompressed_bytes\tactual_bytes\tsize_matches\tsha256\tsha256_matches\turl\ttorrent\tnote"
        )?;
        for item in items {
            let (local_status, actual_bytes, size_matches, sha256_matches) =
                local_status_cells(&item.local);
            writeln!(
                w,
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                item.source.label(),
                item.month,
                availability_cell(item.availability),
                local_status,
                tsv_cell(&item.file_name),
                tsv_cell(&item.expected_path.display().to_string()),
                opt_u64_cell(item.compressed_bytes),
                actual_bytes.map(|n| n.to_string()).unwrap_or_default(),
                opt_bool_cell(size_matches),
                tsv_cell(item.sha256.as_deref().unwrap_or("")),
                opt_bool_cell(sha256_matches),
                tsv_cell(item.url.as_deref().unwrap_or("")),
                tsv_cell(item.torrent.as_deref().unwrap_or("")),
                tsv_cell(item.note.as_deref().unwrap_or("")),
            )?;
        }
        Ok(())
    })
}

fn corpus_plan_next_steps(args: &CorpusPlanArgs) -> Vec<String> {
    let mut steps = vec![
        "Download each item with availability=available and local.status=missing to expected_path. RETL does not yet perform direct downloads.".to_string(),
        format!(
            "After downloading, run: retl describe --expected --data-dir {} --source {} --start {} --end {}",
            args.dest.display(),
            args.source.label(),
            args.start,
            args.end
        ),
        format!(
            "Then validate zstd payloads: retl integrity --expected --mode full --data-dir {} --source {} --start {} --end {}",
            args.dest.display(),
            args.source.label(),
            args.start,
            args.end
        ),
    ];
    if args.manifest.is_some() {
        steps.push(
            "Pass the same --manifest path to describe/integrity when you want RETL to use custom sizes or checksums.".to_string(),
        );
    }
    steps
}

fn local_status_cells(
    local: &CorpusLocalStatus,
) -> (&'static str, Option<u64>, Option<bool>, Option<bool>) {
    match local {
        CorpusLocalStatus::Missing => ("missing", None, None, None),
        CorpusLocalStatus::Inaccessible { .. } => ("inaccessible", None, None, None),
        CorpusLocalStatus::Present {
            actual_bytes,
            size_matches,
            sha256_matches,
            ..
        } => (
            "present",
            Some(*actual_bytes),
            *size_matches,
            *sha256_matches,
        ),
    }
}

fn availability_cell(availability: CorpusAvailability) -> &'static str {
    match availability {
        CorpusAvailability::Available => "available",
        CorpusAvailability::Unavailable => "unavailable",
    }
}

fn opt_u64_cell(n: Option<u64>) -> String {
    n.map(|n| n.to_string()).unwrap_or_default()
}

fn opt_bool_cell(v: Option<bool>) -> &'static str {
    match v {
        Some(true) => "true",
        Some(false) => "false",
        None => "",
    }
}

fn tsv_cell(raw: &str) -> String {
    raw.chars()
        .map(|c| match c {
            '\t' | '\r' | '\n' => ' ',
            other => other,
        })
        .collect()
}

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
