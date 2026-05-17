
pub(crate) fn run_aggregate(args: AggregateArgs) -> Result<()> {
    if args.resume {
        anyhow::bail!(
            "retl aggregate does not support --resume because it reduces existing JSONL inputs. For resumable corpus filtering, run `retl export --format spool --resume ...` first, then aggregate with `retl aggregate --spool <DIR>`."
        );
    }
    let manifest_start = RunManifestStart::now();
    let mut etl = RedditETL::new()
        .progress(!args.runtime.no_progress)
        .run_manifest(!args.runtime.no_manifest);
    if let Some(p) = args.runtime.parallelism {
        etl = etl.parallelism(p);
    }

    let inputs = resolve_aggregate_inputs(&args)?;
    let manifest_inputs = inputs.clone();
    let input_count = inputs.len();

    let shards_dir = args.shards_dir.clone().unwrap_or_else(|| {
        args.out
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join("agg_shards")
    });
    retl::create_dir_all_with_default_backoff(&shards_dir)
        .with_context(|| format!("creating shards_dir {}", shards_dir.display()))?;
    if let Some(by) = args.by.clone() {
        let group_by = GroupBySpec::parse(&by)?;
        let metric = MetricSpec::parse(args.metric.as_deref())?;
        let top = args.top;
        let (agg, report) = etl.aggregate_jsonls_parallel_collect_with::<GroupMetricAgg, _>(
            inputs,
            &shards_dir,
            || GroupMetricAgg::new(group_by.clone(), metric.clone()),
        )?;
        report_aggregate_input_issues(&report);
        ensure_aggregate_inputs_succeeded(input_count, &report)?;
        let rows = if args.scientific {
            agg.rows_with_scientific(top, true)
        } else {
            agg.rows(top)
        };
        write_grouped_tsv(&args.out, rows)?;
        let mut counts = counts_map(&[
            ("input_files", input_count as u64),
            ("ok_inputs", report.ok_inputs.len() as u64),
            ("partial_inputs", report.partial_count() as u64),
            ("fatal_inputs", report.fatal_count() as u64),
            ("merged_shards", report.merged_shards as u64),
            ("output_rows", agg.rows(None).len() as u64),
        ]);
        if let Some(top) = args.top {
            counts.insert("top".to_string(), top as u64);
        }
        let warnings = aggregate_manifest_warnings(&report);
        write_cli_aggregate_manifest(
            manifest_start,
            &args,
            &shards_dir,
            &manifest_inputs,
            "tsv",
            counts,
            warnings,
        )?;
        eprintln!(
            "Aggregated {} shard(s) to TSV; {} input(s) failed during shard build; {} input(s) skipped after partial read",
            report.merged_shards,
            report.fatal_count(),
            report.partial_count()
        );
    } else {
        if args.metric.is_some() {
            anyhow::bail!("--metric requires --by");
        }
        if args.top.is_some() {
            anyhow::bail!("--top requires --by");
        }
        let (agg, report) =
            etl.aggregate_jsonls_parallel_collect::<RecCount>(inputs, &shards_dir)?;
        report_aggregate_input_issues(&report);
        ensure_aggregate_inputs_succeeded(input_count, &report)?;
        write_rec_count_json(&args.out, &agg, args.pretty)?;
        let counts = counts_map(&[
            ("input_files", input_count as u64),
            ("ok_inputs", report.ok_inputs.len() as u64),
            ("partial_inputs", report.partial_count() as u64),
            ("fatal_inputs", report.fatal_count() as u64),
            ("merged_shards", report.merged_shards as u64),
            ("records_aggregated", agg.count),
        ]);
        let warnings = aggregate_manifest_warnings(&report);
        write_cli_aggregate_manifest(
            manifest_start,
            &args,
            &shards_dir,
            &manifest_inputs,
            "json",
            counts,
            warnings,
        )?;
        eprintln!(
            "Aggregated {} shard(s); {} input(s) failed during shard build; {} input(s) skipped after partial read",
            report.merged_shards,
            report.fatal_count(),
            report.partial_count()
        );
    }
    Ok(())
}

fn resolve_aggregate_inputs(args: &AggregateArgs) -> Result<Vec<PathBuf>> {
    if args.spool.is_some() && !args.inputs.is_empty() {
        anyhow::bail!(
            "retl aggregate accepts either --spool <DIR> or explicit JSONL input files, not both"
        );
    }

    if let Some(spool) = &args.spool {
        let (parts, _, _) = discover_spool_parts(spool).with_context(|| {
            format!(
                "discovering spool parts for aggregate in {}; use a directory produced by `retl export --format spool --out {}` or pass explicit JSONL files",
                spool.display(),
                spool.display()
            )
        })?;
        return Ok(parts);
    }

    if args.inputs.is_empty() {
        anyhow::bail!(
            "retl aggregate requires --spool <DIR> or one or more explicit JSONL input files"
        );
    }

    reject_unexpanded_aggregate_globs(&args.inputs)?;
    Ok(args.inputs.clone())
}

fn reject_unexpanded_aggregate_globs(inputs: &[PathBuf]) -> Result<()> {
    for input in inputs {
        if path_contains_glob_meta(input) && !input.exists() {
            let suggested_spool = input
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            anyhow::bail!(
                "aggregate input {} looks like an unexpanded glob. RETL does not expand globs itself; use `retl aggregate --spool {}` for spool directories, or pass explicit JSONL file paths.",
                input.display(),
                suggested_spool.display()
            );
        }
    }
    Ok(())
}

fn path_contains_glob_meta(path: &Path) -> bool {
    path.as_os_str()
        .to_string_lossy()
        .chars()
        .any(|c| matches!(c, '*' | '?' | '[' | ']'))
}

fn aggregate_manifest_warnings(report: &AggregateBuildReport) -> Vec<String> {
    let mut warnings = Vec::new();
    if report.fatal_count() > 0 {
        warnings.push(format!(
            "{} aggregate input(s) failed during shard build",
            report.fatal_count()
        ));
    }
    if report.partial_count() > 0 {
        warnings.push(format!(
            "{} aggregate input(s) were skipped after partial read",
            report.partial_count()
        ));
    }
    warnings
}

fn report_aggregate_input_issues(report: &AggregateBuildReport) {
    if !report.fatal_inputs.is_empty() {
        eprintln!("Aggregate input(s) failed during shard build:");
        for issue in &report.fatal_inputs {
            eprintln!("  {}\t{}", issue.input.display(), issue.error);
        }
    }
    if !report.partial_inputs.is_empty() {
        eprintln!("Aggregate input(s) skipped after partial read:");
        for issue in &report.partial_inputs {
            eprintln!("  {}\t{}", issue.input.display(), issue.error);
        }
    }
}

fn ensure_aggregate_inputs_succeeded(
    input_count: usize,
    report: &AggregateBuildReport,
) -> Result<()> {
    let errors = report.problem_count();
    if input_count > 0 && (errors == input_count || report.merged_shards == 0) {
        anyhow::bail!(
            "aggregate failed: {errors} of {input_count} input(s) failed or were partial; {} shard(s) merged",
            report.merged_shards
        );
    }
    Ok(())
}

fn write_rec_count_json(out: &Path, agg: &RecCount, pretty: bool) -> Result<()> {
    write_text_atomic(out, CLI_TEXT_WRITE_BUF_BYTES, |w| {
        if pretty {
            serde_json::to_writer_pretty(w, agg)?;
        } else {
            serde_json::to_writer(w, agg)?;
        }
        Ok(())
    })
    .with_context(|| format!("publishing output file {}", out.display()))
}

fn write_grouped_tsv(out: &Path, rows: Vec<(String, String)>) -> Result<()> {
    write_text_atomic(out, CLI_TEXT_WRITE_BUF_BYTES, |w| {
        for (key, value) in rows {
            writeln!(w, "{key}\t{value}")?;
        }
        Ok(())
    })
    .with_context(|| format!("publishing output file {}", out.display()))
}

#[allow(clippy::too_many_arguments)]
fn write_cli_aggregate_manifest(
    start: RunManifestStart,
    args: &AggregateArgs,
    shards_dir: &Path,
    inputs: &[PathBuf],
    output_format: &str,
    counts: BTreeMap<String, u64>,
    warnings: Vec<String>,
) -> Result<()> {
    if args.runtime.no_manifest {
        return Ok(());
    }
    let mut manifest = RunManifestInput::new("cli.aggregate");
    manifest.start = start;
    manifest.command = Some("retl aggregate".to_string());
    manifest.options = serde_json::json!({
        "spool": args.spool.as_ref().map(|p| path_to_stable_string(p)),
        "explicit_inputs": args.inputs.iter().map(|p| path_to_stable_string(p)).collect::<Vec<_>>(),
        "shards_dir": path_to_stable_string(shards_dir),
        "pretty": args.pretty,
        "by": args.by.as_deref(),
        "metric": args.metric.as_deref(),
        "top": args.top,
        "scientific": args.scientific,
        "parallelism": args.runtime.parallelism,
        "progress": !args.runtime.no_progress,
        "emit_manifest": !args.runtime.no_manifest,
    });
    manifest.inputs = file_identities(inputs);
    manifest.output_format = output_format.to_string();
    manifest.counts = counts;
    manifest.warnings = warnings;
    manifest.upstream_manifests = discover_upstream_manifests_from_inputs(inputs);
    write_run_manifest(manifest, ManifestDestination::File(args.out.clone()))?;
    Ok(())
}
