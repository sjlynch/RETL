
pub(crate) fn run_aggregate(args: AggregateArgs) -> Result<()> {
    if args.resume {
        anyhow::bail!(
            "retl aggregate does not support --resume because it reduces existing JSONL inputs. For resumable corpus filtering, run `retl export --format spool --resume ...` first, then aggregate with `retl aggregate --spool <DIR>`."
        );
    }
    let uses_expr_dsl = args.group_by.is_some() || args.agg.is_some();
    if uses_expr_dsl && (args.by.is_some() || args.metric.is_some()) {
        anyhow::bail!(
            "--group-by/--agg are the generalized DSL and cannot be combined with the legacy --by/--metric flags. Pick one surface for this run."
        );
    }
    if matches!(args.format, AggregateFmt::Jsonl) && !uses_expr_dsl {
        anyhow::bail!(
            "--format jsonl is only valid alongside --group-by/--agg (the generalized DSL); drop --format jsonl or supply --group-by/--agg"
        );
    }
    if matches!(args.format, AggregateFmt::Parquet) && args.by.is_none() {
        anyhow::bail!(
            "--format parquet requires --by (the ungrouped record-count output has no row schema). Re-run with `--by <field>` to write a two-column parquet rollup, or drop `--format parquet`."
        );
    }
    let is_parquet = matches!(args.format, AggregateFmt::Parquet);
    if args.parquet_row_group_size.is_some() && !is_parquet {
        anyhow::bail!(
            "--parquet-row-group-size only applies to --format parquet; drop it or pass --format parquet"
        );
    }
    if args.parquet_compression.is_some() && !is_parquet {
        anyhow::bail!(
            "--parquet-compression only applies to --format parquet; drop it or pass --format parquet"
        );
    }
    if is_parquet && args.out == Path::new("-") {
        anyhow::bail!(
            "--format parquet requires an explicit --out <FILE>; parquet is a binary container and cannot stream to stdout"
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
    if uses_expr_dsl {
        run_aggregate_expr(args, etl, inputs, manifest_inputs, input_count, &shards_dir, manifest_start)?;
        return Ok(());
    }

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
        ensure_aggregate_inputs_succeeded(input_count, &report, args.strict)?;
        let rows = if args.scientific {
            agg.rows_with_scientific(top, true)
        } else {
            agg.rows(top)
        };
        let output_format = match args.format {
            AggregateFmt::Tsv => {
                write_grouped_tsv(&args.out, rows)?;
                "tsv"
            }
            AggregateFmt::Parquet => {
                let row_group_size = args
                    .parquet_row_group_size
                    .unwrap_or(retl::DEFAULT_PARQUET_ROW_GROUP_SIZE);
                let compression = args
                    .parquet_compression
                    .clone()
                    .unwrap_or_else(|| retl::DEFAULT_PARQUET_COMPRESSION.to_string());
                write_grouped_parquet(&args.out, rows, row_group_size, &compression)?;
                "parquet"
            }
            AggregateFmt::Jsonl => {
                // Rejected upstream with a clearer message for the
                // `--by`/`--metric` rollup; this branch is unreachable but
                // exhaustive matching demands it.
                anyhow::bail!(
                    "--format jsonl requires --group-by/--agg, not --by/--metric"
                );
            }
        };
        let mut counts = counts_map(&[
            ("input_files", input_count as u64),
            ("ok_inputs", report.ok_inputs.len() as u64),
            ("partial_inputs", report.partial_count() as u64),
            ("fatal_inputs", report.fatal_count() as u64),
            ("merged_shards", report.merged_shards as u64),
            ("output_rows", agg.rows(None).len() as u64),
            ("records_ingested", agg.records_ingested()),
            (
                "records_skipped_no_group_key",
                agg.records_skipped_no_group_key(),
            ),
            (
                "records_skipped_no_metric_value",
                agg.records_skipped_no_metric_value(),
            ),
        ]);
        if let Some(top) = args.top {
            counts.insert("top".to_string(), top as u64);
        }
        let mut warnings = aggregate_manifest_warnings(&report);
        warnings.extend(warn_aggregate_record_skips(&by, args.metric.as_deref(), &agg));
        write_cli_aggregate_manifest(
            manifest_start,
            &args,
            &shards_dir,
            &manifest_inputs,
            output_format,
            counts,
            warnings,
        )?;
        eprintln!(
            "Aggregated {} shard(s) to {}; {} input(s) failed during shard build; {} input(s) skipped after partial read",
            report.merged_shards,
            output_format.to_uppercase(),
            report.fatal_count(),
            report.partial_count()
        );
        report_aggregate_record_skips(&agg);
    } else {
        if args.metric.is_some() {
            anyhow::bail!("--metric requires --by");
        }
        if args.top.is_some() {
            anyhow::bail!("--top requires --by");
        }
        if args.scientific {
            anyhow::bail!("--scientific requires --by");
        }
        let (agg, report) =
            etl.aggregate_jsonls_parallel_collect::<RecCount>(inputs, &shards_dir)?;
        report_aggregate_input_issues(&report);
        ensure_aggregate_inputs_succeeded(input_count, &report, args.strict)?;
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

/// Warn (and record a manifest warning) once more than this fraction of
/// ingested records is skipped for a missing group key or metric value.
/// Mirrors `dedupe`'s `DEDUPE_KEY_DROP_WARN_RATE`.
const AGGREGATE_RECORD_SKIP_WARN_RATE: f64 = 0.01;

/// Emit a `tracing::warn!` for each record-skip class whose share of
/// ingested records exceeds [`AGGREGATE_RECORD_SKIP_WARN_RATE`], and return
/// the same set as manifest warning strings. A typo like
/// `--metric sum:/scoer` otherwise produces an all-zero TSV with zero
/// diagnostics.
fn warn_aggregate_record_skips(
    by: &str,
    metric: Option<&str>,
    agg: &GroupMetricAgg,
) -> Vec<String> {
    let ingested = agg.records_ingested();
    let mut warnings = Vec::new();
    if ingested == 0 {
        return warnings;
    }

    let no_key = agg.records_skipped_no_group_key();
    let no_key_rate = no_key as f64 / ingested as f64;
    if no_key > 0 && no_key_rate > AGGREGATE_RECORD_SKIP_WARN_RATE {
        tracing::warn!(
            by = %by,
            records_ingested = ingested,
            records_skipped_no_group_key = no_key,
            skip_rate = no_key_rate,
            "aggregate skipped {} of {} record(s) with no --by '{}' value ({:.2}%); check the --by field name",
            no_key,
            ingested,
            by,
            no_key_rate * 100.0,
        );
        warnings.push(format!(
            "{no_key} of {ingested} record(s) skipped: no --by '{by}' value"
        ));
    }

    let no_metric = agg.records_skipped_no_metric_value();
    let no_metric_rate = no_metric as f64 / ingested as f64;
    if no_metric > 0 && no_metric_rate > AGGREGATE_RECORD_SKIP_WARN_RATE {
        let metric_label = metric.unwrap_or("count");
        tracing::warn!(
            metric = %metric_label,
            records_ingested = ingested,
            records_skipped_no_metric_value = no_metric,
            skip_rate = no_metric_rate,
            "aggregate skipped {} of {} record(s) with a missing or non-numeric --metric value ({:.2}%); check the --metric pointer",
            no_metric,
            ingested,
            no_metric_rate * 100.0,
        );
        warnings.push(format!(
            "{no_metric} of {ingested} record(s) skipped: missing or non-numeric --metric '{metric_label}' value"
        ));
    }

    warnings
}

/// Print the per-record skip counts to stderr whenever any record was
/// dropped, so the CLI summary reflects record-level losses and not just
/// input-file-level fail/partial counts.
fn report_aggregate_record_skips(agg: &GroupMetricAgg) {
    let no_key = agg.records_skipped_no_group_key();
    let no_metric = agg.records_skipped_no_metric_value();
    if no_key == 0 && no_metric == 0 {
        return;
    }
    eprintln!(
        "  {} record(s) skipped without a --by group key, {} record(s) skipped without a numeric --metric value (of {} ingested)",
        no_key,
        no_metric,
        agg.records_ingested(),
    );
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
    // A run that merged shards but ingested zero records produces an empty
    // aggregation that otherwise looks successful. The library already emits a
    // `tracing::warn!`; record it in the manifest too so a scripted pipeline
    // reading the sidecar sees the empty result.
    if report.ingested_zero_records() {
        warnings.push(format!(
            "aggregate merged {} shard(s) but ingested 0 record(s); the result is empty",
            report.merged_shards
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
    strict: bool,
) -> Result<()> {
    let errors = report.problem_count();
    if input_count > 0 && (errors == input_count || report.merged_shards == 0) {
        anyhow::bail!(
            "aggregate failed: {errors} of {input_count} input(s) failed or were partial; {} shard(s) merged",
            report.merged_shards
        );
    }
    // `--strict`: any fatal input fails the whole run with a non-zero exit and
    // no output written, so a mistyped path in a multi-file batch can't slip
    // through as a successful partial result. The per-input diagnostics were
    // already printed by `report_aggregate_input_issues`.
    if strict && report.fatal_count() > 0 {
        anyhow::bail!(
            "aggregate --strict: {} of {input_count} input(s) failed during shard build; aborting with no output written",
            report.fatal_count()
        );
    }
    Ok(())
}

fn write_rec_count_json(out: &Path, agg: &RecCount, pretty: bool) -> Result<()> {
    // `write_text_or_stdout` honors `--out -` (stream to stdout) and otherwise
    // routes through `write_text_atomic`, so `retl aggregate --out -` streams
    // the JSON instead of staging-and-renaming a file literally named `-`.
    write_text_or_stdout(out, |w| {
        if pretty {
            serde_json::to_writer_pretty(w, agg)?;
        } else {
            serde_json::to_writer(w, agg)?;
        }
        Ok(())
    })
    .with_context(|| format!("publishing output file {}", out.display()))
}

/// Write a two-column (`key`, `value`) Parquet rollup of `rows` to `out`.
/// Atomically published through [`retl::parquet_writer::write_kv_rows_atomic`]
/// so a crashed run never leaves a partial `.parquet` at the published path.
///
/// The `key` and `value` columns are STRING because grouped values can be
/// integer, decimal, or scientific-notation depending on `--scientific`,
/// matching the TSV path's behavior; downstream readers can `CAST` as
/// needed.
fn write_grouped_parquet(
    out: &Path,
    rows: Vec<(String, String)>,
    row_group_size: usize,
    compression: &str,
) -> Result<()> {
    let parent = out
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    retl::create_dir_all_with_default_backoff(&parent)
        .with_context(|| format!("creating parquet output parent {}", parent.display()))?;
    // `write_kv_rows_atomic` derives its own staging dir from `parent`.
    let staging = parent.join(retl::STAGING_DIR_NAME);
    retl::parquet_writer::write_kv_rows_atomic(
        &staging,
        out,
        row_group_size,
        compression,
        64 * 1024,
        rows.into_iter(),
    )
    .map(|_| ())
    .with_context(|| format!("publishing aggregate parquet {}", out.display()))
}

/// Generalized GROUP BY / agg-expression DSL path (`--group-by`/`--agg`).
/// Distinct from the legacy `--by`/`--metric` rollup: produces one JSON
/// object per group, with columns named after each [`retl::AggOp`] (and
/// each [`retl::GroupKey`]).
fn run_aggregate_expr(
    args: AggregateArgs,
    etl: retl::RedditETL,
    inputs: Vec<PathBuf>,
    manifest_inputs: Vec<PathBuf>,
    input_count: usize,
    shards_dir: &Path,
    manifest_start: RunManifestStart,
) -> Result<()> {
    let group_keys = match args.group_by.as_deref() {
        Some(s) => retl::parse_group_keys(s).map_err(anyhow::Error::msg)?,
        None => Vec::new(),
    };
    let ops = match args.agg.as_deref() {
        Some(s) => retl::parse_agg_ops(s).map_err(anyhow::Error::msg)?,
        None => vec![retl::AggOp::Count],
    };
    let group_keys_for_factory = group_keys.clone();
    let ops_for_factory = ops.clone();

    let (agg, report) = etl
        .aggregate_jsonls_parallel_collect_with::<retl::ExprAggregator, _>(
            inputs,
            shards_dir,
            move || {
                retl::ExprAggregator::new(group_keys_for_factory.clone(), ops_for_factory.clone())
            },
        )?;
    report_aggregate_input_issues(&report);
    ensure_aggregate_inputs_succeeded(input_count, &report, args.strict)?;

    let records_skipped_no_group_key = agg.records_skipped_no_group_key();
    let records_ingested = agg.records_ingested();
    let row_count = agg.row_count();
    let rows = agg.into_rows();

    let output_format = match args.format {
        AggregateFmt::Tsv | AggregateFmt::Jsonl => {
            // TSV is the AggregateFmt::default; treat it as JSONL for the
            // expression DSL since the rollup carries an N-column schema
            // that does not map to two columns.
            write_grouped_jsonl(&args.out, &rows)?;
            "jsonl"
        }
        AggregateFmt::Parquet => {
            anyhow::bail!(
                "--format parquet is not yet supported for --group-by/--agg; use --format jsonl (the default) or --format tsv"
            );
        }
    };

    let mut counts = counts_map(&[
        ("input_files", input_count as u64),
        ("ok_inputs", report.ok_inputs.len() as u64),
        ("partial_inputs", report.partial_count() as u64),
        ("fatal_inputs", report.fatal_count() as u64),
        ("merged_shards", report.merged_shards as u64),
        ("output_rows", row_count as u64),
        ("records_ingested", records_ingested),
        (
            "records_skipped_no_group_key",
            records_skipped_no_group_key,
        ),
    ]);
    if let Some(top) = args.top {
        counts.insert("top".to_string(), top as u64);
    }

    let mut warnings = aggregate_manifest_warnings(&report);
    if records_ingested > 0
        && records_skipped_no_group_key > 0
        && (records_skipped_no_group_key as f64 / records_ingested as f64)
            > AGGREGATE_RECORD_SKIP_WARN_RATE
    {
        let group_by = args.group_by.as_deref().unwrap_or("");
        tracing::warn!(
            group_by = %group_by,
            records_ingested,
            records_skipped_no_group_key,
            skip_rate = records_skipped_no_group_key as f64 / records_ingested as f64,
            "aggregate skipped {} of {} record(s) missing a --group-by '{}' value",
            records_skipped_no_group_key,
            records_ingested,
            group_by,
        );
        warnings.push(format!(
            "{records_skipped_no_group_key} of {records_ingested} record(s) skipped: missing --group-by '{group_by}' value"
        ));
    }

    write_cli_aggregate_manifest(
        manifest_start,
        &args,
        shards_dir,
        &manifest_inputs,
        output_format,
        counts,
        warnings,
    )?;

    eprintln!(
        "Aggregated {} shard(s) to {} ({} group(s)); {} input(s) failed during shard build; {} input(s) skipped after partial read",
        report.merged_shards,
        output_format.to_uppercase(),
        row_count,
        report.fatal_count(),
        report.partial_count()
    );
    if records_skipped_no_group_key > 0 {
        eprintln!(
            "  {} record(s) skipped without a --group-by value (of {} ingested)",
            records_skipped_no_group_key, records_ingested,
        );
    }
    Ok(())
}

fn write_grouped_jsonl(out: &Path, rows: &[serde_json::Map<String, Value>]) -> Result<()> {
    write_text_or_stdout(out, |w| {
        for row in rows {
            serde_json::to_writer(&mut *w, row)?;
            w.write_all(b"\n")?;
        }
        Ok(())
    })
    .with_context(|| format!("publishing output file {}", out.display()))
}

fn write_grouped_tsv(out: &Path, rows: Vec<(String, String)>) -> Result<()> {
    // See `write_rec_count_json`: `--out -` streams the TSV to stdout.
    write_text_or_stdout(out, |w| {
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
    // No manifest when the user opted out, and none for `--out -`: a manifest
    // beside stdout would be staged next to a path literally named `-`.
    // Suppressing it matches `scan`/`dedupe`/`count`/`first-seen` to-stdout.
    if args.runtime.no_manifest || args.out == Path::new("-") {
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
        "strict": args.strict,
        "parallelism": args.runtime.parallelism,
        "progress": !args.runtime.no_progress,
        "emit_manifest": !args.runtime.no_manifest,
        "group_by": args.group_by.as_deref(),
        "agg": args.agg.as_deref(),
        "format": match args.format {
            AggregateFmt::Tsv => "tsv",
            AggregateFmt::Parquet => "parquet",
            AggregateFmt::Jsonl => "jsonl",
        },
        "parquet_row_group_size": (matches!(args.format, AggregateFmt::Parquet)).then_some(
            args.parquet_row_group_size.unwrap_or(retl::DEFAULT_PARQUET_ROW_GROUP_SIZE),
        ),
        "parquet_compression": matches!(args.format, AggregateFmt::Parquet).then(|| {
            args.parquet_compression.clone().unwrap_or_else(|| retl::DEFAULT_PARQUET_COMPRESSION.to_string())
        }),
    });
    manifest.inputs = file_identities(inputs);
    manifest.output_format = output_format.to_string();
    manifest.counts = counts;
    manifest.warnings = warnings;
    manifest.upstream_manifests = discover_upstream_manifests_from_inputs(inputs);
    write_run_manifest(manifest, ManifestDestination::File(args.out.clone()))?;
    Ok(())
}
