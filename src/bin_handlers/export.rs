
/// CSV/TSV exports ignore the timestamp/resume knobs entirely (`extract_to_csv`
/// / `extract_to_tsv` never read them), so accepting these flags would silently
/// no-op them and leave the user believing a CSV/TSV run was resumable. Reject
/// them at the CLI layer rather than silently ignoring them — the library's
/// `extract_to_tabular` enforces the same contract, but failing here keeps the
/// error close to the flag and avoids constructing the scan plan first.
fn reject_unsupported_tabular_flags(args: &ExportArgs) -> Result<()> {
    if args.human_timestamps {
        anyhow::bail!(
            "--human-timestamps is not supported for CSV/TSV export; \
             use --format jsonl/json/spool/zst/partitioned-jsonl or omit the flag"
        );
    }
    if args.resume {
        anyhow::bail!(
            "--resume is not supported for CSV/TSV export; \
             use --format jsonl/json/spool/zst/partitioned-jsonl or omit the flag"
        );
    }
    Ok(())
}

/// Reject `--format`-specific flags up front when the chosen format would
/// make them silent no-ops. `--pretty` only field-indents the `json` array
/// and `--zst-level` only sets the `.zst` compression level; for any other
/// format they have no effect. Mirrors `reject_unsupported_tabular_flags` —
/// fail close to the flag instead of accepting it and doing nothing.
fn reject_inapplicable_export_format_flags(args: &ExportArgs) -> Result<()> {
    if args.pretty && !matches!(args.format, ExportFmt::Json) {
        anyhow::bail!(
            "--pretty only applies to --format json (it field-indents the JSON array); \
             --format {} ignores it",
            export_format_name(args.format)
        );
    }
    if args.zst_level.is_some() && !matches!(args.format, ExportFmt::Zst) {
        anyhow::bail!(
            "--zst-level only applies to --format zst (it sets the .zst compression level); \
             --format {} produces no .zst output",
            export_format_name(args.format)
        );
    }
    Ok(())
}

pub(crate) fn run_export(args: ExportArgs) -> Result<()> {
    reject_inapplicable_export_format_flags(&args)?;
    let mut etl = build_etl(&args.common)?;
    if !args.whitelist.is_empty() {
        if args.whitelist.iter().all(|field| field.trim().is_empty()) {
            anyhow::bail!("--whitelist must include at least one non-empty field");
        }
        etl = etl.whitelist_fields(args.whitelist.iter().cloned());
    }
    if args.strict_whitelist {
        etl = etl.strict_whitelist(true);
    }
    if args.human_timestamps {
        etl = etl.timestamps_human_readable(true);
    }
    if let Some(b) = args.inflight_bytes {
        etl = etl.inflight_bytes(b);
    }
    if let Some(g) = args.inflight_groups {
        etl = etl.inflight_groups(g);
    }
    if let Some(level) = args.zst_level {
        etl = etl.zst_level(level);
    }
    if args.resume {
        etl = etl.resume(true);
    }
    let to_stdout = args.out == Path::new("-");
    if to_stdout {
        etl = etl.run_manifest(false);
    }
    let partial_reporter = etl.partial_read_reporter();
    let work_dir = args.common.work_dir.clone();
    let mut scan = plan!(etl, args.common, args.query);
    if let Some(limit) = args.limit {
        scan = scan.limit(limit);
    }

    match args.format {
        ExportFmt::Jsonl => {
            if to_stdout {
                stream_extract_to_stdout(&work_dir, "stdout.jsonl", |p| scan.extract_to_jsonl(p))?;
            } else {
                scan.extract_to_jsonl(&args.out)?;
            }
        }
        ExportFmt::Json => {
            let pretty = args.pretty;
            if to_stdout {
                stream_extract_to_stdout(&work_dir, "stdout.json", |p| {
                    scan.extract_to_json(p, pretty)
                })?;
            } else {
                scan.extract_to_json(&args.out, pretty)?;
            }
        }
        ExportFmt::Csv => {
            reject_unsupported_tabular_flags(&args)?;
            if args.whitelist.is_empty() {
                anyhow::bail!("--format csv requires --whitelist so the CSV schema is fixed");
            }
            let fields = args.whitelist.clone();
            if to_stdout {
                stream_extract_to_stdout(&work_dir, "stdout.csv", |p| {
                    scan.extract_to_csv(p, fields, TabularExportOptions::default())
                })?;
            } else {
                scan.extract_to_csv(&args.out, fields, TabularExportOptions::default())?;
            }
        }
        ExportFmt::Tsv => {
            reject_unsupported_tabular_flags(&args)?;
            if args.whitelist.is_empty() {
                anyhow::bail!("--format tsv requires --whitelist so the TSV schema is fixed");
            }
            let fields = args.whitelist.clone();
            if to_stdout {
                stream_extract_to_stdout(&work_dir, "stdout.tsv", |p| {
                    scan.extract_to_tsv(p, fields, TabularExportOptions::default())
                })?;
            } else {
                scan.extract_to_tsv(&args.out, fields, TabularExportOptions::default())?;
            }
        }
        ExportFmt::Spool => {
            if to_stdout {
                anyhow::bail!(
                    "--format spool requires an explicit --out <DIR> (it writes one part file per month, not a stream)"
                );
            }
            retl::create_dir_all_with_default_backoff(&args.out)
                .with_context(|| format!("creating spool dir {}", args.out.display()))?;
            let (parts, n) = scan.extract_spool_monthly(&args.out)?;
            eprintln!("Spooled {} records across {} part files", n, parts.len());
        }
        ExportFmt::Zst | ExportFmt::PartitionedJsonl => {
            if to_stdout {
                anyhow::bail!(
                    "--format {} requires an explicit --out <DIR> (it writes a partitioned corpus tree, not a stream)",
                    export_format_name(args.format)
                );
            }
            let partition_format = match args.format {
                ExportFmt::Zst => ExportFormat::Zst,
                ExportFmt::PartitionedJsonl => ExportFormat::Jsonl,
                _ => unreachable!(),
            };
            scan.export_partitioned(&args.out, partition_format)?;
        }
    }
    emit_partial_read_report(&partial_reporter)?;
    Ok(())
}
