
/// Reject `--format`-specific sample flags that would otherwise be silent
/// no-ops, matching `export`'s `reject_inapplicable_export_format_flags` /
/// `reject_unsupported_tabular_flags`. `--pretty` only field-indents the
/// `json` array; `--human-timestamps` is unsupported for the CSV/TSV tabular
/// formats (the library's `extract_to_tabular` enforces the same contract,
/// but failing here keeps the error close to the flag). `sample` has no
/// `--resume` / `--zst-level` flags, so only these two need checking.
fn reject_inapplicable_sample_flags(args: &SampleArgs) -> Result<()> {
    if args.pretty && !matches!(args.format, ExportFmt::Json) {
        anyhow::bail!(
            "--pretty only applies to --format json (it field-indents the JSON array); \
             --format {} ignores it",
            export_format_name(args.format)
        );
    }
    if args.human_timestamps && matches!(args.format, ExportFmt::Csv | ExportFmt::Tsv) {
        anyhow::bail!(
            "--human-timestamps is not supported for CSV/TSV sample output; \
             use --format jsonl/json/spool/zst/partitioned-jsonl or omit the flag"
        );
    }
    Ok(())
}

pub(crate) fn run_sample(args: SampleArgs) -> Result<()> {
    reject_inapplicable_sample_flags(&args)?;
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
    let to_stdout = args.out == Path::new("-");
    if to_stdout {
        etl = etl.run_manifest(false);
    }
    let partial_reporter = etl.partial_read_reporter();
    let work_dir = args.common.work_dir.clone();
    let scan = plan!(etl, args.common, args.query).limit(args.limit);

    match args.format {
        ExportFmt::Jsonl => {
            if to_stdout {
                stream_extract_to_stdout(&work_dir, "sample.jsonl", |p| scan.extract_to_jsonl(p))?;
            } else {
                scan.extract_to_jsonl(&args.out)?;
            }
        }
        ExportFmt::Json => {
            if to_stdout {
                stream_extract_to_stdout(&work_dir, "sample.json", |p| {
                    scan.extract_to_json(p, args.pretty)
                })?;
            } else {
                scan.extract_to_json(&args.out, args.pretty)?;
            }
        }
        ExportFmt::Csv => {
            if args.whitelist.is_empty() {
                anyhow::bail!("--format csv requires --whitelist so the CSV schema is fixed");
            }
            let fields = args.whitelist.clone();
            if to_stdout {
                stream_extract_to_stdout(&work_dir, "sample.csv", |p| {
                    scan.extract_to_csv(p, fields, TabularExportOptions::default())
                })?;
            } else {
                scan.extract_to_csv(&args.out, fields, TabularExportOptions::default())?;
            }
        }
        ExportFmt::Tsv => {
            if args.whitelist.is_empty() {
                anyhow::bail!("--format tsv requires --whitelist so the TSV schema is fixed");
            }
            let fields = args.whitelist.clone();
            if to_stdout {
                stream_extract_to_stdout(&work_dir, "sample.tsv", |p| {
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
            eprintln!(
                "Spooled {} sample records across {} part files",
                n,
                parts.len()
            );
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
