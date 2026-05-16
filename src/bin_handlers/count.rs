
pub(crate) fn run_count(args: CountArgs) -> Result<()> {
    let author_stdout = matches!(args.mode, CountMode::Author)
        && args.out.as_deref().is_some_and(|p| p == Path::new("-"));
    let mut etl = build_etl(&args.common)?;
    if author_stdout {
        etl = etl.run_manifest(false);
    }
    if args.resume {
        etl = etl.resume(true);
    }
    let partial_reporter = etl.partial_read_reporter();
    let scan = plan!(etl, args.common, args.query);

    match args.mode {
        CountMode::Month => {
            let manifest_start = RunManifestStart::now();
            let counts = scan.count_by_month()?;
            let to_stdout = args.out.as_deref().map_or(true, |p| p == Path::new("-"));
            if to_stdout {
                let stdout = io::stdout();
                let mut w = stdout.lock();
                for (ym, n) in &counts {
                    writeln!(w, "{ym}\t{n}")?;
                }
                w.flush()?;
            } else {
                let path = args.out.as_ref().expect("checked to_stdout").clone();
                write_text_file_atomic(&path, |w| {
                    for (ym, n) in &counts {
                        writeln!(w, "{ym}\t{n}")?;
                    }
                    Ok(())
                })?;
                let total_records: u64 = counts.values().copied().sum();
                write_cli_scan_manifest_for_file(
                    manifest_start,
                    "cli.count_by_month",
                    "retl count --mode month",
                    &args.common,
                    &args.query,
                    &path,
                    "tsv",
                    counts_map(&[
                        ("months", counts.len() as u64),
                        ("records_counted", total_records),
                    ]),
                    &partial_reporter,
                    serde_json::json!({ "mode": "month" }),
                )?;
            }
        }
        CountMode::Author => {
            let out = args.out.ok_or_else(|| {
                anyhow::anyhow!("--out is required for `count --mode author` (TSV destination)")
            })?;
            if out == Path::new("-") {
                let work_dir = args.common.work_dir.clone();
                stream_path_output_to_stdout(&work_dir, "count", "author_counts.tsv", |p| {
                    scan.author_counts_to_tsv(p)
                })?;
            } else {
                scan.author_counts_to_tsv(&out)?;
            }
        }
    }
    emit_partial_read_report(&partial_reporter)?;
    Ok(())
}

fn preflight_expected_corpus(
    data_dir: &Path,
    source: SourceArg,
    start: Option<YearMonth>,
    end: Option<YearMonth>,
    manifest_path: Option<&Path>,
    verify_checksums: bool,
) -> Result<()> {
    let (start, end) = required_manifest_range(start, end)?;
    let manifest = load_corpus_manifest(manifest_path)?;
    let rows = manifest
        .plan(
            Sources::from(source),
            start,
            end,
            data_dir,
            verify_checksums,
        )
        .with_context(|| "building manifest preflight for integrity")?;
    let problems: Vec<&CorpusPlanItem> = rows
        .iter()
        .filter(|item| item.has_local_problem())
        .collect();
    if problems.is_empty() {
        eprintln!(
            "Manifest preflight OK: {} source/month item(s) available locally for {}..={}",
            rows.len(),
            start,
            end
        );
        return Ok(());
    }

    eprintln!(
        "Manifest preflight found {} problem(s) before zstd validation:",
        problems.len()
    );
    for item in problems.iter().take(20) {
        eprintln!(
            "  {} {} availability={} local={} path={}{}",
            item.source.label(),
            item.month,
            availability_cell(item.availability),
            local_status_label(&item.local),
            item.expected_path.display(),
            item.note
                .as_ref()
                .map(|note| format!(" note={note}"))
                .unwrap_or_default()
        );
    }
    if problems.len() > 20 {
        eprintln!("  ... {} more problem(s)", problems.len() - 20);
    }
    anyhow::bail!(
        "manifest preflight failed; run `retl corpus plan --dest {} --source {} --start {} --end {}` to inspect missing/unavailable months and expected paths",
        data_dir.display(),
        source.label(),
        start,
        end
    )
}

fn local_status_label(local: &CorpusLocalStatus) -> &'static str {
    match local {
        CorpusLocalStatus::Missing => "missing",
        CorpusLocalStatus::Inaccessible { .. } => "inaccessible",
        CorpusLocalStatus::Present {
            size_matches,
            sha256_matches,
            ..
        } if matches!(size_matches, Some(false)) || matches!(sha256_matches, Some(false)) => {
            "present_mismatch"
        }
        CorpusLocalStatus::Present { .. } => "present",
    }
}
