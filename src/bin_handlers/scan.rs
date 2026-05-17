
pub(crate) fn run_scan(args: ScanArgs) -> Result<()> {
    let mut etl = build_etl(&args.common)?;
    if args.resume {
        etl = etl.resume(true);
    }
    let partial_reporter = etl.partial_read_reporter();
    let mut scan = plan!(etl, args.common, args.query);
    if let Some(limit) = args.limit {
        scan = scan.limit(limit);
    }

    match args.out {
        Some(path) => {
            let manifest_start = RunManifestStart::now();
            let written = write_text_atomic(&path, CLI_TEXT_WRITE_BUF_BYTES, |w| {
                let mut written = 0_u64;
                scan.try_for_each_username(|u| {
                    writeln!(w, "{u}")?;
                    written += 1;
                    Ok(())
                })?;
                Ok(written)
            })?;
            write_cli_scan_manifest_for_file(
                manifest_start,
                "cli.scan",
                "retl scan",
                &args.common,
                &args.query,
                &path,
                "text-lines",
                counts_map(&[("unique_usernames", written)]),
                &partial_reporter,
                serde_json::json!({ "limit": args.limit }),
            )?;
        }
        None => {
            let stdout = io::stdout();
            let mut w = BufWriter::new(stdout.lock());
            scan.try_for_each_username(|u| {
                writeln!(w, "{u}")?;
                Ok(())
            })?;
            w.flush()?;
        }
    }
    emit_partial_read_report(&partial_reporter)?;
    Ok(())
}
