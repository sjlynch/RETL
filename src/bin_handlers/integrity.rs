
pub(crate) fn run_integrity(args: IntegrityArgs) -> Result<HandlerOutcome> {
    if args.resume {
        anyhow::bail!(
            "retl integrity does not support --resume; it validates corpus files directly. Narrow long runs with --source/--start/--end or use resumable scan/export/count workflows for record processing."
        );
    }
    if args.expected || args.manifest.is_some() {
        preflight_expected_corpus(
            &args.common.data_dir,
            args.common.source,
            args.common.start,
            args.common.end,
            args.manifest.as_deref(),
            args.verify_checksums,
        )?;
    } else if args.verify_checksums {
        anyhow::bail!("--verify-checksums requires --expected or --manifest");
    }

    let etl = build_etl(&args.common)?;
    let mode = match args.mode {
        IntegrityModeArg::Quick => IntegrityMode::Quick {
            sample_bytes: args.sample_bytes,
        },
        IntegrityModeArg::Full => IntegrityMode::Full,
    };
    if let IntegrityMode::Quick { sample_bytes } = mode {
        if sample_bytes > 0 && sample_bytes < QUICK_SAMPLE_WARN_BELOW_BYTES {
            eprintln!(
                "WARNING: quick integrity mode validates only a decompressed prefix sample \
                 ({sample_bytes} byte(s)); use --mode full for complete validation"
            );
        }
    }
    let bad = if args.collect {
        etl.check_corpus_integrity(mode)?
    } else {
        let print_lock = std::sync::Mutex::new(());
        etl.check_corpus_integrity_with_failure_sink(mode, |path, err| {
            let _guard = print_lock.lock().unwrap();
            let stdout = io::stdout();
            let mut w = stdout.lock();
            writeln!(w, "{}\t{}", path.display(), err)?;
            w.flush()?;
            Ok(())
        })?
    };
    if bad.is_empty() {
        eprintln!("OK: no corruption detected.");
        Ok(HandlerOutcome::Done)
    } else {
        eprintln!("FAILED: {} file(s) failed integrity check:", bad.len());
        if args.collect {
            for (p, e) in &bad {
                println!("{}\t{}", p.display(), e);
            }
        }
        // Signal exit code 2 to `main` via the return value rather than a
        // bare `std::process::exit(2)`. That keeps the monitor's `finalize`
        // in the loop: it emits the terminal `run.summary` (outcome
        // `corrupt_files_found`) and marks the `--status-file` finished
        // before the process exits 2. See [`HandlerOutcome`].
        Ok(HandlerOutcome::CorruptFilesFound)
    }
}
