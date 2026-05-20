
pub(crate) fn run_integrity(args: IntegrityArgs) -> Result<()> {
    if args.resume {
        anyhow::bail!(
            "retl integrity does not support --resume; it validates corpus files directly. Narrow long runs with --source/--start/--end or use resumable scan/export/count workflows for record processing."
        );
    }
    // `integrity` validates corpus files in place and never writes a
    // provenance manifest, so `--no-manifest` (shared via `CommonOpts`) is
    // inert here. Surface it instead of silently accepting it.
    if args.common.no_manifest {
        tracing::warn!(
            "--no-manifest has no effect on `retl integrity`: it validates corpus files in place and writes no manifest"
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
    let report = if args.collect {
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
    if report.is_ok() {
        eprintln!("OK: no corruption detected.");
    } else {
        eprintln!(
            "FAILED: {} file(s) failed integrity check:",
            report.failure_count()
        );
        if args.collect {
            for (p, e) in &report.failures {
                println!("{}\t{}", p.display(), e);
            }
            // The retained list is capped to bound memory on all-corrupt
            // corpora; the streaming (non-collect) path already printed
            // every failure as it happened, so this only applies here.
            if report.dropped > 0 {
                eprintln!(
                    "... and {} more failure(s) not listed (retained list capped at {})",
                    report.dropped, MAX_RETAINED_FAILURES
                );
            }
        }
        std::process::exit(2);
    }
    Ok(())
}
