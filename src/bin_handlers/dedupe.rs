
pub(crate) fn run_dedupe(args: DedupeArgs) -> Result<()> {
    let key = parse_dedupe_key(&args.key)?;
    let mut etl = build_etl(&args.common)?;
    if args.out == Path::new("-") {
        etl = etl.run_manifest(false);
    }
    if let Some(b) = args.inflight_bytes {
        etl = etl.inflight_bytes(b);
    }
    if let Some(g) = args.inflight_groups {
        etl = etl.inflight_groups(g);
    }
    if args.strict_key {
        etl = etl.strict_key(true);
    }
    if args.resume {
        etl = etl.resume(true);
    }
    let partial_reporter = etl.partial_read_reporter();
    let work_dir = args.common.work_dir.clone();
    let scan = plan!(etl, args.common, args.query);

    if args.out == Path::new("-") {
        let tmp_path = stdout_dedupe_path(&work_dir);
        let _ = retl::remove_with_short_backoff(&tmp_path);
        let stats = match scan.dedupe_keys_to_lines_with_stats(&key, &tmp_path) {
            Ok(stats) => stats,
            Err(e) => {
                let _ = retl::remove_with_short_backoff(&tmp_path);
                return Err(e);
            }
        };
        let copy_result = (|| -> Result<()> {
            let mut f = retl::open_with_default_backoff(&tmp_path)
                .with_context(|| format!("opening dedupe tempfile {}", tmp_path.display()))?;
            let stdout = io::stdout();
            let mut w = stdout.lock();
            io::copy(&mut f, &mut w).context("streaming dedupe output to stdout")?;
            w.flush()?;
            Ok(())
        })();
        let _ = retl::remove_with_short_backoff(&tmp_path);
        copy_result?;
        print_dedupe_summary(&stats);
    } else {
        let stats = scan.dedupe_keys_to_lines_with_stats(&key, &args.out)?;
        print_dedupe_summary(&stats);
    }
    emit_partial_read_report(&partial_reporter)?;
    Ok(())
}

fn print_dedupe_summary(stats: &retl::DedupeKeySummary) {
    eprintln!(
        "Deduped {} unique keys from {} matching records ({} dropped without key; {:.2}% drop rate)",
        stats.unique_keys,
        stats.matched_records,
        stats.key_extractions_failed,
        stats.key_drop_rate() * 100.0,
    );
}

fn parse_dedupe_key(spec: &str) -> Result<KeyExtractor> {
    let trimmed = spec.trim();
    match trimmed.to_ascii_lowercase().as_str() {
        "author" => Ok(KeyExtractor::author_lowercase_fast()),
        "subreddit" => Ok(KeyExtractor::subreddit_lowercase_fast()),
        _ => {
            let ptr = trimmed.strip_prefix("json:").ok_or_else(|| {
                anyhow::anyhow!(
                    "unsupported --key {trimmed:?}; expected author, subreddit, or json:/pointer"
                )
            })?;
            if !ptr.starts_with('/') {
                anyhow::bail!("JSON pointer keys must start with '/': use --key json:/field");
            }
            Ok(KeyExtractor::json_pointer(ptr.to_string()))
        }
    }
}

fn stdout_dedupe_path(work_dir: &Path) -> PathBuf {
    work_dir
        .join("lib_tmp")
        .join(format!("retl_dedupe_stdout_{}.txt", std::process::id()))
}

fn export_format_name(fmt: ExportFmt) -> &'static str {
    match fmt {
        ExportFmt::Jsonl => "jsonl",
        ExportFmt::Json => "json",
        ExportFmt::Csv => "csv",
        ExportFmt::Tsv => "tsv",
        ExportFmt::Spool => "spool",
        ExportFmt::Zst => "zst",
        ExportFmt::PartitionedJsonl => "partitioned-jsonl",
    }
}
