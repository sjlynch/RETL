
pub(crate) fn run_first_seen(args: FirstSeenArgs) -> Result<()> {
    let to_stdout = args.out == Path::new("-");
    let mut etl = build_etl(&args.common)?;
    if to_stdout {
        // The TSV is written to a scratch tempfile, so a run manifest beside
        // it would be orphaned in `work_dir/lib_tmp`. Suppress it, matching
        // `count --mode author --out -` and `dedupe --out -`.
        etl = etl.run_manifest(false);
    }
    if args.resume {
        etl = etl.resume(true);
    }
    let partial_reporter = etl.partial_read_reporter();
    let mut scan = plan!(etl, args.common, args.query);
    if let Some(limit) = args.limit {
        scan = scan.limit(limit);
    }
    // `--out -` streams the TSV to stdout, matching `count --mode author`.
    if to_stdout {
        let work_dir = args.common.work_dir.clone();
        stream_path_output_to_stdout(&work_dir, "first_seen", "first_seen.tsv", |p| {
            scan.build_first_seen_index_to_tsv(p)
        })?;
    } else {
        scan.build_first_seen_index_to_tsv(&args.out)?;
    }
    emit_partial_read_report(&partial_reporter)?;
    Ok(())
}

fn first_spool_record_keys(spool_parts: &[PathBuf]) -> Result<Option<(PathBuf, Vec<String>)>> {
    for path in spool_parts {
        let f = retl::open_with_default_backoff(path)
            .with_context(|| format!("opening spool part {}", path.display()))?;
        let mut r = BufReader::new(f);
        let mut buf = String::new();
        let mut line_no = 0u64;
        loop {
            let n = read_line_capped(&mut r, &mut buf, DEFAULT_MAX_LINE_BYTES, path).with_context(
                || {
                    format!(
                        "reading spool part {} near line {}",
                        path.display(),
                        line_no + 1
                    )
                },
            )?;
            if n == 0 {
                break;
            }
            line_no += 1;
            if buf.trim().is_empty() {
                continue;
            }
            let v: Value = serde_json::from_str(&buf).with_context(|| {
                format!(
                    "parsing spool part {} line {} while diagnosing missing parent IDs",
                    path.display(),
                    line_no
                )
            })?;
            let keys = match v {
                Value::Object(map) => {
                    let mut keys: Vec<String> = map.keys().cloned().collect();
                    keys.sort();
                    keys
                }
                other => vec![format!("<non-object:{}>", other_type_name(&other))],
            };
            return Ok(Some((path.clone(), keys)));
        }
    }
    Ok(None)
}

fn other_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}
