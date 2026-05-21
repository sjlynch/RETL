
pub(crate) fn run_convert(args: ConvertArgs) -> Result<()> {
    if args.fields.iter().all(|field| field.trim().is_empty()) {
        anyhow::bail!("retl convert requires at least one --field selector");
    }

    let mut inputs = Vec::new();
    if let Some(spool) = &args.spool {
        let (parts, _min, _max) = discover_spool_parts(spool)?;
        inputs.extend(parts);
    }
    inputs.extend(args.inputs.iter().cloned());
    if inputs.is_empty() {
        anyhow::bail!("retl convert requires --spool or at least one JSONL input file");
    }

    let opts = TabularExportOptions {
        header: !args.no_header,
    };
    let fields = args.fields.clone();
    let to_stdout = args.out == Path::new("-");
    let write = |path: &Path| -> Result<u64> {
        match args.format {
            ConvertFmt::Csv => convert_jsonl_to_csv(&inputs, path, fields.clone(), opts),
            ConvertFmt::Tsv => convert_jsonl_to_tsv(&inputs, path, fields.clone(), opts),
        }
    };

    if to_stdout {
        let work_dir = args
            .work_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from("./etl_work"));
        let stem = match args.format {
            ConvertFmt::Csv => "convert.csv",
            ConvertFmt::Tsv => "convert.tsv",
        };
        stream_path_output_to_stdout(&work_dir, "convert", stem, |path| {
            write(path).map(|_| ())
        })?;
    } else {
        if args.work_dir.is_some() {
            tracing::warn!(
                "--work-dir is only used when streaming converted output to stdout (--out -); it has no effect with a file --out and is ignored"
            );
        }
        write(&args.out)?;
    }
    Ok(())
}
