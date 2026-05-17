
#[derive(Debug)]
struct SchemaSampleDone;

impl std::fmt::Display for SchemaSampleDone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("schema sample complete")
    }
}

impl std::error::Error for SchemaSampleDone {}

fn schema_sample_done() -> anyhow::Error {
    anyhow::anyhow!(SchemaSampleDone)
}

fn is_schema_sample_done(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|cause| cause.downcast_ref::<SchemaSampleDone>().is_some())
}

#[derive(Default)]
struct SchemaFieldStats {
    present_records: u64,
    type_counts: HashMap<&'static str, u64>,
}

#[derive(Serialize)]
struct SchemaRow {
    field: String,
    #[serde(rename = "type")]
    type_name: String,
    presence_pct: f64,
    present_records: u64,
    sampled_records: u64,
}

fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn most_common_type(stats: &SchemaFieldStats) -> String {
    stats
        .type_counts
        .iter()
        .max_by(|(left_ty, left_count), (right_ty, right_count)| {
            left_count
                .cmp(right_count)
                .then_with(|| right_ty.cmp(left_ty))
        })
        .map(|(ty, _)| (*ty).to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn collect_schema(args: &SchemaArgs) -> Result<Vec<SchemaRow>> {
    if let (Some(start), Some(end)) = (args.start, args.end) {
        if start > end {
            anyhow::bail!("invalid date range: start {start} is after end {end}");
        }
    }

    let comments_dir = args.data_dir.join("comments");
    let submissions_dir = args.data_dir.join("submissions");
    let discovered =
        discover_sources_checked(&comments_dir, &submissions_dir, Sources::from(args.source))?;
    let jobs = plan_files_checked(
        &discovered,
        &comments_dir,
        &submissions_dir,
        Sources::from(args.source),
        args.start,
        args.end,
    )?;

    let mut fields = BTreeMap::<String, SchemaFieldStats>::new();
    let mut sampled_records = 0_u64;
    for job in &jobs {
        if args.sample_per_month == 0 {
            continue;
        }
        let mut sampled_this_month = 0_usize;
        let mut line_number = 0_u64;
        let result = for_each_line_cfg(&job.path, 256 * 1024, |line| -> Result<()> {
            if sampled_this_month >= args.sample_per_month {
                return Err(schema_sample_done());
            }
            line_number += 1;
            let value: Value = serde_json::from_str(line).with_context(|| {
                format!(
                    "parsing {} line {} while discovering schema",
                    job.path.display(),
                    line_number
                )
            })?;
            sampled_this_month += 1;
            sampled_records += 1;
            if let Some(map) = value.as_object() {
                for (field, value) in map {
                    let stats = fields.entry(field.clone()).or_default();
                    stats.present_records += 1;
                    *stats.type_counts.entry(json_type_name(value)).or_insert(0) += 1;
                }
            }
            if sampled_this_month >= args.sample_per_month {
                return Err(schema_sample_done());
            }
            Ok(())
        });
        match result {
            Ok(()) => {}
            Err(e) if is_schema_sample_done(&e) => {}
            Err(e) => return Err(e),
        }
    }

    Ok(fields
        .into_iter()
        .map(|(field, stats)| SchemaRow {
            field,
            type_name: most_common_type(&stats),
            presence_pct: if sampled_records == 0 {
                0.0
            } else {
                stats.present_records as f64 * 100.0 / sampled_records as f64
            },
            present_records: stats.present_records,
            sampled_records,
        })
        .collect())
}

pub(crate) fn run_schema(args: SchemaArgs) -> Result<()> {
    let stdout = io::stdout();
    let mut w = BufWriter::new(stdout.lock());
    run_schema_to(args, &mut w)?;
    w.flush()?;
    Ok(())
}

/// `run_schema` with an explicit writer for in-process tests.
pub(crate) fn run_schema_to(args: SchemaArgs, w: &mut dyn Write) -> Result<()> {
    let rows = collect_schema(&args)?;
    match args.format {
        SchemaFmt::Tsv => {
            writeln!(
                w,
                "field\ttype\tpresence_pct\tpresent_records\tsampled_records"
            )?;
            for row in rows {
                writeln!(
                    w,
                    "{}\t{}\t{:.2}\t{}\t{}",
                    row.field,
                    row.type_name,
                    row.presence_pct,
                    row.present_records,
                    row.sampled_records
                )?;
            }
        }
        SchemaFmt::Json => {
            serde_json::to_writer_pretty(&mut *w, &rows)?;
            writeln!(w)?;
        }
    }
    Ok(())
}
