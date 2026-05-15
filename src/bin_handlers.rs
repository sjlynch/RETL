//! Subcommand handlers for the `retl` binary. Each `run_*` function
//! corresponds 1:1 to a `Command` variant and is invoked from `main.rs`.

use anyhow::{Context, Result};
use retl::{
    create_dir_all_with_backoff, create_new_with_backoff, create_with_backoff,
    discover_sources_checked, for_each_line_cfg, format_year_month_ranges,
    missing_month_diagnostics, open_with_backoff, plan_files, plan_files_checked, read_line_capped,
    remove_with_backoff, replace_file_atomic_backoff, total_compressed_size, AggregateBuildReport,
    ExportFormat, FileKind, IntegrityMode, KeyExtractor, ParentPayloadSpec, RedditETL, Sources,
    TabularExportOptions, YearMonth, DEFAULT_MAX_LINE_BYTES,
};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
#[cfg(test)]
use std::fs;
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::bin_args::{
    AggregateArgs, CountArgs, CountMode, DedupeArgs, DescribeArgs, ExportArgs, ExportFmt,
    FirstSeenArgs, IntegrityArgs, IntegrityModeArg, ParentsArgs, QuickstartArgs, SampleArgs,
    ScanArgs, SchemaArgs, SchemaFmt, SourceArg,
};
use crate::bin_helpers::{
    build_etl, discover_spool_parts, emit_partial_read_report, plan, stream_extract_to_stdout,
    stream_path_output_to_stdout, GroupBySpec, GroupMetricAgg, MetricSpec, RecCount,
};

const CLI_TEXT_WRITE_BUF_BYTES: usize = 64 * 1024;
const QUICK_SAMPLE_WARN_BELOW_BYTES: u64 = 4096;
const QUICKSTART_SAMPLE_JSONL: &str = include_str!("../benches/data/sample.jsonl");
const QUICKSTART_ZST_LEVEL: i32 = 3;
static CLI_STAGE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn output_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn write_text_file_atomic<T, F>(final_path: &Path, body: F) -> Result<T>
where
    F: FnOnce(&mut dyn Write) -> Result<T>,
{
    let parent = output_parent(final_path);
    let staging_dir = parent.join("_staging");
    create_dir_all_with_backoff(&staging_dir, 16, 50)
        .with_context(|| format!("creating staging dir {}", staging_dir.display()))?;

    let file_name = final_path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("output path has no file name: {}", final_path.display()))?;
    let mut staged_name = file_name.to_os_string();
    let counter = CLI_STAGE_COUNTER.fetch_add(1, Ordering::Relaxed);
    staged_name.push(format!(".{}.{}.inprogress", std::process::id(), counter));
    let staged = staging_dir.join(staged_name);

    let file = create_new_with_backoff(&staged, 16, 50)
        .with_context(|| format!("creating staged output {}", staged.display()))?;
    let mut w = BufWriter::with_capacity(CLI_TEXT_WRITE_BUF_BYTES, file);

    let result = match body(&mut w) {
        Ok(v) => v,
        Err(e) => {
            drop(w);
            let _ = remove_with_backoff(&staged, 8, 50);
            return Err(e).with_context(|| format!("writing staged output {}", staged.display()));
        }
    };

    if let Err(e) = w.flush() {
        drop(w);
        let _ = remove_with_backoff(&staged, 8, 50);
        return Err(e).with_context(|| format!("flushing staged output {}", staged.display()));
    }
    drop(w);

    let parent = output_parent(final_path);
    create_dir_all_with_backoff(parent, 16, 50)
        .with_context(|| format!("creating output parent {}", parent.display()))?;

    if let Err(e) = replace_file_atomic_backoff(&staged, final_path) {
        let _ = remove_with_backoff(&staged, 8, 50);
        return Err(e).with_context(|| {
            format!(
                "publishing staged output {} -> {}",
                staged.display(),
                final_path.display()
            )
        });
    }

    Ok(result)
}

pub(crate) fn run_describe(args: DescribeArgs) -> Result<()> {
    if args.schema {
        return run_schema(SchemaArgs {
            data_dir: args.data_dir,
            start: args.start,
            end: args.end,
            source: args.source,
            sample_per_month: args.schema_sample,
            format: args.schema_format,
        });
    }

    if let (Some(start), Some(end)) = (args.start, args.end) {
        if start > end {
            anyhow::bail!("invalid date range: start {start} is after end {end}");
        }
    }

    let comments_dir = args.data_dir.join("comments");
    let submissions_dir = args.data_dir.join("submissions");
    let discovered =
        discover_sources_checked(&comments_dir, &submissions_dir, Sources::from(args.source))?;

    let mut rows = Vec::new();
    for kind in describe_kinds(args.source) {
        let map = match kind {
            FileKind::Comment => &discovered.comments,
            FileKind::Submission => &discovered.submissions,
        };
        let jobs = plan_files(&discovered, source_for_kind(kind), args.start, args.end);
        let bytes = total_compressed_size(&jobs);
        let diagnostics =
            missing_month_diagnostics(&discovered, source_for_kind(kind), args.start, args.end);
        let missing_months = diagnostics
            .first()
            .map(|d| format_year_month_ranges(&d.months))
            .unwrap_or_else(|| "-".to_string());
        let missing_count = diagnostics.first().map(|d| d.months.len()).unwrap_or(0);
        rows.push((
            source_label(kind),
            available_range(map),
            jobs.len(),
            bytes,
            missing_count,
            missing_months,
        ));
    }

    let total_files: usize = rows.iter().map(|(_, _, files, _, _, _)| *files).sum();
    let total_bytes: u64 = rows.iter().map(|(_, _, _, bytes, _, _)| *bytes).sum();
    let total_missing: usize = rows.iter().map(|(_, _, _, _, missing, _)| *missing).sum();

    let stdout = io::stdout();
    let mut w = BufWriter::new(stdout.lock());
    writeln!(
        w,
        "source\tavailable\tfiles_in_range\tcompressed_bytes\tmissing_month_count\tmissing_months"
    )?;
    for (label, available, files, bytes, missing_count, missing_months) in rows {
        writeln!(
            w,
            "{label}\t{available}\t{files}\t{bytes}\t{missing_count}\t{missing_months}"
        )?;
    }
    writeln!(
        w,
        "total\t\t{total_files}\t{total_bytes}\t{total_missing}\t-"
    )?;
    w.flush()?;
    Ok(())
}

fn describe_kinds(source: SourceArg) -> Vec<FileKind> {
    match source {
        SourceArg::Rc => vec![FileKind::Comment],
        SourceArg::Rs => vec![FileKind::Submission],
        SourceArg::Both => vec![FileKind::Comment, FileKind::Submission],
    }
}

fn source_for_kind(kind: FileKind) -> Sources {
    match kind {
        FileKind::Comment => Sources::Comments,
        FileKind::Submission => Sources::Submissions,
    }
}

fn source_label(kind: FileKind) -> &'static str {
    match kind {
        FileKind::Comment => "rc",
        FileKind::Submission => "rs",
    }
}

fn available_range(map: &BTreeMap<YearMonth, PathBuf>) -> String {
    match (map.keys().next(), map.keys().next_back()) {
        (Some(first), Some(last)) => format!("{first}..={last}"),
        _ => "<none>".to_string(),
    }
}

pub(crate) fn run_quickstart(args: QuickstartArgs) -> Result<()> {
    let data_dir = prepare_quickstart_sample_corpus(&args.out_dir)?;
    let work_dir = args.out_dir.join("etl_work");
    create_dir_all_with_backoff(&work_dir, 16, 50)
        .with_context(|| format!("creating quickstart work dir {}", work_dir.display()))?;

    let base = RedditETL::new()
        .base_dir(&data_dir)
        .work_dir(&work_dir)
        .parallelism(1)
        .file_concurrency(1)
        .progress(false)
        .sources(Sources::Both);

    let counts = base
        .clone()
        .scan()
        .subreddit("programming")
        .count_by_month()?;

    let mut authors = Vec::new();
    base.scan()
        .subreddit("programming")
        .for_each_username(|u| authors.push(u.to_string()))?;
    authors.sort();

    let stdout = io::stdout();
    let mut w = BufWriter::new(stdout.lock());
    writeln!(
        w,
        "Prepared sample corpus from benches/data/sample.jsonl under {}",
        data_dir.display()
    )?;
    writeln!(w, "Feature demo: subreddit=programming, source=RC+RS")?;
    for (ym, n) in counts {
        writeln!(w, "{ym}\t{n} records")?;
    }
    writeln!(
        w,
        "Found {} unique authors: {}",
        authors.len(),
        authors.join(", ")
    )?;
    writeln!(
        w,
        "Next: retl sample --data-dir {} --source both --subreddit programming --limit 3",
        data_dir.display()
    )?;
    w.flush()?;
    Ok(())
}

fn prepare_quickstart_sample_corpus(out_dir: &Path) -> Result<PathBuf> {
    let data_dir = out_dir.join("data");
    let comments_dir = data_dir.join("comments");
    let submissions_dir = data_dir.join("submissions");
    create_dir_all_with_backoff(&comments_dir, 16, 50)
        .with_context(|| format!("creating comments dir {}", comments_dir.display()))?;
    create_dir_all_with_backoff(&submissions_dir, 16, 50)
        .with_context(|| format!("creating submissions dir {}", submissions_dir.display()))?;

    let mut comments = Vec::new();
    let mut submissions = Vec::new();
    for line in QUICKSTART_SAMPLE_JSONL
        .lines()
        .filter(|line| !line.trim().is_empty())
    {
        let value: Value = serde_json::from_str(line).context("parse sample JSONL line")?;
        if value.get("body").is_some() {
            comments.push(line.to_owned());
        } else if value.get("title").is_some() {
            submissions.push(line.to_owned());
        }
    }

    write_quickstart_zst_lines(&comments_dir.join("RC_2020-01.zst"), &comments)?;
    write_quickstart_zst_lines(&submissions_dir.join("RS_2020-01.zst"), &submissions)?;

    Ok(data_dir)
}

fn write_quickstart_zst_lines(path: &Path, lines: &[String]) -> Result<()> {
    let file = create_with_backoff(path, 16, 50)
        .with_context(|| format!("creating quickstart sample {}", path.display()))?;
    let mut enc =
        zstd::stream::write::Encoder::new(file, QUICKSTART_ZST_LEVEL).context("zstd encoder")?;
    enc.include_checksum(true).context("enable zstd checksum")?;
    for line in lines {
        enc.write_all(line.as_bytes())?;
        enc.write_all(b"\n")?;
    }
    enc.finish().context("finish zstd frame")?;
    Ok(())
}

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
    let rows = collect_schema(&args)?;
    let stdout = io::stdout();
    let mut w = BufWriter::new(stdout.lock());
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
            serde_json::to_writer_pretty(&mut w, &rows)?;
            writeln!(w)?;
        }
    }
    w.flush()?;
    Ok(())
}

pub(crate) fn run_scan(args: ScanArgs) -> Result<()> {
    let etl = build_etl(&args.common)?;
    let partial_reporter = etl.partial_read_reporter();
    let mut scan = plan!(etl, args.common, args.query);
    if let Some(limit) = args.limit {
        scan = scan.limit(limit);
    }

    match args.out {
        Some(path) => {
            write_text_file_atomic(&path, |w| {
                scan.try_for_each_username(|u| {
                    writeln!(w, "{u}")?;
                    Ok(())
                })
            })?;
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

pub(crate) fn run_dedupe(args: DedupeArgs) -> Result<()> {
    let key = parse_dedupe_key(&args.key)?;
    let mut etl = build_etl(&args.common)?;
    if let Some(b) = args.inflight_bytes {
        etl = etl.inflight_bytes(b);
    }
    if let Some(g) = args.inflight_groups {
        etl = etl.inflight_groups(g);
    }
    if args.strict_key {
        etl = etl.strict_key(true);
    }
    let partial_reporter = etl.partial_read_reporter();
    let work_dir = args.common.work_dir.clone();
    let scan = plan!(etl, args.common, args.query);

    if args.out == Path::new("-") {
        let tmp_path = stdout_dedupe_path(&work_dir);
        let _ = remove_with_backoff(&tmp_path, 8, 50);
        let stats = match scan.dedupe_keys_to_lines_with_stats(&key, &tmp_path) {
            Ok(stats) => stats,
            Err(e) => {
                let _ = remove_with_backoff(&tmp_path, 8, 50);
                return Err(e);
            }
        };
        let copy_result = (|| -> Result<()> {
            let mut f = open_with_backoff(&tmp_path, 16, 50)
                .with_context(|| format!("opening dedupe tempfile {}", tmp_path.display()))?;
            let stdout = io::stdout();
            let mut w = stdout.lock();
            io::copy(&mut f, &mut w).context("streaming dedupe output to stdout")?;
            w.flush()?;
            Ok(())
        })();
        let _ = remove_with_backoff(&tmp_path, 8, 50);
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

pub(crate) fn run_export(args: ExportArgs) -> Result<()> {
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
    let partial_reporter = etl.partial_read_reporter();
    let work_dir = args.common.work_dir.clone();
    let mut scan = plan!(etl, args.common, args.query);
    if let Some(limit) = args.limit {
        scan = scan.limit(limit);
    }
    let to_stdout = args.out == Path::new("-");

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
                anyhow::bail!("--out - is not valid for --format spool (it expects a directory)");
            }
            create_dir_all_with_backoff(&args.out, 16, 50)
                .with_context(|| format!("creating spool dir {}", args.out.display()))?;
            let (parts, n) = scan.extract_spool_monthly(&args.out)?;
            eprintln!("Spooled {} records across {} part files", n, parts.len());
        }
        ExportFmt::Zst | ExportFmt::PartitionedJsonl => {
            if to_stdout {
                anyhow::bail!(
                    "--out - is not valid for --format {} (it expects a directory)",
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

pub(crate) fn run_sample(args: SampleArgs) -> Result<()> {
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
    let partial_reporter = etl.partial_read_reporter();
    let work_dir = args.common.work_dir.clone();
    let scan = plan!(etl, args.common, args.query).limit(args.limit);
    let to_stdout = args.out == Path::new("-");

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
                anyhow::bail!("--out - is not valid for --format spool (it expects a directory)");
            }
            create_dir_all_with_backoff(&args.out, 16, 50)
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
                    "--out - is not valid for --format {} (it expects a directory)",
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

pub(crate) fn run_count(args: CountArgs) -> Result<()> {
    let etl = build_etl(&args.common)?;
    let partial_reporter = etl.partial_read_reporter();
    let scan = plan!(etl, args.common, args.query);

    match args.mode {
        CountMode::Month => {
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
                let path = args.out.unwrap();
                write_text_file_atomic(&path, |w| {
                    for (ym, n) in &counts {
                        writeln!(w, "{ym}\t{n}")?;
                    }
                    Ok(())
                })?;
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

pub(crate) fn run_integrity(args: IntegrityArgs) -> Result<()> {
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
    } else {
        eprintln!("FAILED: {} file(s) failed integrity check:", bad.len());
        if args.collect {
            for (p, e) in &bad {
                println!("{}\t{}", p.display(), e);
            }
        }
        std::process::exit(2);
    }
    Ok(())
}

pub(crate) fn run_aggregate(args: AggregateArgs) -> Result<()> {
    let mut etl = RedditETL::new().progress(!args.runtime.no_progress);
    if let Some(p) = args.runtime.parallelism {
        etl = etl.parallelism(p);
    }

    let inputs = resolve_aggregate_inputs(&args)?;
    let input_count = inputs.len();

    let shards_dir = args.shards_dir.unwrap_or_else(|| {
        args.out
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join("agg_shards")
    });
    create_dir_all_with_backoff(&shards_dir, 16, 50)
        .with_context(|| format!("creating shards_dir {}", shards_dir.display()))?;
    if let Some(by) = args.by {
        let group_by = GroupBySpec::parse(&by)?;
        let metric = MetricSpec::parse(args.metric.as_deref())?;
        let top = args.top;
        let (agg, report) = etl.aggregate_jsonls_parallel_collect_with::<GroupMetricAgg, _>(
            inputs,
            &shards_dir,
            || GroupMetricAgg::new(group_by.clone(), metric.clone()),
        )?;
        report_aggregate_input_issues(&report);
        ensure_aggregate_inputs_succeeded(input_count, &report)?;
        let rows = if args.scientific {
            agg.rows_with_scientific(top, true)
        } else {
            agg.rows(top)
        };
        write_grouped_tsv(&args.out, rows)?;
        eprintln!(
            "Aggregated {} shard(s) to TSV; {} input(s) failed during shard build; {} input(s) skipped after partial read",
            report.merged_shards,
            report.fatal_count(),
            report.partial_count()
        );
    } else {
        if args.metric.is_some() {
            anyhow::bail!("--metric requires --by");
        }
        if args.top.is_some() {
            anyhow::bail!("--top requires --by");
        }
        let (agg, report) =
            etl.aggregate_jsonls_parallel_collect::<RecCount>(inputs, &shards_dir)?;
        report_aggregate_input_issues(&report);
        ensure_aggregate_inputs_succeeded(input_count, &report)?;
        write_rec_count_json(&args.out, &agg, args.pretty)?;
        eprintln!(
            "Aggregated {} shard(s); {} input(s) failed during shard build; {} input(s) skipped after partial read",
            report.merged_shards,
            report.fatal_count(),
            report.partial_count()
        );
    }
    Ok(())
}

fn resolve_aggregate_inputs(args: &AggregateArgs) -> Result<Vec<PathBuf>> {
    if args.spool.is_some() && !args.inputs.is_empty() {
        anyhow::bail!(
            "retl aggregate accepts either --spool <DIR> or explicit JSONL input files, not both"
        );
    }

    if let Some(spool) = &args.spool {
        let (parts, _, _) = discover_spool_parts(spool).with_context(|| {
            format!(
                "discovering spool parts for aggregate in {}; use a directory produced by `retl export --format spool --out {}` or pass explicit JSONL files",
                spool.display(),
                spool.display()
            )
        })?;
        return Ok(parts);
    }

    if args.inputs.is_empty() {
        anyhow::bail!(
            "retl aggregate requires --spool <DIR> or one or more explicit JSONL input files"
        );
    }

    reject_unexpanded_aggregate_globs(&args.inputs)?;
    Ok(args.inputs.clone())
}

fn reject_unexpanded_aggregate_globs(inputs: &[PathBuf]) -> Result<()> {
    for input in inputs {
        if path_contains_glob_meta(input) && !input.exists() {
            let suggested_spool = input
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            anyhow::bail!(
                "aggregate input {} looks like an unexpanded glob. RETL does not expand globs itself; use `retl aggregate --spool {}` for spool directories, or pass explicit JSONL file paths.",
                input.display(),
                suggested_spool.display()
            );
        }
    }
    Ok(())
}

fn path_contains_glob_meta(path: &Path) -> bool {
    path.as_os_str()
        .to_string_lossy()
        .chars()
        .any(|c| matches!(c, '*' | '?' | '[' | ']'))
}

fn report_aggregate_input_issues(report: &AggregateBuildReport) {
    if !report.fatal_inputs.is_empty() {
        eprintln!("Aggregate input(s) failed during shard build:");
        for issue in &report.fatal_inputs {
            eprintln!("  {}\t{}", issue.input.display(), issue.error);
        }
    }
    if !report.partial_inputs.is_empty() {
        eprintln!("Aggregate input(s) skipped after partial read:");
        for issue in &report.partial_inputs {
            eprintln!("  {}\t{}", issue.input.display(), issue.error);
        }
    }
}

fn ensure_aggregate_inputs_succeeded(
    input_count: usize,
    report: &AggregateBuildReport,
) -> Result<()> {
    let errors = report.problem_count();
    if input_count > 0 && (errors == input_count || report.merged_shards == 0) {
        anyhow::bail!(
            "aggregate failed: {errors} of {input_count} input(s) failed or were partial; {} shard(s) merged",
            report.merged_shards
        );
    }
    Ok(())
}

fn write_rec_count_json(out: &Path, agg: &RecCount, pretty: bool) -> Result<()> {
    write_text_file_atomic(out, |w| {
        if pretty {
            serde_json::to_writer_pretty(w, agg)?;
        } else {
            serde_json::to_writer(w, agg)?;
        }
        Ok(())
    })
    .with_context(|| format!("publishing output file {}", out.display()))
}

fn write_grouped_tsv(out: &Path, rows: Vec<(String, String)>) -> Result<()> {
    write_text_file_atomic(out, |w| {
        for (key, value) in rows {
            writeln!(w, "{key}\t{value}")?;
        }
        Ok(())
    })
    .with_context(|| format!("publishing output file {}", out.display()))
}

pub(crate) fn run_first_seen(args: FirstSeenArgs) -> Result<()> {
    let etl = build_etl(&args.common)?;
    let partial_reporter = etl.partial_read_reporter();
    let scan = plan!(etl, args.common, args.query);
    scan.build_first_seen_index_to_tsv(&args.out)?;
    emit_partial_read_report(&partial_reporter)?;
    Ok(())
}

fn first_spool_record_keys(spool_parts: &[PathBuf]) -> Result<Option<(PathBuf, Vec<String>)>> {
    for path in spool_parts {
        let f = open_with_backoff(path, 16, 50)
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

fn bail_empty_parent_ids(spool_parts: &[PathBuf]) -> Result<()> {
    let observed = match first_spool_record_keys(spool_parts)? {
        Some((path, keys)) => format!(
            "first record in {} contained keys: [{}]",
            path.display(),
            keys.join(",")
        ),
        None => "the discovered spool parts contained no JSON records".to_string(),
    };

    anyhow::bail!(
        "parents pipeline found no usable parent references in the spool: no `t1_`/`t3_` `parent_id` or `link_id` values were collected. This usually means the spool was produced with --whitelist/.whitelist_fields that omitted `parent_id` and `link_id`; {observed}. Re-run export/spool with --whitelist including body,parent_id,link_id (body is optional for matching but preserves the child comment text)."
    );
}

pub(crate) fn run_parents(args: ParentsArgs) -> Result<()> {
    let (spool_parts, min_ym, max_ym) = discover_spool_parts(&args.spool)?;

    let mut wstart = min_ym;
    let mut wend = max_ym;
    for _ in 0..args.window_months {
        if let Some(p) = wstart.prev() {
            wstart = p;
        }
        if let Some(n) = wend.next() {
            wend = n;
        }
    }

    create_dir_all_with_backoff(&args.cache, 16, 50)
        .with_context(|| format!("creating cache dir {}", args.cache.display()))?;
    create_dir_all_with_backoff(&args.out, 16, 50)
        .with_context(|| format!("creating output dir {}", args.out.display()))?;
    create_dir_all_with_backoff(&args.work_dir, 16, 50)
        .with_context(|| format!("creating work_dir {}", args.work_dir.display()))?;
    let lib_tmp = args.work_dir.join("lib_tmp");
    create_dir_all_with_backoff(&lib_tmp, 16, 50)
        .with_context(|| format!("creating work_dir {}", lib_tmp.display()))?;

    let parent_payload_spec = if args.parent_full {
        ParentPayloadSpec::full_record()
    } else if args.parent_fields.is_empty() {
        ParentPayloadSpec::default()
    } else {
        ParentPayloadSpec::from_fields(&args.parent_fields)
    };

    let build = |sources: Option<Sources>, range: Option<(YearMonth, YearMonth)>| -> RedditETL {
        let mut etl = RedditETL::new()
            .base_dir(&args.data_dir)
            .work_dir(&lib_tmp)
            .parent_payload_spec(parent_payload_spec.clone())
            .progress(!args.no_progress);
        if let Some(s) = sources {
            etl = etl.sources(s);
        }
        if let Some((s, e)) = range {
            etl = etl.date_range(Some(s), Some(e));
        }
        if let Some(p) = args.parallelism {
            etl = etl.parallelism(p);
        }
        if let Some(fc) = args.file_concurrency {
            etl = etl.file_concurrency(fc);
        }
        if let Some(b) = args.inflight_bytes {
            etl = etl.inflight_bytes(b);
        }
        if let Some(g) = args.inflight_groups {
            etl = etl.inflight_groups(g);
        }
        etl
    };

    let ids = build(None, None).collect_parent_ids_from_jsonls(spool_parts.clone())?;
    if ids.is_empty() {
        return bail_empty_parent_ids(&spool_parts);
    }

    let parents = build(Some(Sources::Both), Some((wstart, wend))).resolve_parent_maps(
        &ids,
        &args.cache,
        args.resume,
    )?;
    let (attached, stats) = build(None, Some((wstart, wend)))
        .attach_parents_jsonls_parallel_with_stats(spool_parts, &args.out, &parents, args.resume)?;

    if stats.total() > 0 && stats.unresolved_rate() > 0.05 {
        tracing::warn!(
            resolved = stats.resolved,
            unresolved = stats.unresolved,
            unresolved_rate = stats.unresolved_rate(),
            window_months = args.window_months,
            "more than 5% of parent lookups were unresolved; consider a larger --window-months"
        );
    }

    eprintln!(
        "Attached parents to {} file(s) in {} (resolved over {}..={}; parents resolved={}, unresolved={})",
        attached.len(),
        args.out.display(),
        wstart,
        wend,
        stats.resolved,
        stats.unresolved
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staged_text_write_error_preserves_existing_final() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.txt");
        fs::write(&out, "old\n").unwrap();

        let res: Result<()> = write_text_file_atomic(&out, |w| {
            writeln!(w, "new")?;
            anyhow::bail!("synthetic write failure")
        });

        assert!(res.is_err());
        assert_eq!(fs::read_to_string(&out).unwrap(), "old\n");
        let staging = dir.path().join("_staging");
        let leftovers = fs::read_dir(staging).map(|it| it.count()).unwrap_or(0);
        assert_eq!(leftovers, 0, "failed staged write should be cleaned up");
    }
}
