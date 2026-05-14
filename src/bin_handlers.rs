//! Subcommand handlers for the `retl` binary. Each `run_*` function
//! corresponds 1:1 to a `Command` variant and is invoked from `main.rs`.

use anyhow::{Context, Result};
use retl::{
    create_with_backoff, discover_all, format_year_month_ranges, missing_month_diagnostics,
    plan_files, remove_with_backoff, replace_file_atomic_backoff, total_compressed_size,
    ExportFormat, FileKind, IntegrityMode, KeyExtractor, RedditETL, Sources, YearMonth,
};
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::bin_args::{
    AggregateArgs, CountArgs, CountMode, DedupeArgs, DescribeArgs, ExportArgs, ExportFmt,
    FirstSeenArgs, IntegrityArgs, IntegrityModeArg, ParentsArgs, ScanArgs, SourceArg,
};
use crate::bin_helpers::{
    build_etl, discover_spool_parts, emit_partial_read_report, plan, stream_extract_to_stdout,
    stream_path_output_to_stdout, GroupBySpec, GroupMetricAgg, MetricSpec, RecCount,
};

const CLI_TEXT_WRITE_BUF_BYTES: usize = 64 * 1024;
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
    fs::create_dir_all(&staging_dir)
        .with_context(|| format!("creating staging dir {}", staging_dir.display()))?;

    let file_name = final_path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("output path has no file name: {}", final_path.display()))?;
    let mut staged_name = file_name.to_os_string();
    let counter = CLI_STAGE_COUNTER.fetch_add(1, Ordering::Relaxed);
    staged_name.push(format!(".{}.{}.inprogress", std::process::id(), counter));
    let staged = staging_dir.join(staged_name);

    let file = create_with_backoff(&staged, 16, 50)
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
    fs::create_dir_all(parent)
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
    if let (Some(start), Some(end)) = (args.start, args.end) {
        if start > end {
            anyhow::bail!("invalid date range: start {start} is after end {end}");
        }
    }

    let comments_dir = args.data_dir.join("comments");
    let submissions_dir = args.data_dir.join("submissions");
    let discovered = discover_all(&comments_dir, &submissions_dir);

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

pub(crate) fn run_scan(args: ScanArgs) -> Result<()> {
    let etl = build_etl(&args.common)?;
    let partial_reporter = etl.partial_read_reporter();
    let scan = plan!(etl, args.common, args.query);

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
    let partial_reporter = etl.partial_read_reporter();
    let work_dir = args.common.work_dir.clone();
    let scan = plan!(etl, args.common, args.query);

    if args.out == Path::new("-") {
        let tmp_path = stdout_dedupe_path(&work_dir);
        let _ = fs::remove_file(&tmp_path);
        let result = scan.dedupe_keys_to_lines(&key, &tmp_path);
        if let Err(e) = result {
            let _ = fs::remove_file(&tmp_path);
            return Err(e);
        }
        let copy_result = (|| -> Result<()> {
            let mut f = fs::File::open(&tmp_path)
                .with_context(|| format!("opening dedupe tempfile {}", tmp_path.display()))?;
            let stdout = io::stdout();
            let mut w = stdout.lock();
            io::copy(&mut f, &mut w).context("streaming dedupe output to stdout")?;
            w.flush()?;
            Ok(())
        })();
        let _ = fs::remove_file(&tmp_path);
        copy_result?;
    } else {
        scan.dedupe_keys_to_lines(&key, &args.out)?;
    }
    emit_partial_read_report(&partial_reporter)?;
    Ok(())
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
    if let Some(level) = args.zst_level {
        etl = etl.zst_level(level);
    }
    if args.resume {
        etl = etl.resume(true);
    }
    let partial_reporter = etl.partial_read_reporter();
    let work_dir = args.common.work_dir.clone();
    let scan = plan!(etl, args.common, args.query);
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
        ExportFmt::Spool => {
            if to_stdout {
                anyhow::bail!("--out - is not valid for --format spool (it expects a directory)");
            }
            fs::create_dir_all(&args.out)
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

    let shards_dir = args.shards_dir.unwrap_or_else(|| {
        args.out
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join("agg_shards")
    });
    fs::create_dir_all(&shards_dir)
        .with_context(|| format!("creating shards_dir {}", shards_dir.display()))?;

    let input_count = args.inputs.len();
    if let Some(by) = args.by {
        let group_by = GroupBySpec::parse(&by)?;
        let metric = MetricSpec::parse(args.metric.as_deref())?;
        let top = args.top;
        let (agg, built, errors) = etl
            .aggregate_jsonls_parallel_collect_with::<GroupMetricAgg, _>(
                args.inputs,
                &shards_dir,
                || GroupMetricAgg::new(group_by.clone(), metric.clone()),
            )?;
        ensure_aggregate_inputs_succeeded(input_count, built, errors)?;
        write_grouped_tsv(&args.out, agg.rows(top))?;
        eprintln!(
            "Aggregated {} shard(s) to TSV; {} input(s) failed during shard build",
            built, errors
        );
    } else {
        if args.metric.is_some() {
            anyhow::bail!("--metric requires --by");
        }
        if args.top.is_some() {
            anyhow::bail!("--top requires --by");
        }
        let (agg, built, errors) =
            etl.aggregate_jsonls_parallel_collect::<RecCount>(args.inputs, &shards_dir)?;
        ensure_aggregate_inputs_succeeded(input_count, built, errors)?;
        write_rec_count_json(&args.out, &agg, args.pretty)?;
        eprintln!(
            "Aggregated {} shard(s); {} input(s) failed during shard build",
            built, errors
        );
    }
    Ok(())
}

fn ensure_aggregate_inputs_succeeded(
    input_count: usize,
    built: usize,
    errors: usize,
) -> Result<()> {
    if input_count > 0 && (errors == input_count || built == 0) {
        anyhow::bail!(
            "aggregate failed: {errors} of {input_count} input(s) failed during shard build; {built} shard(s) merged"
        );
    }
    Ok(())
}

fn write_rec_count_json(out: &Path, agg: &RecCount, pretty: bool) -> Result<()> {
    let tmp = out.with_extension("json.inprogress");
    {
        let f = create_with_backoff(&tmp, 16, 50)
            .with_context(|| format!("creating output tempfile {}", tmp.display()))?;
        let mut w = BufWriter::new(f);
        if pretty {
            serde_json::to_writer_pretty(&mut w, agg)?;
        } else {
            serde_json::to_writer(&mut w, agg)?;
        }
        w.flush()?;
    }
    replace_file_atomic_backoff(&tmp, out)
        .with_context(|| format!("publishing output file {}", out.display()))?;
    Ok(())
}

fn write_grouped_tsv(out: &Path, rows: Vec<(String, String)>) -> Result<()> {
    let tmp = out.with_extension("tsv.inprogress");
    {
        let f = create_with_backoff(&tmp, 16, 50)
            .with_context(|| format!("creating output tempfile {}", tmp.display()))?;
        let mut w = BufWriter::new(f);
        for (key, value) in rows {
            writeln!(w, "{key}\t{value}")?;
        }
        w.flush()?;
    }
    replace_file_atomic_backoff(&tmp, out)
        .with_context(|| format!("publishing output file {}", out.display()))?;
    Ok(())
}

pub(crate) fn run_first_seen(args: FirstSeenArgs) -> Result<()> {
    let etl = build_etl(&args.common)?;
    let partial_reporter = etl.partial_read_reporter();
    let scan = plan!(etl, args.common, args.query);
    scan.build_first_seen_index_to_tsv(&args.out)?;
    emit_partial_read_report(&partial_reporter)?;
    Ok(())
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

    fs::create_dir_all(&args.cache)
        .with_context(|| format!("creating cache dir {}", args.cache.display()))?;
    fs::create_dir_all(&args.out)
        .with_context(|| format!("creating output dir {}", args.out.display()))?;
    fs::create_dir_all(&args.work_dir)
        .with_context(|| format!("creating work_dir {}", args.work_dir.display()))?;
    let lib_tmp = args.work_dir.join("lib_tmp");
    fs::create_dir_all(&lib_tmp)
        .with_context(|| format!("creating work_dir {}", lib_tmp.display()))?;

    let build = |sources: Option<Sources>, range: Option<(YearMonth, YearMonth)>| -> RedditETL {
        let mut etl = RedditETL::new()
            .base_dir(&args.data_dir)
            .work_dir(&lib_tmp)
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
        etl
    };

    let ids = build(None, None).collect_parent_ids_from_jsonls(spool_parts.clone())?;
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
