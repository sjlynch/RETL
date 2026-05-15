//! Subcommand handlers for the `retl` binary. Each `run_*` function
//! corresponds 1:1 to a `Command` variant and is invoked from `main.rs`.

use anyhow::{Context, Result};
use retl::{
    convert_jsonl_to_csv, convert_jsonl_to_tsv, create_dir_all_with_backoff,
    create_new_with_backoff, create_with_backoff, discover_sources_checked,
    discover_upstream_manifests_from_inputs, file_identities, file_identity, for_each_line_cfg,
    format_year_month_ranges, missing_month_diagnostics, open_with_backoff, path_to_stable_string,
    plan_files, plan_files_checked, read_line_capped, remove_with_backoff,
    replace_file_atomic_backoff, total_compressed_size, upstream_manifest_for_directory,
    write_run_manifest, AggregateBuildReport, CorpusAvailability, CorpusLocalStatus, CorpusManifest,
    CorpusPlanItem, CorpusSnapshot, ExportFormat, FileIdentity, FileKind, IntegrityMode,
    KeyExtractor, ManifestDestination, ParentIds, ParentPayloadSpec, RedditETL, RunManifestInput,
    RunManifestStart, Sources, TabularExportOptions, YearMonth, DEFAULT_MAX_LINE_BYTES,
};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
#[cfg(test)]
use std::fs;
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::bin_args::{
    AggregateArgs, CommonOpts, ConvertArgs, ConvertFmt, CorpusArgs, CorpusCommand,
    CorpusManifestArgs, CorpusPlanArgs, CorpusPlanFmt, CountArgs, CountMode, DedupeArgs,
    DescribeArgs, ExportArgs, ExportFmt, FirstSeenArgs, IntegrityArgs, IntegrityModeArg,
    ParentIdKindArg, ParentsArgs, QueryOpts, QuickstartArgs, SampleArgs, ScanArgs, SchemaArgs,
    SchemaFmt, SourceArg,
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

#[derive(Serialize)]
struct CorpusPlanDocument {
    manifest_version: u32,
    manifest_name: Option<String>,
    source: String,
    start: YearMonth,
    end: YearMonth,
    dest: PathBuf,
    summary: CorpusPlanSummary,
    items: Vec<CorpusPlanItem>,
    next_steps: Vec<String>,
}

#[derive(Default, Clone, Debug, Serialize)]
struct CorpusPlanSummary {
    total_items: usize,
    available_items: usize,
    unavailable_items: usize,
    local_present: usize,
    local_missing: usize,
    local_inaccessible: usize,
    size_mismatches: usize,
    checksum_mismatches: usize,
    known_expected_compressed_bytes: u64,
}

impl CorpusPlanSummary {
    fn from_items(items: &[CorpusPlanItem]) -> Self {
        let mut summary = Self {
            total_items: items.len(),
            ..Self::default()
        };
        for item in items {
            match item.availability {
                CorpusAvailability::Available => summary.available_items += 1,
                CorpusAvailability::Unavailable => summary.unavailable_items += 1,
            }
            if item.availability == CorpusAvailability::Available {
                if let Some(bytes) = item.compressed_bytes {
                    summary.known_expected_compressed_bytes = summary
                        .known_expected_compressed_bytes
                        .saturating_add(bytes);
                }
            }
            match &item.local {
                CorpusLocalStatus::Missing => summary.local_missing += 1,
                CorpusLocalStatus::Inaccessible { .. } => summary.local_inaccessible += 1,
                CorpusLocalStatus::Present {
                    size_matches,
                    sha256_matches,
                    ..
                } => {
                    summary.local_present += 1;
                    if matches!(size_matches, Some(false)) {
                        summary.size_mismatches += 1;
                    }
                    if matches!(sha256_matches, Some(false)) {
                        summary.checksum_mismatches += 1;
                    }
                }
            }
        }
        summary
    }
}

pub(crate) fn run_corpus(args: CorpusArgs) -> Result<()> {
    match args.command {
        CorpusCommand::Plan(plan) => run_corpus_plan(plan),
        CorpusCommand::Manifest(manifest) => run_corpus_manifest(manifest),
    }
}

fn run_corpus_plan(args: CorpusPlanArgs) -> Result<()> {
    let manifest = load_corpus_manifest(args.manifest.as_deref())?;
    let sources = Sources::from(args.source);
    let mut items = manifest
        .plan(
            sources,
            args.start,
            args.end,
            &args.dest,
            args.verify_checksums,
        )
        .with_context(|| "building corpus acquisition plan")?;
    if args.only_missing {
        items.retain(CorpusPlanItem::needs_download);
    }
    let summary = CorpusPlanSummary::from_items(&items);

    match args.format {
        CorpusPlanFmt::Json => {
            let doc = CorpusPlanDocument {
                manifest_version: manifest.version,
                manifest_name: manifest.name.clone(),
                source: args.source.label().to_string(),
                start: args.start,
                end: args.end,
                dest: args.dest.clone(),
                summary,
                items,
                next_steps: corpus_plan_next_steps(&args),
            };
            write_corpus_plan_json(&args.out, &doc)?;
        }
        CorpusPlanFmt::Tsv => write_corpus_plan_tsv(&args.out, &items)?,
    }
    Ok(())
}

fn run_corpus_manifest(args: CorpusManifestArgs) -> Result<()> {
    write_text_or_stdout(&args.out, |w| {
        w.write_all(CorpusManifest::builtin_json().as_bytes())?;
        Ok(())
    })
}

fn load_corpus_manifest(path: Option<&Path>) -> Result<CorpusManifest> {
    match path {
        Some(path) => {
            let file = open_with_backoff(path, 16, 50)
                .with_context(|| format!("opening corpus manifest {}", path.display()))?;
            let reader = BufReader::new(file);
            CorpusManifest::from_reader(reader)
                .with_context(|| format!("parsing corpus manifest {}", path.display()))
        }
        None => CorpusManifest::builtin().with_context(|| "parsing built-in corpus manifest"),
    }
}

fn write_text_or_stdout<T, F>(out: &Path, body: F) -> Result<T>
where
    F: FnOnce(&mut dyn Write) -> Result<T>,
{
    if out == Path::new("-") {
        let stdout = io::stdout();
        let mut w = BufWriter::new(stdout.lock());
        let result = body(&mut w)?;
        w.flush()?;
        Ok(result)
    } else {
        write_text_file_atomic(out, body)
    }
}

fn write_corpus_plan_json(out: &Path, doc: &CorpusPlanDocument) -> Result<()> {
    write_text_or_stdout(out, |w| {
        serde_json::to_writer_pretty(&mut *w, doc)?;
        writeln!(w)?;
        Ok(())
    })
}

fn write_corpus_plan_tsv(out: &Path, items: &[CorpusPlanItem]) -> Result<()> {
    write_text_or_stdout(out, |w| {
        writeln!(
            w,
            "source\tmonth\tavailability\tlocal_status\tfile_name\texpected_path\tcompressed_bytes\tactual_bytes\tsize_matches\tsha256\tsha256_matches\turl\ttorrent\tnote"
        )?;
        for item in items {
            let (local_status, actual_bytes, size_matches, sha256_matches) =
                local_status_cells(&item.local);
            writeln!(
                w,
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                item.source.label(),
                item.month,
                availability_cell(item.availability),
                local_status,
                tsv_cell(&item.file_name),
                tsv_cell(&item.expected_path.display().to_string()),
                opt_u64_cell(item.compressed_bytes),
                actual_bytes.map(|n| n.to_string()).unwrap_or_default(),
                opt_bool_cell(size_matches),
                tsv_cell(item.sha256.as_deref().unwrap_or("")),
                opt_bool_cell(sha256_matches),
                tsv_cell(item.url.as_deref().unwrap_or("")),
                tsv_cell(item.torrent.as_deref().unwrap_or("")),
                tsv_cell(item.note.as_deref().unwrap_or("")),
            )?;
        }
        Ok(())
    })
}

fn corpus_plan_next_steps(args: &CorpusPlanArgs) -> Vec<String> {
    let mut steps = vec![
        "Download each item with availability=available and local.status=missing to expected_path. RETL does not yet perform direct downloads.".to_string(),
        format!(
            "After downloading, run: retl describe --expected --data-dir {} --source {} --start {} --end {}",
            args.dest.display(),
            args.source.label(),
            args.start,
            args.end
        ),
        format!(
            "Then validate zstd payloads: retl integrity --expected --mode full --data-dir {} --source {} --start {} --end {}",
            args.dest.display(),
            args.source.label(),
            args.start,
            args.end
        ),
    ];
    if args.manifest.is_some() {
        steps.push(
            "Pass the same --manifest path to describe/integrity when you want RETL to use custom sizes or checksums.".to_string(),
        );
    }
    steps
}

fn local_status_cells(
    local: &CorpusLocalStatus,
) -> (&'static str, Option<u64>, Option<bool>, Option<bool>) {
    match local {
        CorpusLocalStatus::Missing => ("missing", None, None, None),
        CorpusLocalStatus::Inaccessible { .. } => ("inaccessible", None, None, None),
        CorpusLocalStatus::Present {
            actual_bytes,
            size_matches,
            sha256_matches,
            ..
        } => (
            "present",
            Some(*actual_bytes),
            *size_matches,
            *sha256_matches,
        ),
    }
}

fn availability_cell(availability: CorpusAvailability) -> &'static str {
    match availability {
        CorpusAvailability::Available => "available",
        CorpusAvailability::Unavailable => "unavailable",
    }
}

fn opt_u64_cell(n: Option<u64>) -> String {
    n.map(|n| n.to_string()).unwrap_or_default()
}

fn opt_bool_cell(v: Option<bool>) -> &'static str {
    match v {
        Some(true) => "true",
        Some(false) => "false",
        None => "",
    }
}

fn tsv_cell(raw: &str) -> String {
    raw.chars()
        .map(|c| match c {
            '\t' | '\r' | '\n' => ' ',
            other => other,
        })
        .collect()
}

fn counts_map(entries: &[(&str, u64)]) -> BTreeMap<String, u64> {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_string(), *value))
        .collect()
}

fn normalize_cli_values(values: &[String], strip_subreddit_prefix: bool) -> Vec<String> {
    let mut out: Vec<String> = values
        .iter()
        .filter_map(|value| {
            let mut normalized = value.trim().to_lowercase();
            if strip_subreddit_prefix {
                if let Some(rest) = normalized.strip_prefix("r/") {
                    normalized = rest.to_string();
                }
            }
            (!normalized.is_empty()).then_some(normalized)
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

fn source_arg_manifest_label(source: SourceArg) -> &'static str {
    match source {
        SourceArg::Rc => "comments",
        SourceArg::Rs => "submissions",
        SourceArg::Both => "both",
    }
}

fn file_kind_manifest_label(kind: FileKind) -> &'static str {
    match kind {
        FileKind::Comment => "comment",
        FileKind::Submission => "submission",
    }
}

fn cli_selected_file_identities(common: &CommonOpts) -> Result<Vec<FileIdentity>> {
    let comments_dir = common.data_dir.join("comments");
    let submissions_dir = common.data_dir.join("submissions");
    let discovered = discover_sources_checked(
        &comments_dir,
        &submissions_dir,
        Sources::from(common.source),
    )?;
    let jobs = plan_files_checked(
        &discovered,
        &comments_dir,
        &submissions_dir,
        Sources::from(common.source),
        common.start,
        common.end,
    )?;
    let mut identities: Vec<FileIdentity> = jobs
        .iter()
        .map(|job| {
            let mut identity = file_identity(&job.path);
            identity.kind = Some(file_kind_manifest_label(job.kind).to_string());
            identity.month = Some(job.ym.to_string());
            identity
        })
        .collect();
    identities.sort_by(|a, b| {
        a.kind
            .cmp(&b.kind)
            .then_with(|| a.month.cmp(&b.month))
            .then_with(|| a.path.cmp(&b.path))
    });
    Ok(identities)
}

fn cli_corpus_snapshot(common: &CommonOpts) -> Result<CorpusSnapshot> {
    Ok(CorpusSnapshot {
        base_dir: Some(path_to_stable_string(&common.data_dir)),
        comments_dir: Some(path_to_stable_string(&common.data_dir.join("comments"))),
        submissions_dir: Some(path_to_stable_string(&common.data_dir.join("submissions"))),
        sources: Some(source_arg_manifest_label(common.source).to_string()),
        selected_files: cli_selected_file_identities(common)?,
    })
}

fn cli_query_value(common: &CommonOpts, query: &QueryOpts) -> Value {
    serde_json::json!({
        "subreddits": normalize_cli_values(&common.subreddits, true),
        "authors_in": normalize_cli_values(&query.authors, false),
        "authors_out": normalize_cli_values(&query.exclude_authors, false),
        "exclude_common_bots": query.exclude_common_bots,
        "author_regex": query.author_regex.as_deref(),
        "min_score": query.min_score,
        "max_score": query.max_score,
        "keywords_any": normalize_cli_values(&query.keywords, false),
        "domains_in": normalize_cli_values(&query.domains, false),
        "contains_url": query.contains_url.then_some(true),
        "json_predicates": &query.json_predicates,
        "filter_pseudo_users": !common.include_deleted,
    })
}

fn cli_common_options_value(common: &CommonOpts, extra: Value) -> Value {
    serde_json::json!({
        "data_dir": path_to_stable_string(&common.data_dir),
        "work_dir": path_to_stable_string(&common.work_dir),
        "start": common.start.map(|ym| ym.to_string()),
        "end": common.end.map(|ym| ym.to_string()),
        "source": source_arg_manifest_label(common.source),
        "parallelism": common.parallelism,
        "file_concurrency": common.file_concurrency,
        "progress": !common.no_progress,
        "allow_partial": common.allow_partial,
        "emit_manifest": !common.no_manifest,
        "extra": extra,
    })
}

#[allow(clippy::too_many_arguments)]
fn write_cli_scan_manifest_for_file(
    start: RunManifestStart,
    operation: &str,
    command: &str,
    common: &CommonOpts,
    query: &QueryOpts,
    out_path: &Path,
    output_format: &str,
    counts: BTreeMap<String, u64>,
    partial_reporter: &retl::PartialReadReporter,
    extra_options: Value,
) -> Result<()> {
    if common.no_manifest || out_path == Path::new("-") {
        return Ok(());
    }
    let mut manifest = RunManifestInput::new(operation);
    manifest.start = start;
    manifest.command = Some(command.to_string());
    manifest.query = cli_query_value(common, query);
    manifest.options = cli_common_options_value(common, extra_options);
    manifest.corpus = cli_corpus_snapshot(common)?;
    manifest.output_format = output_format.to_string();
    manifest.counts = counts;
    manifest.partial_read = partial_reporter.snapshot();
    write_run_manifest(manifest, ManifestDestination::File(out_path.to_path_buf()))?;
    Ok(())
}

pub(crate) fn run_describe(args: DescribeArgs) -> Result<()> {
    if args.schema {
        if args.expected || args.manifest.is_some() {
            anyhow::bail!("describe --schema cannot be combined with --expected/--manifest");
        }
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

    if args.expected || args.manifest.is_some() {
        emit_manifest_describe_comparison(
            &args.data_dir,
            args.source,
            args.start,
            args.end,
            args.manifest.as_deref(),
        )?;
    }
    Ok(())
}

#[derive(Default)]
struct DescribeManifestSummary {
    desired: usize,
    available: usize,
    unavailable: usize,
    local_present: usize,
    local_missing: usize,
    local_inaccessible: usize,
    size_mismatches: usize,
    checksum_mismatches: usize,
    known_expected_compressed_bytes: u64,
    missing_months: Vec<YearMonth>,
    unavailable_months: Vec<YearMonth>,
}

fn emit_manifest_describe_comparison(
    data_dir: &Path,
    source: SourceArg,
    start: Option<YearMonth>,
    end: Option<YearMonth>,
    manifest_path: Option<&Path>,
) -> Result<()> {
    let (start, end) = required_manifest_range(start, end)?;
    let manifest = load_corpus_manifest(manifest_path)?;
    let rows = manifest
        .plan(Sources::from(source), start, end, data_dir, false)
        .with_context(|| "building manifest comparison for describe")?;
    let mut by_source = BTreeMap::<String, DescribeManifestSummary>::new();
    for item in rows {
        let entry = by_source
            .entry(item.source.label().to_string())
            .or_default();
        entry.desired += 1;
        match item.availability {
            CorpusAvailability::Available => {
                entry.available += 1;
                if let Some(bytes) = item.compressed_bytes {
                    entry.known_expected_compressed_bytes =
                        entry.known_expected_compressed_bytes.saturating_add(bytes);
                }
            }
            CorpusAvailability::Unavailable => {
                entry.unavailable += 1;
                entry.unavailable_months.push(item.month);
            }
        }
        match item.local {
            CorpusLocalStatus::Missing => {
                entry.local_missing += 1;
                if item.availability == CorpusAvailability::Available {
                    entry.missing_months.push(item.month);
                }
            }
            CorpusLocalStatus::Inaccessible { .. } => entry.local_inaccessible += 1,
            CorpusLocalStatus::Present {
                size_matches,
                sha256_matches,
                ..
            } => {
                entry.local_present += 1;
                if matches!(size_matches, Some(false)) {
                    entry.size_mismatches += 1;
                }
                if matches!(sha256_matches, Some(false)) {
                    entry.checksum_mismatches += 1;
                }
            }
        }
    }

    let stdout = io::stdout();
    let mut w = BufWriter::new(stdout.lock());
    writeln!(w)?;
    writeln!(
        w,
        "manifest_source\tdesired\tavailable\tunavailable\tlocal_present\tlocal_missing\tlocal_inaccessible\tsize_mismatches\tchecksum_mismatches\tmissing_months\tunavailable_months\tknown_expected_compressed_bytes"
    )?;
    for (source, summary) in by_source {
        writeln!(
            w,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            source,
            summary.desired,
            summary.available,
            summary.unavailable,
            summary.local_present,
            summary.local_missing,
            summary.local_inaccessible,
            summary.size_mismatches,
            summary.checksum_mismatches,
            format_year_month_ranges(&summary.missing_months),
            format_year_month_ranges(&summary.unavailable_months),
            summary.known_expected_compressed_bytes,
        )?;
    }
    w.flush()?;
    Ok(())
}

fn required_manifest_range(
    start: Option<YearMonth>,
    end: Option<YearMonth>,
) -> Result<(YearMonth, YearMonth)> {
    match (start, end) {
        (Some(start), Some(end)) if start <= end => Ok((start, end)),
        (Some(start), Some(end)) => {
            anyhow::bail!("invalid date range: start {start} is after end {end}")
        }
        _ => anyhow::bail!(
            "manifest comparison requires both --start YYYY-MM and --end YYYY-MM so RETL knows the desired corpus range"
        ),
    }
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
            let written = write_text_file_atomic(&path, |w| {
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
    let to_stdout = args.out == Path::new("-");
    if to_stdout {
        etl = etl.run_manifest(false);
    }
    let partial_reporter = etl.partial_read_reporter();
    let work_dir = args.common.work_dir.clone();
    let mut scan = plan!(etl, args.common, args.query);
    if let Some(limit) = args.limit {
        scan = scan.limit(limit);
    }

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
    let to_stdout = args.out == Path::new("-");
    if to_stdout {
        etl = etl.run_manifest(false);
    }
    let partial_reporter = etl.partial_read_reporter();
    let work_dir = args.common.work_dir.clone();
    let scan = plan!(etl, args.common, args.query).limit(args.limit);

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
        let stem = match args.format {
            ConvertFmt::Csv => "convert.csv",
            ConvertFmt::Tsv => "convert.tsv",
        };
        stream_path_output_to_stdout(&args.work_dir, "convert", stem, |path| {
            write(path).map(|_| ())
        })?;
    } else {
        write(&args.out)?;
    }
    Ok(())
}

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

pub(crate) fn run_integrity(args: IntegrityArgs) -> Result<()> {
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
    if args.resume {
        anyhow::bail!(
            "retl aggregate does not support --resume because it reduces existing JSONL inputs. For resumable corpus filtering, run `retl export --format spool --resume ...` first, then aggregate with `retl aggregate --spool <DIR>`."
        );
    }
    let manifest_start = RunManifestStart::now();
    let mut etl = RedditETL::new()
        .progress(!args.runtime.no_progress)
        .run_manifest(!args.runtime.no_manifest);
    if let Some(p) = args.runtime.parallelism {
        etl = etl.parallelism(p);
    }

    let inputs = resolve_aggregate_inputs(&args)?;
    let manifest_inputs = inputs.clone();
    let input_count = inputs.len();

    let shards_dir = args.shards_dir.clone().unwrap_or_else(|| {
        args.out
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join("agg_shards")
    });
    create_dir_all_with_backoff(&shards_dir, 16, 50)
        .with_context(|| format!("creating shards_dir {}", shards_dir.display()))?;
    if let Some(by) = args.by.clone() {
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
        let mut counts = counts_map(&[
            ("input_files", input_count as u64),
            ("ok_inputs", report.ok_inputs.len() as u64),
            ("partial_inputs", report.partial_count() as u64),
            ("fatal_inputs", report.fatal_count() as u64),
            ("merged_shards", report.merged_shards as u64),
            ("output_rows", agg.rows(None).len() as u64),
        ]);
        if let Some(top) = args.top {
            counts.insert("top".to_string(), top as u64);
        }
        let warnings = aggregate_manifest_warnings(&report);
        write_cli_aggregate_manifest(
            manifest_start,
            &args,
            &shards_dir,
            &manifest_inputs,
            "tsv",
            counts,
            warnings,
        )?;
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
        let counts = counts_map(&[
            ("input_files", input_count as u64),
            ("ok_inputs", report.ok_inputs.len() as u64),
            ("partial_inputs", report.partial_count() as u64),
            ("fatal_inputs", report.fatal_count() as u64),
            ("merged_shards", report.merged_shards as u64),
            ("records_aggregated", agg.count),
        ]);
        let warnings = aggregate_manifest_warnings(&report);
        write_cli_aggregate_manifest(
            manifest_start,
            &args,
            &shards_dir,
            &manifest_inputs,
            "json",
            counts,
            warnings,
        )?;
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

fn aggregate_manifest_warnings(report: &AggregateBuildReport) -> Vec<String> {
    let mut warnings = Vec::new();
    if report.fatal_count() > 0 {
        warnings.push(format!(
            "{} aggregate input(s) failed during shard build",
            report.fatal_count()
        ));
    }
    if report.partial_count() > 0 {
        warnings.push(format!(
            "{} aggregate input(s) were skipped after partial read",
            report.partial_count()
        ));
    }
    warnings
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

#[allow(clippy::too_many_arguments)]
fn write_cli_aggregate_manifest(
    start: RunManifestStart,
    args: &AggregateArgs,
    shards_dir: &Path,
    inputs: &[PathBuf],
    output_format: &str,
    counts: BTreeMap<String, u64>,
    warnings: Vec<String>,
) -> Result<()> {
    if args.runtime.no_manifest {
        return Ok(());
    }
    let mut manifest = RunManifestInput::new("cli.aggregate");
    manifest.start = start;
    manifest.command = Some("retl aggregate".to_string());
    manifest.options = serde_json::json!({
        "spool": args.spool.as_ref().map(|p| path_to_stable_string(p)),
        "explicit_inputs": args.inputs.iter().map(|p| path_to_stable_string(p)).collect::<Vec<_>>(),
        "shards_dir": path_to_stable_string(shards_dir),
        "pretty": args.pretty,
        "by": args.by.as_deref(),
        "metric": args.metric.as_deref(),
        "top": args.top,
        "scientific": args.scientific,
        "parallelism": args.runtime.parallelism,
        "progress": !args.runtime.no_progress,
        "emit_manifest": !args.runtime.no_manifest,
    });
    manifest.inputs = file_identities(inputs);
    manifest.output_format = output_format.to_string();
    manifest.counts = counts;
    manifest.warnings = warnings;
    manifest.upstream_manifests = discover_upstream_manifests_from_inputs(inputs);
    write_run_manifest(manifest, ManifestDestination::File(args.out.clone()))?;
    Ok(())
}

pub(crate) fn run_first_seen(args: FirstSeenArgs) -> Result<()> {
    let mut etl = build_etl(&args.common)?;
    if args.resume {
        etl = etl.resume(true);
    }
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

fn parent_payload_spec_from_args(args: &ParentsArgs) -> ParentPayloadSpec {
    if args.parent_full {
        ParentPayloadSpec::full_record()
    } else if args.parent_fields.is_empty() {
        ParentPayloadSpec::default()
    } else {
        ParentPayloadSpec::from_fields(&args.parent_fields)
    }
}

fn ensure_parent_work_dirs(args: &ParentsArgs) -> Result<PathBuf> {
    create_dir_all_with_backoff(&args.cache, 16, 50)
        .with_context(|| format!("creating cache dir {}", args.cache.display()))?;
    create_dir_all_with_backoff(&args.work_dir, 16, 50)
        .with_context(|| format!("creating work_dir {}", args.work_dir.display()))?;
    let lib_tmp = args.work_dir.join("lib_tmp");
    create_dir_all_with_backoff(&lib_tmp, 16, 50)
        .with_context(|| format!("creating work_dir {}", lib_tmp.display()))?;
    Ok(lib_tmp)
}

fn build_parent_etl(
    args: &ParentsArgs,
    lib_tmp: &Path,
    parent_payload_spec: &ParentPayloadSpec,
    sources: Option<Sources>,
    start: Option<YearMonth>,
    end: Option<YearMonth>,
) -> RedditETL {
    let mut etl = RedditETL::new()
        .base_dir(&args.data_dir)
        .work_dir(lib_tmp)
        .parent_payload_spec(parent_payload_spec.clone())
        .progress(!args.no_progress)
        .run_manifest(!args.no_manifest);
    if let Some(s) = sources {
        etl = etl.sources(s);
    }
    if start.is_some() || end.is_some() {
        etl = etl.date_range(start, end);
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
}

fn validate_parent_bare_id(raw: &str, source: &str) -> Result<String> {
    let id = raw.trim();
    if id.is_empty() {
        anyhow::bail!("empty parent ID in {source}");
    }
    if id.chars().any(char::is_whitespace) {
        anyhow::bail!("parent ID in {source} contains whitespace: {id:?}");
    }
    Ok(id.to_string())
}

fn unsupported_fullname_prefix(id: &str) -> Option<String> {
    let (prefix, _) = id.split_once('_')?;
    let bytes = prefix.as_bytes();
    if bytes.len() == 2 && bytes[0] == b't' && bytes[1].is_ascii_digit() {
        Some(format!("{prefix}_"))
    } else {
        None
    }
}

fn direct_parent_kind_prefix(kind: ParentIdKindArg) -> &'static str {
    match kind {
        ParentIdKindArg::Comment => "t1_",
        ParentIdKindArg::Submission => "t3_",
    }
}

fn direct_parent_kind_label(kind: ParentIdKindArg) -> &'static str {
    match kind {
        ParentIdKindArg::Comment => "comment",
        ParentIdKindArg::Submission => "submission",
    }
}

fn normalize_direct_parent_id(
    raw: &str,
    bare_kind: Option<ParentIdKindArg>,
    source: &str,
) -> Result<(ParentIdKindArg, String, String)> {
    let id = raw.trim();
    if id.is_empty() {
        anyhow::bail!("empty parent ID in {source}");
    }

    let (kind, bare) = if let Some(rest) = id.strip_prefix("t1_") {
        (
            ParentIdKindArg::Comment,
            validate_parent_bare_id(rest, source)?,
        )
    } else if let Some(rest) = id.strip_prefix("t3_") {
        (
            ParentIdKindArg::Submission,
            validate_parent_bare_id(rest, source)?,
        )
    } else {
        if let Some(prefix) = unsupported_fullname_prefix(id) {
            anyhow::bail!(
                "unsupported parent ID prefix `{prefix}` in {source}; expected `t1_`/`t3_` or a bare ID with --id-kind"
            );
        }
        let kind = bare_kind.ok_or_else(|| {
            anyhow::anyhow!(
                "bare parent ID `{id}` in {source} requires --id-kind comment or --id-kind submission"
            )
        })?;
        (kind, validate_parent_bare_id(id, source)?)
    };

    let prefixed = format!("{}{}", direct_parent_kind_prefix(kind), bare);
    Ok((kind, bare, prefixed))
}

fn add_direct_parent_id(
    raw: &str,
    bare_kind: Option<ParentIdKindArg>,
    source: &str,
    ids: &mut ParentIds,
    ordered_prefixed: &mut Vec<String>,
    seen: &mut HashSet<String>,
) -> Result<()> {
    let (kind, bare, prefixed) = normalize_direct_parent_id(raw, bare_kind, source)?;
    let already_seen = seen.contains(&prefixed);
    let inserted = match kind {
        ParentIdKindArg::Comment => ids.insert_t1(&bare),
        ParentIdKindArg::Submission => ids.insert_t3(&bare),
    };
    if !inserted && !already_seen {
        anyhow::bail!(
            "invalid {} parent ID `{}` in {source}",
            direct_parent_kind_label(kind),
            bare
        );
    }
    if !already_seen {
        seen.insert(prefixed.clone());
        ordered_prefixed.push(prefixed);
    }
    Ok(())
}

fn collect_direct_parent_ids(args: &ParentsArgs) -> Result<(ParentIds, Vec<String>)> {
    let mut ids = ParentIds::new();
    let mut ordered_prefixed = Vec::new();
    let mut seen = HashSet::new();

    for path in &args.ids_file {
        let f = open_with_backoff(path, 16, 50)
            .with_context(|| format!("opening parent IDs file {}", path.display()))?;
        let mut r = BufReader::new(f);
        let mut buf = String::new();
        let mut line_no = 0_u64;
        loop {
            let n = read_line_capped(&mut r, &mut buf, DEFAULT_MAX_LINE_BYTES, path)
                .with_context(|| format!("reading parent IDs file {}", path.display()))?;
            if n == 0 {
                break;
            }
            line_no += 1;
            let trimmed = buf.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let source = format!("{} line {}", path.display(), line_no);
            add_direct_parent_id(
                trimmed,
                args.id_kind,
                &source,
                &mut ids,
                &mut ordered_prefixed,
                &mut seen,
            )?;
        }
    }

    for (idx, raw) in args.parent_id.iter().enumerate() {
        let source = format!("--parent-id #{}", idx + 1);
        add_direct_parent_id(
            raw,
            args.id_kind,
            &source,
            &mut ids,
            &mut ordered_prefixed,
            &mut seen,
        )?;
    }

    Ok((ids, ordered_prefixed))
}

fn direct_resolution_range_label(start: Option<YearMonth>, end: Option<YearMonth>) -> String {
    match (start, end) {
        (Some(s), Some(e)) => format!("{s}..={e}"),
        (Some(s), None) => format!("{s}..=latest discovered month"),
        (None, Some(e)) => format!("earliest discovered month..={e}"),
        (None, None) => "all discovered months".to_string(),
    }
}

pub(crate) fn run_parents(args: ParentsArgs) -> Result<()> {
    let has_spool = args.spool.is_some();
    let has_direct_ids = !args.ids_file.is_empty() || !args.parent_id.is_empty();

    match (has_spool, has_direct_ids) {
        (true, true) => anyhow::bail!(
            "choose exactly one parents input mode: --spool <DIR> or --ids-file/--parent-id"
        ),
        (false, false) => {
            anyhow::bail!("parents requires an input mode: --spool <DIR> or --ids-file/--parent-id")
        }
        (true, false) => run_parents_spool(args),
        (false, true) => run_parents_direct(args),
    }
}

fn run_parents_spool(args: ParentsArgs) -> Result<()> {
    if args.start.is_some() || args.end.is_some() {
        anyhow::bail!("--start/--end are only used with direct parent IDs; spool mode uses --window-months around the discovered spool range");
    }
    if args.id_kind.is_some() {
        anyhow::bail!("--id-kind is only used with --ids-file/--parent-id direct-ID mode");
    }

    let manifest_start = RunManifestStart::now();
    let spool = args
        .spool
        .as_ref()
        .expect("spool mode already validated --spool is present");
    let (spool_parts, min_ym, max_ym) = discover_spool_parts(spool)?;
    let manifest_spool_parts = spool_parts.clone();

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

    create_dir_all_with_backoff(&args.out, 16, 50)
        .with_context(|| format!("creating output dir {}", args.out.display()))?;
    let lib_tmp = ensure_parent_work_dirs(&args)?;
    let parent_payload_spec = parent_payload_spec_from_args(&args);

    let ids = build_parent_etl(&args, &lib_tmp, &parent_payload_spec, None, None, None)
        .collect_parent_ids_from_jsonls(spool_parts.clone())?;
    if ids.is_empty() {
        return bail_empty_parent_ids(&spool_parts);
    }

    let parents = build_parent_etl(
        &args,
        &lib_tmp,
        &parent_payload_spec,
        Some(Sources::Both),
        Some(wstart),
        Some(wend),
    )
    .resolve_parent_maps(&ids, &args.cache, args.resume)?;
    let (attached, stats) = build_parent_etl(
        &args,
        &lib_tmp,
        &parent_payload_spec,
        None,
        Some(wstart),
        Some(wend),
    )
    .attach_parents_jsonls_parallel_with_stats(
        spool_parts,
        &args.out,
        &parents,
        args.resume,
    )?;

    if !args.no_manifest {
        let mut warnings = Vec::new();
        if stats.total() > 0 && stats.unresolved_rate() > 0.05 {
            warnings.push(format!(
                "more than 5% of parent lookups were unresolved ({:.2}%)",
                stats.unresolved_rate() * 100.0
            ));
        }
        let mut manifest = RunManifestInput::new("cli.parents");
        manifest.start = manifest_start;
        manifest.command = Some("retl parents".to_string());
        manifest.options = serde_json::json!({
            "spool": path_to_stable_string(spool),
            "cache": path_to_stable_string(&args.cache),
            "out": path_to_stable_string(&args.out),
            "data_dir": path_to_stable_string(&args.data_dir),
            "work_dir": path_to_stable_string(&args.work_dir),
            "resume": args.resume,
            "window_months": args.window_months,
            "resolved_range": { "start": wstart.to_string(), "end": wend.to_string() },
            "parent_payload": {
                "full_record": parent_payload_spec.is_full_record(),
                "fields": parent_payload_spec.fields(),
            },
            "parallelism": args.parallelism,
            "file_concurrency": args.file_concurrency,
            "inflight_bytes": args.inflight_bytes,
            "inflight_groups": args.inflight_groups,
            "progress": !args.no_progress,
            "emit_manifest": !args.no_manifest,
        });
        manifest.inputs = file_identities(&manifest_spool_parts);
        manifest.output_format = "jsonl-directory".to_string();
        manifest.counts = counts_map(&[
            ("input_files", manifest_spool_parts.len() as u64),
            ("attached_files", attached.len() as u64),
            ("parents_resolved", stats.resolved),
            ("parents_unresolved", stats.unresolved),
        ]);
        manifest.warnings = warnings;
        manifest.upstream_manifests = vec![upstream_manifest_for_directory(spool)];
        write_run_manifest(manifest, ManifestDestination::Directory(args.out.clone()))?;
    }

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

fn run_parents_direct(args: ParentsArgs) -> Result<()> {
    if let (Some(start), Some(end)) = (args.start, args.end) {
        if start > end {
            anyhow::bail!("invalid date range: start {start} is after end {end}");
        }
    }

    let (ids, ordered_prefixed) = collect_direct_parent_ids(&args)?;
    if ids.is_empty() || ordered_prefixed.is_empty() {
        anyhow::bail!("direct parent-ID mode received no usable IDs");
    }

    let lib_tmp = ensure_parent_work_dirs(&args)?;
    let parent_payload_spec = parent_payload_spec_from_args(&args);
    let resolver = build_parent_etl(
        &args,
        &lib_tmp,
        &parent_payload_spec,
        Some(Sources::Both),
        args.start,
        args.end,
    );
    let parents = resolver.resolve_parent_maps(&ids, &args.cache, args.resume)?;
    let stats =
        resolver.write_resolved_parent_payloads_jsonl(ordered_prefixed, &args.out, &parents)?;

    if stats.total() > 0 && stats.unresolved_rate() > 0.05 {
        tracing::warn!(
            resolved = stats.resolved,
            unresolved = stats.unresolved,
            unresolved_rate = stats.unresolved_rate(),
            "more than 5% of direct parent IDs were unresolved; consider --start/--end that cover the parent records or omit the range to scan all discovered months"
        );
    }

    eprintln!(
        "Resolved direct parent IDs to {} (cache {}, range {}; parents resolved={}, unresolved={})",
        args.out.display(),
        args.cache.display(),
        direct_resolution_range_label(args.start, args.end),
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
