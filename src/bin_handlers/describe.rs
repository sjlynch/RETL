
pub(crate) fn run_describe(args: DescribeArgs) -> Result<()> {
    let stdout = io::stdout();
    let mut w = BufWriter::new(stdout.lock());
    run_describe_to(args, &mut w)?;
    w.flush()?;
    Ok(())
}

/// `run_describe` with an explicit writer so in-process tests can capture the
/// TSV output without spawning the `retl` binary. `main.rs` calls
/// `run_describe`; tests call this with a `&mut Vec<u8>` and assert on the
/// captured bytes.
pub(crate) fn run_describe_to(args: DescribeArgs, w: &mut dyn Write) -> Result<()> {
    if args.schema {
        if args.expected || args.manifest.is_some() {
            anyhow::bail!("describe --schema cannot be combined with --expected/--manifest");
        }
        return run_schema_to(
            SchemaArgs {
                data_dir: args.data_dir,
                start: args.start,
                end: args.end,
                source: args.source,
                sample_per_month: args.schema_sample.unwrap_or(100),
                format: args.schema_format.unwrap_or(SchemaFmt::Tsv),
            },
            w,
        );
    }

    // `--schema-sample` / `--schema-format` (and its `--format` alias) only
    // shape the `--schema` output. Without `--schema`, `describe` prints the
    // plain discovery table and would silently ignore them — reject instead so
    // `retl describe --format json` fails loudly rather than emitting TSV.
    if args.schema_sample.is_some() {
        anyhow::bail!(
            "--schema-sample only applies to `retl describe --schema`; add --schema or drop the flag"
        );
    }
    if args.schema_format.is_some() {
        anyhow::bail!(
            "--schema-format/--format only applies to `retl describe --schema`; add --schema or drop the flag"
        );
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

    if args.expected || args.manifest.is_some() {
        emit_manifest_describe_comparison(
            &args.data_dir,
            args.source,
            args.start,
            args.end,
            args.manifest.as_deref(),
            w,
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
    w: &mut dyn Write,
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
