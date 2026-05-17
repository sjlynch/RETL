// Orchestration for tabular (CSV/TSV) exports. The actual field-selector
// parsing and row-writing helpers live in `tabular_format/{selector,writer}.rs`
// and are spliced into this module by `mod.rs`.

fn tabular_part_path(tmp_dir: &Path, key: &str, format: TabularFormat) -> PathBuf {
    tmp_dir.join(format!(".part_{}{}", key, format.row_suffix()))
}

fn tabular_part_paths(tmp_dir: &Path, format: TabularFormat) -> Result<Vec<PathBuf>> {
    let suffix = format.row_suffix();
    let mut paths = Vec::new();
    for entry in crate::util::read_dir_with_default_backoff(tmp_dir)
        .with_context(|| format!("read_dir {}", tmp_dir.display()))?
    {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if name.starts_with(".part_") && name.ends_with(suffix) {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

fn stitch_tabular_parts(
    tmp_dir: &Path,
    out_path: &Path,
    fields: &[String],
    opts: TabularExportOptions,
    format: TabularFormat,
    write_buf: usize,
) -> Result<()> {
    let parts = tabular_part_paths(tmp_dir, format)?;
    write_at_path_atomic(out_path, write_buf, |out| {
        if opts.header {
            write_tabular_header(out, fields, format)?;
        }
        for path in &parts {
            let mut r = BufReader::new(crate::util::open_with_default_backoff(path)?);
            std::io::copy(&mut r, out)?;
        }
        Ok(())
    })
}

#[allow(clippy::too_many_arguments)]
fn stream_tabular_job<W: Write + ?Sized>(
    job: &FileJob,
    writer: &mut W,
    targets: Option<&Vec<String>>,
    query: &QuerySpec,
    fields: &[String],
    selectors: &[TabularFieldSelector],
    format: TabularFormat,
    pb: Option<ProgressBar>,
    bounds: Option<DateBounds>,
    read_buf_bytes: usize,
    whitelist_tracker: Option<&WhitelistMatchTracker>,
    allow_partial: bool,
    partial_reporter: Option<&crate::config::PartialReadReporter>,
    record_limit: Option<&RecordLimit>,
) -> Result<StreamJobResult> {
    let mut written = 0_u64;
    let mut line_number = 0_u64;
    let mut on_line = |line: &str| -> Result<()> {
        line_number += 1;
        let min = match parse_minimal(line) {
            Ok(min) => min,
            Err(_) => match serde_json::from_str::<Value>(line) {
                Ok(_) => return Ok(()),
                Err(e) => return Err(malformed_json_error(&job.path, line_number, e)),
            },
        };
        if !matches_minimal(&min, targets, query, job.kind) || !within_bounds(&min, bounds) {
            return Ok(());
        }
        if query.requires_full_parse() {
            let val: Value = serde_json::from_str(line)
                .map_err(|e| malformed_json_error(&job.path, line_number, e))?;
            if !matches_full(&val, job.kind, query) {
                return Ok(());
            }
        }
        claim_record_or_stop(record_limit)?;
        let val: Value = serde_json::from_str(line)
            .map_err(|e| malformed_json_error(&job.path, line_number, e))?;
        let (cells, matched_indices) = tabular_cells_from_value(&val, selectors)?;
        write_tabular_row(writer, fields, &cells, format).with_context(|| {
            format!(
                "writing {} row for {} line {}",
                format.label(),
                job.path.display(),
                line_number
            )
        })?;
        written += 1;
        if let Some(tracker) = whitelist_tracker {
            tracker.observe(crate::streaming::WhitelistEmission {
                matched_fields: &matched_indices,
                used_slow_path: false,
            })?;
        }
        Ok(())
    };

    let partial_read_policy = if allow_partial {
        PartialReadPolicy::AllowPartial
    } else {
        PartialReadPolicy::Strict
    };
    let mut progress_cb = pb.map(|pb| move |delta| pb.inc(delta));
    let mut skip_cb = |path: &Path, err: &anyhow::Error| {
        if let Some(reporter) = partial_reporter {
            reporter.record(path, err);
        }
    };
    let stream_result = for_each_line_with_opts_status(
        &job.path,
        LineStreamOpts {
            read_buf_bytes: Some(read_buf_bytes),
            progress: progress_cb.as_mut().map(|cb| cb as &mut dyn FnMut(u64)),
            on_skip: allow_partial.then_some(&mut skip_cb as &mut dyn FnMut(&Path, &anyhow::Error)),
            partial_read_policy,
            ..Default::default()
        },
        |s| on_line(s),
    );
    let complete = match stream_result {
        Ok(complete) => complete,
        Err(e) if is_record_limit_reached(&e) => true,
        Err(e) => return Err(e),
    };
    Ok(StreamJobResult { written, complete })
}

#[allow(clippy::too_many_arguments)]
fn extract_tabular_common(
    etl: &RedditETL,
    query: &QuerySpec,
    targets: Option<&Vec<String>>,
    out_path: &Path,
    fields: &[String],
    opts: TabularExportOptions,
    format: TabularFormat,
    limit: Option<u64>,
) -> Result<()> {
    let parallelism = etl.opts.parallelism;
    with_thread_pool(parallelism, || {
        let manifest_start = RunManifestStart::now();
        let files = plan_pipeline_files(etl, Some(query))?;
        warn_if_unfiltered_undated_query(etl, query, &files);

        let work_dir = etl.ensure_work_dir()?;
        let tmp_dir = extract_scratch_dir(&work_dir, format.tmp_dir_name(), out_path, false, None)?;
        crate::util::create_dir_all_with_default_backoff(&tmp_dir).with_context(|| {
            format!(
                "creating {} extract work dir {}",
                format.label(),
                tmp_dir.display()
            )
        })?;
        let staging_dir = ensure_staging_dir(&tmp_dir)?;
        sweep_stale_inprogress(&tmp_dir, true)?;

        let selectors = parse_tabular_field_selectors(fields)?;
        let whitelist_tracker = Some(Arc::new(WhitelistMatchTracker::new(
            etl.opts.strict_whitelist,
            fields.iter().cloned(),
        )));
        let record_limit = record_limit_from(limit);
        let total_bytes = total_compressed_size(&files);
        let pb = if etl.opts.progress {
            Some(make_progress_bar_labeled(
                total_bytes,
                etl.opts.progress_label.as_deref(),
            ))
        } else {
            None
        };

        let bounds = bounds_tuple(etl.opts.start, etl.opts.end);
        let read_buf = etl.opts.read_buffer_bytes;
        let write_buf = etl.opts.write_buffer_bytes;
        let output_records = AtomicU64::new(0);

        crate::concurrency::for_each_file_limited(
            &files,
            etl.opts.file_concurrency,
            |job| -> Result<()> {
                if record_limit
                    .as_ref()
                    .is_some_and(|limit| limit.is_exhausted())
                {
                    return Ok(());
                }
                let key = export_part_key(job);
                let tmp_file = tabular_part_path(&tmp_dir, &key, format);
                let lines = match write_jsonl_atomic(&staging_dir, &tmp_file, write_buf, |w| {
                    let result = stream_tabular_job(
                        job,
                        w,
                        targets,
                        query,
                        fields,
                        &selectors,
                        format,
                        pb.clone(),
                        bounds,
                        read_buf,
                        whitelist_tracker.as_deref(),
                        etl.opts.allow_partial,
                        Some(&etl.opts.partial_read_reporter),
                        record_limit.as_deref(),
                    )?;
                    complete_stream_job(job, result)
                }) {
                    Ok(lines) => lines,
                    Err(e) if etl.opts.allow_partial && is_partial_scan_error(&e) => {
                        tracing::warn!(path=%job.path.display(), part=%tmp_file.display(), error=%e, "Skipping tabular export month after zstd decode error; staged part was discarded");
                        return Ok(());
                    }
                    Err(e) => return Err(e),
                };
                output_records.fetch_add(lines, Ordering::Relaxed);
                if lines == 0 {
                    let _ = crate::util::remove_with_short_backoff(&tmp_file);
                }
                Ok(())
            },
        )?;

        if let Some(tracker) = &whitelist_tracker {
            tracker.finalize()?;
        }
        if let Some(pb) = pb {
            pb.finish_with_message("done");
        }
        stitch_tabular_parts(&tmp_dir, out_path, fields, opts, format, write_buf)?;
        let manifest = scan_manifest_input(
            manifest_start,
            &format!("scan.extract_to_{}", format.label()),
            format.label(),
            etl,
            query,
            &files,
            limit,
            manifest_counts(&[("records_written", output_records.load(Ordering::Relaxed))]),
            None,
            None,
            serde_json::json!({
                "tabular_fields": fields,
                "header": opts.header,
            }),
        );
        maybe_write_run_manifest(
            etl.opts.emit_manifest,
            manifest,
            ManifestDestination::File(out_path.to_path_buf()),
        )?;
        if let Err(e) = crate::util::remove_dir_all_with_short_backoff(&tmp_dir) {
            tracing::warn!(path=%tmp_dir.display(), error=%e, "failed to remove tabular extract scratch dir");
        }
        Ok(())
    })
}

impl ScanPlan {
    /// Export matching records as CSV. `fields` is the fixed top-level schema;
    /// missing fields render as empty cells.
    pub fn extract_to_csv<I, S>(
        self,
        out_path: &Path,
        fields: I,
        opts: TabularExportOptions,
    ) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.extract_to_tabular(out_path, fields, opts, TabularFormat::Csv)
    }

    /// Export matching records as TSV. `fields` is the fixed top-level schema;
    /// missing fields render as empty cells. Values containing a literal tab
    /// are rejected because TSV has no standard escaping convention.
    pub fn extract_to_tsv<I, S>(
        self,
        out_path: &Path,
        fields: I,
        opts: TabularExportOptions,
    ) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.extract_to_tabular(out_path, fields, opts, TabularFormat::Tsv)
    }

    fn extract_to_tabular<I, S>(
        self,
        out_path: &Path,
        fields: I,
        opts: TabularExportOptions,
        format: TabularFormat,
    ) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        if self.etl.opts.human_readable_timestamps {
            anyhow::bail!(
                "--human-timestamps is not supported for CSV/TSV export; use --format jsonl/json/spool/zst/partitioned-jsonl or omit the flag"
            );
        }
        if self.etl.opts.resume {
            anyhow::bail!(
                "--resume is not supported for CSV/TSV export; use --format jsonl/json/spool/zst/partitioned-jsonl or omit the flag"
            );
        }
        let fields = normalize_tabular_fields(fields)?;
        let mut scan = self;
        scan.etl.opts.whitelist_fields = Some(fields.clone());
        let plan = scan.build()?;
        log_pseudo_user_filter(&plan.query);
        let targets = resolve_target_subs_from(&plan.etl.opts.subreddit, &plan.query.subreddits);
        extract_tabular_common(
            &plan.etl,
            &plan.query,
            targets.as_ref(),
            out_path,
            &fields,
            opts,
            format,
            plan.limit,
        )
    }
}

/// Convert existing plain JSONL files (including RETL spool/parent-enriched
/// parts) into a single CSV file using top-level, dotted, or JSON Pointer
/// field selectors. Missing fields render as empty cells.
pub fn convert_jsonl_to_csv<I, P, J, S>(
    inputs: I,
    out_path: &Path,
    fields: J,
    opts: TabularExportOptions,
) -> Result<u64>
where
    I: IntoIterator<Item = P>,
    P: AsRef<Path>,
    J: IntoIterator<Item = S>,
    S: Into<String>,
{
    convert_jsonl_to_tabular(inputs, out_path, fields, opts, TabularFormat::Csv)
}

/// Convert existing plain JSONL files (including RETL spool/parent-enriched
/// parts) into a single TSV file using top-level, dotted, or JSON Pointer
/// field selectors. Values containing tabs or line breaks are rejected; use
/// CSV for arbitrary Reddit text fields.
pub fn convert_jsonl_to_tsv<I, P, J, S>(
    inputs: I,
    out_path: &Path,
    fields: J,
    opts: TabularExportOptions,
) -> Result<u64>
where
    I: IntoIterator<Item = P>,
    P: AsRef<Path>,
    J: IntoIterator<Item = S>,
    S: Into<String>,
{
    convert_jsonl_to_tabular(inputs, out_path, fields, opts, TabularFormat::Tsv)
}

fn convert_jsonl_to_tabular<I, P, J, S>(
    inputs: I,
    out_path: &Path,
    fields: J,
    opts: TabularExportOptions,
    format: TabularFormat,
) -> Result<u64>
where
    I: IntoIterator<Item = P>,
    P: AsRef<Path>,
    J: IntoIterator<Item = S>,
    S: Into<String>,
{
    let input_paths: Vec<PathBuf> = inputs
        .into_iter()
        .map(|path| path.as_ref().to_path_buf())
        .collect();
    if input_paths.is_empty() {
        anyhow::bail!("convert requires at least one JSONL input file");
    }
    let fields = normalize_tabular_fields(fields)?;
    let selectors = parse_tabular_field_selectors(&fields)?;
    write_at_path_atomic(out_path, 64 * 1024, |out| {
        let mut written = 0_u64;
        if opts.header {
            write_tabular_header(out, &fields, format)?;
        }
        for path in &input_paths {
            let file = crate::util::open_with_default_backoff(path)
                .with_context(|| format!("opening JSONL input {}", path.display()))?;
            let mut reader = BufReader::with_capacity(256 * 1024, file);
            let mut line = String::with_capacity(16 * 1024);
            let mut line_number = 0_u64;
            loop {
                let n = crate::ndjson::read_line_capped(
                    &mut reader,
                    &mut line,
                    crate::ndjson::DEFAULT_MAX_LINE_BYTES,
                    path,
                )
                .with_context(|| {
                    format!(
                        "reading JSONL input {} near line {}",
                        path.display(),
                        line_number + 1
                    )
                })?;
                if n == 0 {
                    break;
                }
                line_number += 1;
                if line.trim().is_empty() {
                    continue;
                }
                let value: Value = serde_json::from_str(&line).with_context(|| {
                    format!("malformed JSON in {} line {}", path.display(), line_number)
                })?;
                let (cells, _matched_indices) = tabular_cells_from_value(&value, &selectors)?;
                write_tabular_row(out, &fields, &cells, format).with_context(|| {
                    format!(
                        "writing {} row for {} line {}",
                        format.label(),
                        path.display(),
                        line_number
                    )
                })?;
                written += 1;
            }
        }
        Ok(written)
    })
}
