
const PARTITIONED_COMMENTS_DIR: &str = "comments";
const PARTITIONED_SUBMISSIONS_DIR: &str = "submissions";
const PARTITIONED_COMMENT_KEY_PREFIX: &str = "RC";
const PARTITIONED_SUBMISSION_KEY_PREFIX: &str = "RS";

fn export_part_key(job: &FileJob) -> String {
    let prefix = match job.kind {
        FileKind::Comment => PARTITIONED_COMMENT_KEY_PREFIX,
        FileKind::Submission => PARTITIONED_SUBMISSION_KEY_PREFIX,
    };
    crate::progress_manifest::month_key(prefix, job.ym)
}

fn partitioned_resume_operation(format: ExportFormat) -> &'static str {
    match format {
        ExportFormat::Jsonl => "partitioned-jsonl",
        ExportFormat::Zst => "partitioned-zst",
    }
}

fn partitioned_ext(format: ExportFormat) -> &'static str {
    match format {
        ExportFormat::Jsonl => "jsonl",
        ExportFormat::Zst => "zst",
    }
}

fn partitioned_subdir(kind: FileKind) -> &'static str {
    match kind {
        FileKind::Comment => PARTITIONED_COMMENTS_DIR,
        FileKind::Submission => PARTITIONED_SUBMISSIONS_DIR,
    }
}

fn partitioned_output_path(out_base_dir: &Path, job: &FileJob, format: ExportFormat) -> PathBuf {
    let prefix = match job.kind {
        FileKind::Comment => PARTITIONED_COMMENT_KEY_PREFIX,
        FileKind::Submission => PARTITIONED_SUBMISSION_KEY_PREFIX,
    };
    out_base_dir
        .join(partitioned_subdir(job.kind))
        .join(format!("{}_{}.{}", prefix, job.ym, partitioned_ext(format)))
}

fn partitioned_output_path_for_key(
    out_base_dir: &Path,
    key: &str,
    format: ExportFormat,
) -> Option<PathBuf> {
    let dir = if key.starts_with(&format!("{}_", PARTITIONED_COMMENT_KEY_PREFIX)) {
        PARTITIONED_COMMENTS_DIR
    } else if key.starts_with(&format!("{}_", PARTITIONED_SUBMISSION_KEY_PREFIX)) {
        PARTITIONED_SUBMISSIONS_DIR
    } else {
        return None;
    };
    Some(
        out_base_dir
            .join(dir)
            .join(format!("{}.{}", key, partitioned_ext(format))),
    )
}

fn partitioned_key_from_output_name(
    name: &str,
    key_prefix: &str,
    format: ExportFormat,
) -> Option<String> {
    let prefix = format!("{key_prefix}_");
    let suffix = format!(".{}", partitioned_ext(format));
    let ym = name.strip_prefix(&prefix)?.strip_suffix(&suffix)?;
    ym.parse::<YearMonth>().ok()?;
    Some(format!("{key_prefix}_{ym}"))
}

fn clear_partitioned_resume_outputs(out_base_dir: &Path, format: ExportFormat) -> Result<()> {
    remove_matching_files(&out_base_dir.join(PARTITIONED_COMMENTS_DIR), |name| {
        partitioned_key_from_output_name(name, PARTITIONED_COMMENT_KEY_PREFIX, format).is_some()
    })?;
    remove_matching_files(&out_base_dir.join(PARTITIONED_SUBMISSIONS_DIR), |name| {
        partitioned_key_from_output_name(name, PARTITIONED_SUBMISSION_KEY_PREFIX, format).is_some()
    })?;
    Ok(())
}

fn prune_partitioned_outputs_except(
    out_base_dir: &Path,
    format: ExportFormat,
    keep_keys: &HashSet<String>,
) -> Result<()> {
    remove_matching_files(&out_base_dir.join(PARTITIONED_COMMENTS_DIR), |name| {
        match partitioned_key_from_output_name(name, PARTITIONED_COMMENT_KEY_PREFIX, format) {
            Some(key) => !keep_keys.contains(&key),
            None => false,
        }
    })?;
    remove_matching_files(&out_base_dir.join(PARTITIONED_SUBMISSIONS_DIR), |name| {
        match partitioned_key_from_output_name(name, PARTITIONED_SUBMISSION_KEY_PREFIX, format) {
            Some(key) => !keep_keys.contains(&key),
            None => false,
        }
    })?;
    Ok(())
}

fn validate_partitioned_resume_output(
    path: &Path,
    format: ExportFormat,
    expected: &MonthEntry,
) -> Result<()> {
    if expected.lines == 0 && !path.exists() {
        return Ok(());
    }
    let meta = fs::metadata(path)
        .with_context(|| format!("stat partitioned output {}", path.display()))?;
    if meta.len() != expected.size {
        anyhow::bail!(
            "partitioned output size mismatch for {}: expected {}, got {}",
            path.display(),
            expected.size,
            meta.len()
        );
    }
    match format {
        ExportFormat::Jsonl => {
            let actual = validate_jsonl_part(path)?;
            if actual.lines != expected.lines {
                anyhow::bail!(
                    "partitioned JSONL line-count mismatch for {}: expected {}, got {}",
                    path.display(),
                    expected.lines,
                    actual.lines
                );
            }
        }
        ExportFormat::Zst => {
            crate::zstd_jsonl::validate_zst_full(path)
                .with_context(|| format!("validating partitioned zst {}", path.display()))?;
        }
    }
    Ok(())
}

fn validate_partitioned_entry(
    out_base_dir: &Path,
    format: ExportFormat,
    key: &str,
    entry: MonthEntry,
) -> Result<MonthEntry> {
    let Some(path) = partitioned_output_path_for_key(out_base_dir, key, format) else {
        anyhow::bail!("unrecognized partitioned resume key {key}");
    };
    validate_partitioned_resume_output(&path, format, &entry)?;
    Ok(entry)
}

/// Parameters threaded through [`process_partitioned_job`] — bundled so the
/// per-file closure stays a one-liner.
struct PartitionedJobCtx<'a> {
    out_base_dir: &'a Path,
    staging_dir: &'a Path,
    format: ExportFormat,
    targets: Option<&'a Vec<String>>,
    query: &'a QuerySpec,
    whitelist: &'a Option<Vec<String>>,
    pb: Option<&'a ProgressBar>,
    bounds: Option<DateBounds>,
    read_buf: usize,
    write_buf: usize,
    human_ts: bool,
    zst_level: i32,
    whitelist_tracker: Option<&'a WhitelistMatchTracker>,
    record_limit: Option<&'a RecordLimit>,
    allow_partial: bool,
    partial_reporter: Option<&'a crate::config::PartialReadReporter>,
    resume: bool,
    completed_keys: &'a HashSet<String>,
    accumulator: Option<&'a ManifestAccumulator>,
    output_records: &'a AtomicU64,
    output_files: &'a AtomicU64,
}

/// Per-file body for partitioned exports. Returns `Ok(())` on resume-skip,
/// successful publish (including the zero-records-discard case), or a tolerated
/// zstd partial-scan skip (already logged). Single `stream_job_with_partial_policy`
/// call site dispatched by `format` — the writer wrapper (`write_jsonl_atomic`
/// vs `write_zst_atomic`) is the only thing that differs between branches.
fn process_partitioned_job(job: &FileJob, ctx: &PartitionedJobCtx<'_>) -> Result<()> {
    let key = export_part_key(job);
    let out_path = partitioned_output_path(ctx.out_base_dir, job, ctx.format);

    if ctx.record_limit.is_some_and(|limit| limit.is_exhausted()) {
        return Ok(());
    }

    if ctx.resume && ctx.completed_keys.contains(&key) {
        if let Some(pb) = ctx.pb {
            let sz = fs::metadata(&job.path).map(|m| m.len()).unwrap_or(0);
            pb.inc(sz);
        }
        return Ok(());
    }

    let stream = |w: &mut dyn Write| -> Result<u64> {
        let result = stream_job_with_partial_policy(
            job,
            w,
            ctx.targets,
            ctx.query,
            ctx.whitelist,
            ctx.pb.cloned(),
            ctx.bounds,
            ctx.read_buf,
            ctx.human_ts,
            ctx.whitelist_tracker,
            ctx.allow_partial,
            ctx.partial_reporter,
            ctx.record_limit,
        )?;
        complete_stream_job(job, result)
    };

    let written_result = match ctx.format {
        ExportFormat::Jsonl => {
            write_jsonl_atomic(ctx.staging_dir, &out_path, ctx.write_buf, stream)
        }
        ExportFormat::Zst => write_zst_atomic(
            ctx.staging_dir,
            &out_path,
            ctx.zst_level,
            ctx.write_buf,
            stream,
        ),
    };

    let written = match written_result {
        Ok(n) => n,
        Err(e) if ctx.allow_partial && is_partial_scan_error(&e) => {
            tracing::warn!(path=%job.path.display(), error=%e, "Skipping partitioned export month after zstd decode error; staged output was discarded");
            return Ok(());
        }
        Err(e) => return Err(e),
    };

    ctx.output_records.fetch_add(written, Ordering::Relaxed);
    if written == 0 {
        let _ = crate::util::remove_with_short_backoff(&out_path);
    } else {
        ctx.output_files.fetch_add(1, Ordering::Relaxed);
    }
    if let Some(acc) = ctx.accumulator {
        let size = if written == 0 {
            0
        } else {
            fs::metadata(&out_path).map(|m| m.len()).unwrap_or(0)
        };
        acc.commit(
            key,
            MonthEntry {
                size,
                lines: written,
                sha256: None,
            },
        )
        .context(
            "failed to durably update partitioned export progress manifest after publishing partition",
        )?;
    }
    Ok(())
}

/// Build + persist the run-manifest tail for `export_partitioned`.
fn emit_partitioned_resume_manifest(
    start: RunManifestStart,
    plan: &PreparedScan<'_>,
    files: &[FileJob],
    format: ExportFormat,
    out_base_dir: &Path,
    resume: bool,
    resume_fingerprint: &str,
    output_records: u64,
    output_files: u64,
) -> Result<()> {
    let manifest = scan_manifest_input(
        start,
        "scan.export_partitioned",
        partitioned_resume_operation(format),
        plan.etl,
        plan.query,
        files,
        plan.limit,
        manifest_counts(&[
            ("records_written", output_records),
            ("partition_files", output_files),
        ]),
        Some(resume_fingerprint.to_string()),
        resume.then(|| crate::progress_manifest::manifest_path(out_base_dir)),
        serde_json::json!({
            "partition_format": partitioned_ext(format),
            "zst_level": (format == ExportFormat::Zst).then_some(plan.etl.opts.zst_level),
        }),
    );
    maybe_write_run_manifest(
        plan.etl.opts.emit_manifest,
        manifest,
        ManifestDestination::Directory(out_base_dir.to_path_buf()),
    )?;
    Ok(())
}

/// Borrowed view of a fully-built `ScanPlan` used by partitioned-export helpers.
struct PreparedScan<'a> {
    etl: &'a RedditETL,
    query: &'a QuerySpec,
    limit: Option<u64>,
}

impl ScanPlan {
    /// Export corpus back to partitioned JSONL or ZST by month/kinds with query filters.
    /// This lives on ScanPlan (advanced query mode).
    ///
    /// Each output is staged as a unique
    /// `<out_base_dir>/_staging/<file>.*.inprogress`, finalized (zstd frame
    /// closed with checksum, or buffer flushed) and atomically renamed onto its
    /// final path. Stale `*.inprogress` from a crashed prior run are swept on
    /// entry only when their owner PID is no longer live.
    pub fn export_partitioned(self, out_base_dir: &Path, format: ExportFormat) -> Result<()> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        validate_export_whitelist(&plan.etl)?;
        let targets = resolve_target_subs_from(&plan.etl.opts.subreddit, &plan.query.subreddits);
        let parallelism = plan.etl.opts.parallelism;
        let prepared = PreparedScan {
            etl: &plan.etl,
            query: &plan.query,
            limit: plan.limit,
        };

        with_thread_pool(parallelism, || {
            let manifest_start = RunManifestStart::now();
            let files = plan_pipeline_files(prepared.etl, Some(prepared.query))?;
            warn_if_unfiltered_undated_query(prepared.etl, prepared.query, &files);

            let comments_dir = out_base_dir.join(PARTITIONED_COMMENTS_DIR);
            let submissions_dir = out_base_dir.join(PARTITIONED_SUBMISSIONS_DIR);
            crate::util::create_dir_all_with_default_backoff(&comments_dir).with_context(|| {
                format!("creating comments export dir {}", comments_dir.display())
            })?;
            crate::util::create_dir_all_with_default_backoff(&submissions_dir).with_context(|| {
                format!(
                    "creating submissions export dir {}",
                    submissions_dir.display()
                )
            })?;
            let staging_dir = ensure_staging_dir(out_base_dir)?;
            sweep_stale_inprogress(out_base_dir, true)?;

            let planned_keys = planned_job_keys(&files);
            let resume = prepared.etl.opts.resume;
            let resume_fingerprint = build_resume_fingerprint(
                prepared.etl,
                prepared.query,
                partitioned_resume_operation(format),
                prepared.limit,
                &files,
            )?;

            let ResumePrelude {
                initial_months,
                completed_keys,
                accumulator,
            } = prepare_resume_run(
                out_base_dir,
                &resume_fingerprint,
                Some(&planned_keys),
                resume,
                false,
                ResumeLogLabels {
                    fingerprint_mismatch:
                        "partitioned resume manifest fingerprint does not match current query/config; discarding partitioned outputs",
                    out_of_plan: "dropping partitioned progress entry outside current plan",
                    stale_entry: "dropping stale partitioned progress entry; month will be re-run",
                },
                || clear_partitioned_resume_outputs(out_base_dir, format),
                |keep_keys| prune_partitioned_outputs_except(out_base_dir, format, keep_keys),
                |key, entry| validate_partitioned_entry(out_base_dir, format, key, entry),
            )?;

            let resumed_lines = committed_line_count(&initial_months);
            let output_records = AtomicU64::new(resumed_lines);
            let output_files = AtomicU64::new(completed_keys.len() as u64);

            let whitelist = prepared.etl.opts.whitelist_fields.clone();
            let whitelist_tracker = whitelist.as_ref().map(|fields| {
                Arc::new(WhitelistMatchTracker::new(
                    prepared.etl.opts.strict_whitelist,
                    fields.iter().cloned(),
                ))
            });
            let record_limit = record_limit_from_with_claimed(prepared.limit, resumed_lines);
            let total_bytes = total_compressed_size(&files);
            let pb = if prepared.etl.opts.progress {
                Some(make_progress_bar_labeled(
                    total_bytes,
                    prepared.etl.opts.progress_label.as_deref(),
                ))
            } else {
                None
            };

            let ctx = PartitionedJobCtx {
                out_base_dir,
                staging_dir: &staging_dir,
                format,
                targets: targets.as_ref(),
                query: prepared.query,
                whitelist: &whitelist,
                pb: pb.as_ref(),
                bounds: bounds_tuple(prepared.etl.opts.start, prepared.etl.opts.end),
                read_buf: prepared.etl.opts.read_buffer_bytes,
                write_buf: prepared.etl.opts.write_buffer_bytes,
                human_ts: prepared.etl.opts.human_readable_timestamps,
                zst_level: prepared.etl.opts.zst_level,
                whitelist_tracker: whitelist_tracker.as_deref(),
                record_limit: record_limit.as_deref(),
                allow_partial: prepared.etl.opts.allow_partial,
                partial_reporter: Some(&prepared.etl.opts.partial_read_reporter),
                resume,
                completed_keys: &completed_keys,
                accumulator: accumulator.as_ref(),
                output_records: &output_records,
                output_files: &output_files,
            };

            crate::concurrency::for_each_file_limited(
                &files,
                prepared.etl.opts.file_concurrency,
                |job| process_partitioned_job(job, &ctx),
            )?;

            if let Some(tracker) = &whitelist_tracker {
                tracker.finalize()?;
            }
            if let Some(pb) = pb {
                pb.finish_with_message("done");
            }
            ensure_resume_manifest_durable(accumulator.as_ref(), "partitioned export")?;
            emit_partitioned_resume_manifest(
                manifest_start,
                &prepared,
                &files,
                format,
                out_base_dir,
                resume,
                &resume_fingerprint,
                output_records.load(Ordering::Relaxed),
                output_files.load(Ordering::Relaxed),
            )
        })
    }
}
