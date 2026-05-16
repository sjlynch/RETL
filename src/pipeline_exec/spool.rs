
fn remove_matching_files(dir: &Path, mut should_remove: impl FnMut(&str) -> bool) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    let entries = crate::util::read_dir_with_default_backoff(dir)
        .with_context(|| format!("read_dir {}", dir.display()))?;
    for entry in entries {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if should_remove(name) {
            crate::util::remove_with_short_backoff(&path)
                .with_context(|| format!("remove stale RETL-owned output {}", path.display()))?;
        }
    }
    Ok(())
}

fn planned_job_keys(files: &[FileJob]) -> HashSet<String> {
    files.iter().map(export_part_key).collect()
}

fn spool_key_from_part_name(name: &str) -> Option<String> {
    let (prefix, key_prefix) = if name.starts_with("part_RC_") {
        ("part_RC_", "RC")
    } else if name.starts_with("part_RS_") {
        ("part_RS_", "RS")
    } else {
        return None;
    };
    let ym = name.strip_prefix(prefix)?.strip_suffix(".jsonl")?;
    ym.parse::<YearMonth>().ok()?;
    Some(format!("{key_prefix}_{ym}"))
}

fn clear_spool_resume_parts(out_dir: &Path) -> Result<()> {
    remove_matching_files(out_dir, |name| spool_key_from_part_name(name).is_some())
}

fn prune_spool_outputs_except(out_dir: &Path, keep_keys: &HashSet<String>) -> Result<()> {
    remove_matching_files(out_dir, |name| match spool_key_from_part_name(name) {
        Some(key) => !keep_keys.contains(&key),
        None => false,
    })
}

fn clear_extract_resume_parts(tmp_dir: &Path) -> Result<()> {
    remove_matching_files(tmp_dir, |name| {
        name.starts_with(".part_") && name.ends_with(".jsonl")
    })
}

struct MonthJobCtx<'a> {
    out_dir: &'a Path,
    staging_dir: &'a Path,
    targets: Option<&'a Vec<String>>,
    query: &'a QuerySpec,
    whitelist: &'a Option<Vec<String>>,
    pb: Option<&'a ProgressBar>,
    bounds: Option<DateBounds>,
    read_buf: usize,
    write_buf: usize,
    human_ts: bool,
    whitelist_tracker: Option<&'a WhitelistMatchTracker>,
    record_limit: Option<&'a RecordLimit>,
    resume: bool,
    completed_keys: &'a HashSet<String>,
    allow_partial: bool,
    partial_reporter: Option<&'a crate::config::PartialReadReporter>,
}

/// Load `_progress.json`, validate it against the current fingerprint and each
/// entry against the on-disk file size, drop mismatches with a warning, and
/// return the surviving entries plus a snapshot of completed keys.
fn load_and_validate_manifest(
    out_dir: &Path,
    fingerprint: &str,
    planned_keys: &HashSet<String>,
) -> Result<(HashMap<String, MonthEntry>, HashSet<String>)> {
    let manifest = crate::progress_manifest::load(out_dir);
    if manifest.fingerprint.as_deref() != Some(fingerprint) && !manifest.months.is_empty() {
        tracing::warn!(
            path=%crate::progress_manifest::manifest_path(out_dir).display(),
            stored=?manifest.fingerprint,
            current=%fingerprint,
            "resume manifest fingerprint does not match current query/config; discarding spool parts"
        );
        clear_spool_resume_parts(out_dir)?;
        return Ok((HashMap::new(), HashSet::new()));
    }
    let mut keep: HashMap<String, MonthEntry> = HashMap::new();
    for (k, v) in manifest.months {
        if !planned_keys.contains(&k) {
            tracing::info!(key=%k, "dropping progress manifest entry outside current spool plan");
            continue;
        }
        let final_name = format!("part_{}.jsonl", k);
        let final_path = out_dir.join(&final_name);
        match fs::metadata(&final_path) {
            Ok(meta) if meta.len() == v.size => {
                keep.insert(k, v);
            }
            _ => {
                tracing::info!(key=%k, "dropping stale progress manifest entry; month will be re-run");
            }
        }
    }
    let completed_keys: HashSet<String> = keep.keys().cloned().collect();
    Ok((keep, completed_keys))
}

/// Per-month closure body: skip if the month is already published (resume
/// fast-path), otherwise atomically write the spool output via
/// `write_jsonl_atomic`. Returns `Ok(None)` on resume-skip or a tolerated zstd
/// decode/partial-scan skip (already logged); `Ok(Some(MonthResult))` on a
/// successful publish whose entry the caller should commit to the manifest.
/// Non-decode output, malformed-JSON, and publish failures are fatal.
fn process_month(job: &FileJob, ctx: &MonthJobCtx<'_>) -> Result<Option<MonthResult>> {
    let (file_prefix, key_prefix) = match job.kind {
        FileKind::Comment => (FILE_PREFIX_RC, "RC"),
        FileKind::Submission => (FILE_PREFIX_RS, "RS"),
    };
    let out_path = ctx
        .out_dir
        .join(format!("{}_{}.jsonl", file_prefix, job.ym));
    let key = crate::progress_manifest::month_key(key_prefix, job.ym);

    if ctx.record_limit.is_some_and(|limit| limit.is_exhausted()) {
        return Ok(None);
    }

    // Resume fast-path: skip the month entirely if it's already in the
    // manifest (and the on-disk file matched at load time). Still bump the
    // progress bar by the input file's compressed size so the user sees the
    // work accounted for.
    if ctx.resume && ctx.completed_keys.contains(&key) {
        if let Some(pb) = ctx.pb {
            let sz = fs::metadata(&job.path).map(|m| m.len()).unwrap_or(0);
            pb.inc(sz);
        }
        return Ok(None);
    }

    let n = match write_jsonl_atomic(ctx.staging_dir, &out_path, ctx.write_buf, |w| {
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
    }) {
        Ok(n) => n,
        Err(e) if ctx.allow_partial && is_partial_scan_error(&e) => {
            tracing::warn!(path=%job.path.display(), output=%out_path.display(), error=%e, "Skipping month after zstd decode error; staged spool output was discarded and resume will retry it");
            return Ok(None);
        }
        Err(e) => return Err(e),
    };

    Ok(Some(MonthResult {
        out_path,
        key,
        lines: n,
    }))
}

/// Atomically commit a month's manifest entry after a successful publish.
/// `write_jsonl_atomic` only returns Ok once the rename landed, so the size
/// stat is taken from the published path. Resume-enabled callers fail the run
/// immediately if this commit fails: the data file may remain published, but
/// returning success would incorrectly claim a durable checkpoint.
fn commit_entry_to_manifest(acc: &ManifestAccumulator, result: MonthResult) -> Result<()> {
    let size = fs::metadata(&result.out_path).map(|m| m.len()).unwrap_or(0);
    let entry = MonthEntry {
        size,
        lines: result.lines,
        sha256: None,
    };
    acc.commit(result.key, entry)
}

fn ensure_resume_manifest_durable(
    accumulator: Option<&ManifestAccumulator>,
    operation: &str,
) -> Result<()> {
    if let Some(error) = accumulator.and_then(ManifestAccumulator::last_save_error) {
        anyhow::bail!(
            "{operation} resume progress manifest is not durable; earlier save failed: {error}"
        );
    }
    Ok(())
}

impl ScanPlan {
    /// Spool per-month outputs for later parent attachment + aggregation.
    /// Writes **separate** files per source to avoid clobbering:
    ///   - comments → `part_RC_YYYY-MM.jsonl`
    ///   - submissions → `part_RS_YYYY-MM.jsonl`
    ///
    /// Each month is staged as a unique `<out_dir>/_staging/<file>.*.inprogress`
    /// then atomically renamed onto the final path so a crashed run never
    /// publishes a partial file. On entry, leftover `*.inprogress` files from
    /// a prior crashed run are swept only when their owner PID is no longer live.
    ///
    /// Returns `(vector_of_paths, total_records_written)`.
    pub fn extract_spool_monthly(self, out_dir: &Path) -> Result<(Vec<PathBuf>, u64)> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        validate_export_whitelist(&plan.etl)?;
        let parallelism = plan.etl.opts.parallelism;

        with_thread_pool(parallelism, || {
            let manifest_start = RunManifestStart::now();
            crate::util::create_dir_all_with_default_backoff(out_dir)
                .with_context(|| format!("creating spool dir {}", out_dir.display()))?;
            let staging_dir = ensure_staging_dir(out_dir)?;
            sweep_stale_inprogress(out_dir, true)?;

            let targets =
                resolve_target_subs_from(&plan.etl.opts.subreddit, &plan.query.subreddits);
            let files = plan_pipeline_files(&plan.etl, Some(&plan.query))?;
            warn_if_unfiltered_undated_query(&plan.etl, &plan.query, &files);
            let planned_keys = planned_job_keys(&files);

            let resume = plan.etl.opts.resume;
            let resume_fingerprint =
                build_resume_fingerprint(&plan.etl, &plan.query, "spool", plan.limit, &files)?;
            // Resume manifest: load on entry, validate each entry against the
            // current query/config fingerprint and the file actually on disk,
            // then snapshot surviving keys before any worker mutates the accumulator.
            let (initial_months, completed_keys) = if resume {
                load_and_validate_manifest(out_dir, &resume_fingerprint, &planned_keys)?
            } else {
                clear_spool_resume_parts(out_dir)?;
                (HashMap::new(), HashSet::new())
            };
            if resume {
                prune_spool_outputs_except(out_dir, &completed_keys)?;
            }

            let total_bytes = total_compressed_size(&files);
            let pb = if plan.etl.opts.progress {
                Some(make_progress_bar_labeled(
                    total_bytes,
                    plan.etl.opts.progress_label.as_deref(),
                ))
            } else {
                None
            };

            let total_written = AtomicU64::new(0);
            let parts = Mutex::new(Vec::<PathBuf>::new());

            // Pre-seed `parts` with already-completed months so the returned
            // list reflects the full set of published outputs (resumed + new).
            if resume {
                let mut guard = parts.lock().unwrap();
                for key in &completed_keys {
                    guard.push(out_dir.join(format!("part_{}.jsonl", key)));
                }
            }

            let resumed_lines = committed_line_count(&initial_months);
            let total_output_lines = AtomicU64::new(resumed_lines);

            // Persist the (pruned) manifest before any work, so a crash mid-
            // prune cannot resurrect entries we already know are stale.
            let accumulator = if resume {
                crate::progress_manifest::save(
                    out_dir,
                    &initial_months,
                    Some(&resume_fingerprint),
                )?;
                Some(ManifestAccumulator::new(
                    out_dir,
                    initial_months,
                    Some(resume_fingerprint.clone()),
                ))
            } else {
                None
            };

            let whitelist = plan.etl.opts.whitelist_fields.clone();
            let whitelist_tracker = whitelist.as_ref().map(|fields| {
                Arc::new(WhitelistMatchTracker::new(
                    plan.etl.opts.strict_whitelist,
                    fields.iter().cloned(),
                ))
            });
            let record_limit = record_limit_from_with_claimed(plan.limit, resumed_lines);
            let targets_ref = targets.as_ref();
            let bounds = bounds_tuple(plan.etl.opts.start, plan.etl.opts.end);
            let read_buf = plan.etl.opts.read_buffer_bytes;
            let write_buf = plan.etl.opts.write_buffer_bytes;
            let human_ts = plan.etl.opts.human_readable_timestamps;

            crate::concurrency::for_each_file_limited(
                &files,
                plan.etl.opts.file_concurrency,
                |job| -> Result<()> {
                    let ctx = MonthJobCtx {
                        out_dir,
                        staging_dir: &staging_dir,
                        targets: targets_ref,
                        query: &plan.query,
                        whitelist: &whitelist,
                        pb: pb.as_ref(),
                        bounds,
                        read_buf,
                        write_buf,
                        human_ts,
                        whitelist_tracker: whitelist_tracker.as_deref(),
                        record_limit: record_limit.as_deref(),
                        resume,
                        completed_keys: &completed_keys,
                        allow_partial: plan.etl.opts.allow_partial,
                        partial_reporter: Some(&plan.etl.opts.partial_read_reporter),
                    };
                    let outcome = process_month(job, &ctx)?;

                    if let Some(month) = outcome {
                        total_written.fetch_add(month.lines, Ordering::Relaxed);
                        total_output_lines.fetch_add(month.lines, Ordering::Relaxed);
                        parts.lock().unwrap().push(month.out_path.clone());
                        if let Some(acc) = &accumulator {
                            commit_entry_to_manifest(acc, month).context(
                                "failed to durably update resume progress manifest after publishing spool output",
                            )?;
                        }
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
            ensure_resume_manifest_durable(accumulator.as_ref(), "spool")?;

            let mut list = parts.into_inner().unwrap();
            list.sort();
            list.dedup();
            let manifest = scan_manifest_input(
                manifest_start,
                "scan.extract_spool_monthly",
                "spool-jsonl-directory",
                &plan.etl,
                &plan.query,
                &files,
                plan.limit,
                manifest_counts(&[
                    (
                        "records_written",
                        total_output_lines.load(Ordering::Relaxed),
                    ),
                    ("part_files", list.len() as u64),
                    (
                        "records_written_this_run",
                        total_written.load(Ordering::Relaxed),
                    ),
                ]),
                Some(resume_fingerprint.clone()),
                resume.then(|| crate::progress_manifest::manifest_path(out_dir)),
                serde_json::json!({}),
            );
            maybe_write_run_manifest(
                plan.etl.opts.emit_manifest,
                manifest,
                ManifestDestination::Directory(out_dir.to_path_buf()),
            )?;
            Ok((list, total_written.load(Ordering::Relaxed)))
        })
    }
}
