
fn export_part_key(job: &FileJob) -> String {
    let prefix = match job.kind {
        FileKind::Comment => "RC",
        FileKind::Submission => "RS",
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

fn partitioned_output_path(out_base_dir: &Path, job: &FileJob, format: ExportFormat) -> PathBuf {
    let (dir, prefix) = match job.kind {
        FileKind::Comment => ("comments", "RC"),
        FileKind::Submission => ("submissions", "RS"),
    };
    out_base_dir
        .join(dir)
        .join(format!("{}_{}.{}", prefix, job.ym, partitioned_ext(format)))
}

fn partitioned_output_path_for_key(
    out_base_dir: &Path,
    key: &str,
    format: ExportFormat,
) -> Option<PathBuf> {
    let dir = if key.starts_with("RC_") {
        "comments"
    } else if key.starts_with("RS_") {
        "submissions"
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
    remove_matching_files(&out_base_dir.join("comments"), |name| {
        partitioned_key_from_output_name(name, "RC", format).is_some()
    })?;
    remove_matching_files(&out_base_dir.join("submissions"), |name| {
        partitioned_key_from_output_name(name, "RS", format).is_some()
    })?;
    Ok(())
}

fn prune_partitioned_outputs_except(
    out_base_dir: &Path,
    format: ExportFormat,
    keep_keys: &HashSet<String>,
) -> Result<()> {
    remove_matching_files(&out_base_dir.join("comments"), |name| {
        match partitioned_key_from_output_name(name, "RC", format) {
            Some(key) => !keep_keys.contains(&key),
            None => false,
        }
    })?;
    remove_matching_files(&out_base_dir.join("submissions"), |name| {
        match partitioned_key_from_output_name(name, "RS", format) {
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

        with_thread_pool(parallelism, || {
            let manifest_start = RunManifestStart::now();
            let files = plan_pipeline_files(&plan.etl, Some(&plan.query))?;
            warn_if_unfiltered_undated_query(&plan.etl, &plan.query, &files);

            let comments_dir = out_base_dir.join("comments");
            let submissions_dir = out_base_dir.join("submissions");
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
            let resume = plan.etl.opts.resume;
            let resume_fingerprint = build_resume_fingerprint(
                &plan.etl,
                &plan.query,
                partitioned_resume_operation(format),
                plan.limit,
                &files,
            )?;
            let (initial_months, completed_keys) = if resume {
                load_and_validate_partitioned_manifest(
                    out_base_dir,
                    &resume_fingerprint,
                    format,
                    &planned_keys,
                )?
            } else {
                clear_partitioned_resume_outputs(out_base_dir, format)?;
                (HashMap::new(), HashSet::new())
            };
            if resume {
                prune_partitioned_outputs_except(out_base_dir, format, &completed_keys)?;
            }
            let resumed_lines = committed_line_count(&initial_months);
            let output_records = AtomicU64::new(resumed_lines);
            let output_files = AtomicU64::new(completed_keys.len() as u64);
            let accumulator = if resume {
                crate::progress_manifest::save(
                    out_base_dir,
                    &initial_months,
                    Some(&resume_fingerprint),
                )?;
                Some(ManifestAccumulator::new(
                    out_base_dir,
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
            let total_bytes = total_compressed_size(&files);
            let pb = if plan.etl.opts.progress {
                Some(make_progress_bar_labeled(
                    total_bytes,
                    plan.etl.opts.progress_label.as_deref(),
                ))
            } else {
                None
            };
            let read_buf = plan.etl.opts.read_buffer_bytes;
            let write_buf = plan.etl.opts.write_buffer_bytes;
            let human_ts = plan.etl.opts.human_readable_timestamps;
            let zst_level = plan.etl.opts.zst_level;

            crate::concurrency::for_each_file_limited(
                &files,
                plan.etl.opts.file_concurrency,
                |job| -> Result<()> {
                    let key = export_part_key(job);
                    let out_path = partitioned_output_path(out_base_dir, job, format);

                    if record_limit
                        .as_ref()
                        .is_some_and(|limit| limit.is_exhausted())
                    {
                        return Ok(());
                    }

                    if resume && completed_keys.contains(&key) {
                        if let Some(pb) = &pb {
                            let sz = fs::metadata(&job.path).map(|m| m.len()).unwrap_or(0);
                            pb.inc(sz);
                        }
                        return Ok(());
                    }

                    let written_result = match format {
                        ExportFormat::Jsonl => {
                            write_jsonl_atomic(&staging_dir, &out_path, write_buf, |w| {
                                let result = stream_job_with_partial_policy(
                                    job,
                                    w,
                                    targets_ref,
                                    &plan.query,
                                    &whitelist,
                                    pb.clone(),
                                    bounds,
                                    read_buf,
                                    human_ts,
                                    whitelist_tracker.as_deref(),
                                    plan.etl.opts.allow_partial,
                                    Some(&plan.etl.opts.partial_read_reporter),
                                    record_limit.as_deref(),
                                )?;
                                complete_stream_job(job, result)
                            })
                        }
                        ExportFormat::Zst => {
                            write_zst_atomic(&staging_dir, &out_path, zst_level, write_buf, |w| {
                                let result = stream_job_with_partial_policy(
                                    job,
                                    w,
                                    targets_ref,
                                    &plan.query,
                                    &whitelist,
                                    pb.clone(),
                                    bounds,
                                    read_buf,
                                    human_ts,
                                    whitelist_tracker.as_deref(),
                                    plan.etl.opts.allow_partial,
                                    Some(&plan.etl.opts.partial_read_reporter),
                                    record_limit.as_deref(),
                                )?;
                                complete_stream_job(job, result)
                            })
                        }
                    };

                    let written = match written_result {
                        Ok(n) => n,
                        Err(e) if plan.etl.opts.allow_partial && is_partial_scan_error(&e) => {
                            tracing::warn!(path=%job.path.display(), error=%e, "Skipping partitioned export month after zstd decode error; staged output was discarded");
                            return Ok(());
                        }
                        Err(e) => return Err(e),
                    };

                    output_records.fetch_add(written, Ordering::Relaxed);
                    if written == 0 {
                        let _ = crate::util::remove_with_short_backoff(&out_path);
                    } else {
                        output_files.fetch_add(1, Ordering::Relaxed);
                    }
                    if let Some(acc) = &accumulator {
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
                },
            )?;

            if let Some(tracker) = &whitelist_tracker {
                tracker.finalize()?;
            }
            if let Some(pb) = pb {
                pb.finish_with_message("done");
            }
            ensure_resume_manifest_durable(accumulator.as_ref(), "partitioned export")?;
            let manifest = scan_manifest_input(
                manifest_start,
                "scan.export_partitioned",
                partitioned_resume_operation(format),
                &plan.etl,
                &plan.query,
                &files,
                plan.limit,
                manifest_counts(&[
                    ("records_written", output_records.load(Ordering::Relaxed)),
                    ("partition_files", output_files.load(Ordering::Relaxed)),
                ]),
                Some(resume_fingerprint.clone()),
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
        })
    }
}

fn load_and_validate_partitioned_manifest(
    out_base_dir: &Path,
    fingerprint: &str,
    format: ExportFormat,
    planned_keys: &HashSet<String>,
) -> Result<(HashMap<String, MonthEntry>, HashSet<String>)> {
    let manifest = crate::progress_manifest::load(out_base_dir);
    if manifest.fingerprint.as_deref() != Some(fingerprint) && !manifest.months.is_empty() {
        tracing::warn!(
            path=%crate::progress_manifest::manifest_path(out_base_dir).display(),
            stored=?manifest.fingerprint,
            current=%fingerprint,
            "partitioned resume manifest fingerprint does not match current query/config; discarding partitioned outputs"
        );
        clear_partitioned_resume_outputs(out_base_dir, format)?;
        return Ok((HashMap::new(), HashSet::new()));
    }

    let mut keep: HashMap<String, MonthEntry> = HashMap::new();
    for (key, entry) in manifest.months {
        if !planned_keys.contains(&key) {
            tracing::info!(key=%key, "dropping partitioned progress entry outside current plan");
            continue;
        }
        let Some(path) = partitioned_output_path_for_key(out_base_dir, &key, format) else {
            tracing::info!(key=%key, "dropping unrecognized partitioned progress entry");
            continue;
        };
        match validate_partitioned_resume_output(&path, format, &entry) {
            Ok(()) => {
                keep.insert(key, entry);
            }
            Err(e) => {
                tracing::info!(key=%key, path=%path.display(), error=%e, "dropping stale partitioned progress entry; month will be re-run");
            }
        }
    }
    let completed_keys: HashSet<String> = keep.keys().cloned().collect();
    Ok((keep, completed_keys))
}
