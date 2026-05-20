
/// Per-entry validator for the extract scratch dir: re-reads the cached part
/// file and returns a freshly computed [`MonthEntry`] (size+line count). The
/// returned entry replaces the manifest's recorded line count, matching the
/// pre-refactor behavior of [`extract_to_jsonl`].
fn validate_extract_entry(tmp_dir: &Path, key: &str, expected: MonthEntry) -> Result<MonthEntry> {
    let part_path = export_part_path(tmp_dir, key);
    let actual = validate_jsonl_part(&part_path)?;
    if actual.size != expected.size {
        anyhow::bail!(
            "extract resume part size mismatch for {}: expected {}, got {}",
            part_path.display(),
            expected.size,
            actual.size
        );
    }
    Ok(actual)
}

/// Shared extraction logic used by:
///  - `ScanPlan::extract_to_jsonl`
///  - `ScanPlan::extract_to_json`
/// Keeps progress/date-bounds/streaming consistent and DRY.
fn extract_common(
    etl: &RedditETL,
    query: &QuerySpec,
    targets: Option<&Vec<String>>,
    tmp_dir_name: &str,
    out_path: &Path,
    finalize: Finalize,
    output_format: &str,
    operation: &str,
    limit: Option<u64>,
) -> Result<()> {
    validate_export_whitelist(etl)?;
    let parallelism = etl.opts.parallelism;
    with_thread_pool(parallelism, || {
        let manifest_start = RunManifestStart::now();
        let files = plan_pipeline_files(etl, Some(query))?;
        warn_if_unfiltered_undated_query(etl, query, &files);

        let work_dir = etl.ensure_work_dir()?;
        let resume = etl.opts.resume;
        let resume_fingerprint = if resume {
            Some(build_resume_fingerprint(
                etl, query, "extract", limit, &files,
            )?)
        } else {
            None
        };
        let tmp_dir = extract_scratch_dir(
            &work_dir,
            tmp_dir_name,
            out_path,
            resume,
            resume_fingerprint.as_deref(),
        )?;
        crate::util::create_dir_all_with_default_backoff(&tmp_dir)
            .with_context(|| format!("creating extract work dir {}", tmp_dir.display()))?;
        let staging_dir = ensure_staging_dir(&tmp_dir)?;
        sweep_stale_inprogress(&tmp_dir, true)?;

        let ResumePrelude {
            initial_months,
            completed_keys,
            accumulator,
        } = if let Some(fingerprint) = resume_fingerprint.as_deref() {
            prepare_resume_run(
                &tmp_dir,
                fingerprint,
                None,
                true,
                true,
                ResumeLogLabels {
                    fingerprint_mismatch:
                        "extract resume manifest fingerprint does not match current query/config; discarding cached parts",
                    out_of_plan: "dropping extract progress entry outside current plan",
                    stale_entry: "dropping stale extract progress entry; month will be re-run",
                },
                || clear_extract_resume_parts(&tmp_dir),
                |_keep_keys| Ok(()),
                |key, entry| validate_extract_entry(&tmp_dir, key, entry),
            )?
        } else {
            ResumePrelude {
                initial_months: HashMap::new(),
                completed_keys: HashSet::new(),
                accumulator: None,
            }
        };
        let resumed_lines = committed_line_count(&initial_months);
        let output_records = AtomicU64::new(resumed_lines);

        let whitelist = etl.opts.whitelist_fields.clone();
        let whitelist_tracker = whitelist.as_ref().map(|fields| {
            Arc::new(WhitelistMatchTracker::new(
                etl.opts.strict_whitelist,
                fields.iter().cloned(),
            ))
        });
        let record_limit = record_limit_from_with_claimed(limit, resumed_lines);

        let total_bytes = total_compressed_size(&files);
        let pb = if etl.opts.progress {
            Some(make_progress_bar_labeled(
                total_bytes,
                etl.opts.progress_label.as_deref(),
            ))
        } else {
            None
        };

        let targets_ref = targets;
        let bounds = bounds_tuple(etl.opts.start, etl.opts.end);
        let read_buf = etl.opts.read_buffer_bytes;
        let write_buf = etl.opts.write_buffer_bytes;
        let human_ts = etl.opts.human_readable_timestamps;

        crate::concurrency::for_each_file_limited(
            &files,
            etl.opts.file_concurrency,
            |job| -> Result<()> {
                let key = export_part_key(job);
                let tmp_file = export_part_path(&tmp_dir, &key);

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

                if resume && tmp_file.exists() {
                    match validate_jsonl_part(&tmp_file) {
                        Ok(entry) => {
                            output_records.fetch_add(entry.lines, Ordering::Relaxed);
                            if let Some(acc) = &accumulator {
                                acc.commit(key.clone(), entry).with_context(|| {
                                    format!(
                                        "failed to durably update extract progress manifest for existing resume part {key}"
                                    )
                                })?;
                            }
                            if let Some(pb) = &pb {
                                let sz = fs::metadata(&job.path).map(|m| m.len()).unwrap_or(0);
                                pb.inc(sz);
                            }
                            return Ok(());
                        }
                        Err(e) => {
                            tracing::info!(path=%tmp_file.display(), error=%e, "resume part failed validation; rebuilding");
                            let _ = crate::util::remove_with_short_backoff(&tmp_file);
                        }
                    }
                }

                let lines = match write_jsonl_atomic(&staging_dir, &tmp_file, write_buf, |w| {
                    let result = stream_job_with_partial_policy(
                        job,
                        w,
                        targets_ref,
                        query,
                        &whitelist,
                        pb.clone(),
                        bounds,
                        read_buf,
                        human_ts,
                        whitelist_tracker.as_deref(),
                        etl.opts.allow_partial,
                        Some(&etl.opts.partial_read_reporter),
                        record_limit.as_deref(),
                    )?;
                    complete_stream_job(job, result)
                }) {
                    Ok(lines) => lines,
                    Err(e) if etl.opts.allow_partial && is_partial_scan_error(&e) => {
                        tracing::warn!(path=%job.path.display(), part=%tmp_file.display(), error=%e, "Skipping extract month after zstd decode error; staged part was discarded and resume will retry it");
                        return Ok(());
                    }
                    Err(e) => return Err(e),
                };

                output_records.fetch_add(lines, Ordering::Relaxed);
                if let Some(acc) = &accumulator {
                    let size = fs::metadata(&tmp_file).map(|m| m.len()).unwrap_or(0);
                    let entry = MonthEntry {
                        size,
                        lines,
                        sha256: None,
                    };
                    acc.commit(key, entry).context(
                        "failed to durably update extract progress manifest after publishing resume part",
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
        ensure_resume_manifest_durable(accumulator.as_ref(), "extract")?;

        match finalize {
            Finalize::Jsonl => stitch_tmp_parts(&tmp_dir, out_path, write_buf)?,
            Finalize::JsonArray { pretty } => {
                stitch_tmp_parts_to_json_array(&tmp_dir, out_path, pretty, write_buf)?
            }
        }
        let manifest = scan_manifest_input(
            manifest_start,
            operation,
            output_format,
            etl,
            query,
            &files,
            limit,
            manifest_counts(&[("records_written", output_records.load(Ordering::Relaxed))]),
            resume_fingerprint.clone(),
            resume.then(|| crate::progress_manifest::manifest_path(&tmp_dir)),
            serde_json::json!({}),
        );
        maybe_write_run_manifest(
            etl.opts.emit_manifest,
            manifest,
            ManifestDestination::File(out_path.to_path_buf()),
        )?;
        // The scratch dir holds a complete per-month copy of the output (the
        // `.part_*.jsonl` files just stitched into `out_path`). The final
        // stitch succeeded above, so the checkpoint is no longer needed —
        // remove it for resumed runs too, otherwise every successful resumed
        // JSONL/JSON export silently leaves a full second copy of the output
        // behind in `work_dir`.
        if let Err(e) = crate::util::remove_dir_all_with_short_backoff(&tmp_dir) {
            tracing::warn!(path=%tmp_dir.display(), error=%e, "failed to remove extract scratch dir");
        }
        Ok(())
    })
}
