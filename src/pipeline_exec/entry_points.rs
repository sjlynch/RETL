
impl RedditETL {
    #[deprecated(note = "use RedditETL::scan().usernames()")]
    pub fn usernames(self) -> Result<UsernameStream> {
        let subreddit = self
            .opts
            .subreddit
            .clone()
            .ok_or_else(|| anyhow!("subreddit is required"))?;
        let parallelism = self.opts.parallelism;

        with_thread_pool(parallelism, || {
            let work_dir = self.ensure_work_dir()?;
            let files = plan_pipeline_files(&self, None)?;
            tracing::info!("Planned {} files for processing.", files.len());

            let shard_writer =
                ShardedWriter::create(&work_dir, "usernames", self.opts.shard_count)?;
            let scratch_root = shard_writer.scratch_root().to_path_buf();
            let result = (|| -> Result<UsernameStream> {
                let read_buf = self.opts.read_buffer_bytes;

                let total_bytes = total_compressed_size(&files);
                let pb = if self.opts.progress {
                    Some(make_progress_bar_labeled(
                        total_bytes,
                        self.opts.progress_label.as_deref(),
                    ))
                } else {
                    None
                };

                // Per-run counter of files skipped by the decoder. Surfaced to
                // observers via tracing at the end of the run. Per-file detail is
                // already logged by `warn_decode_skip`; this aggregate makes the
                // total visible without parsing log output.
                let skip_count = std::sync::Arc::new(AtomicU64::new(0));
                let skip_count_inner = skip_count.clone();

                crate::concurrency::for_each_file_limited(
                    &files,
                    self.opts.file_concurrency,
                    |job| {
                        let skip_count_per_call = skip_count_inner.clone();
                        process_file_for_usernames_with_skip(
                            job,
                            read_buf,
                            &subreddit,
                            &shard_writer,
                            pb.clone(),
                            self.opts.allow_partial,
                            Some(&self.opts.partial_read_reporter),
                            move |_path, _err| {
                                skip_count_per_call.fetch_add(1, Ordering::Relaxed);
                            },
                        )
                        .with_context(|| format!("processing {}", job.path.display()))
                    },
                )?;

                let skipped = skip_count.load(Ordering::Relaxed);
                if skipped > 0 {
                    tracing::warn!(
                        skipped_files = skipped,
                        "usernames: {} input file(s) skipped due to zstd decode errors; see prior warnings for paths",
                        skipped
                    );
                }

                let (deduped_files, scratch_root) = shard_writer.dedup_with_scratch("usernames")?;
                UsernameStream::from_deduped_files_with_cleanup(deduped_files, vec![scratch_root])
            })();
            if result.is_err() {
                cleanup_scratch_dir(&scratch_root, "usernames");
            }
            result
        })
    }
}

// -------- ScanPlan execution methods --------

impl ScanPlan {
    pub fn usernames(self) -> Result<UsernameStream> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        let parallelism = plan.etl.opts.parallelism;
        with_thread_pool(parallelism, || {
            let work_dir = plan.etl.ensure_work_dir()?;
            let shard_writer =
                ShardedWriter::create(&work_dir, "usernames_q", plan.etl.opts.shard_count)?;
            let scratch_root = shard_writer.scratch_root().to_path_buf();

            let result = (|| -> Result<UsernameStream> {
                if plan.etl.opts.resume {
                    let checkpoint = materialize_scan_checkpoint(
                        &plan.etl,
                        &plan.query,
                        /*show_progress=*/ true,
                        plan.limit,
                    )?;
                    for_each_checkpoint_record(
                        &checkpoint.parts,
                        plan.etl.opts.read_buffer_bytes,
                        |min, _line| {
                            if let Some(a) = min.author.as_deref() {
                                let a = a.trim();
                                if a.is_empty() {
                                    return Ok(());
                                }
                                shard_writer.write(a)?;
                            }
                            Ok(())
                        },
                    )?;
                } else {
                    scan_records(
                        &plan.etl,
                        &plan.query,
                        /*show_progress=*/ true,
                        plan.limit,
                        |min, _kind, _line| {
                            if let Some(a) = min.author.as_deref() {
                                let a = a.trim();
                                if a.is_empty() {
                                    return Ok(());
                                }
                                shard_writer.write(a)?;
                            }
                            Ok(())
                        },
                    )?;
                }

                let (deduped, scratch_root) = shard_writer.dedup_with_scratch("usernames_q")?;
                UsernameStream::from_deduped_files_with_cleanup(deduped, vec![scratch_root])
            })();
            if result.is_err() {
                cleanup_scratch_dir(&scratch_root, "usernames_q");
            }
            result
        })
    }

    /// JS-like lambda: stream **deduped** usernames and invoke a callback for each one.
    /// Example:
    ///   etl.scan().subreddit("programming").for_each_username(|name| authors.push(name.to_string()))?;
    pub fn for_each_username<F>(self, mut f: F) -> Result<()>
    where
        F: FnMut(&str),
    {
        let mut it = self.usernames()?;
        while let Some(u) = it.next() {
            f(&u);
        }
        Ok(())
    }

    /// Fallible variant of [`ScanPlan::for_each_username`]. Callback errors and
    /// stream open/read errors are returned to the caller instead of being
    /// ignored or logged-and-skipped.
    pub fn try_for_each_username<F>(self, mut f: F) -> Result<()>
    where
        F: FnMut(&str) -> Result<()>,
    {
        let mut it = self.usernames()?;
        while let Some(u) = it.try_next() {
            let u = u?;
            f(&u)?;
        }
        Ok(())
    }

    pub fn extract_to_jsonl(self, out_path: &Path) -> Result<()> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        let targets = resolve_target_subs_from(&plan.etl.opts.subreddit, &plan.query.subreddits);
        extract_common(
            &plan.etl,
            &plan.query,
            targets.as_ref(),
            "extract_jsonl_q_tmp",
            out_path,
            Finalize::Jsonl,
            "jsonl",
            "scan.extract_to_jsonl",
            plan.limit,
        )
    }

    /// Write the distinct keys from matching records as one key per line.
    ///
    /// The scan/filter pass first spools matching raw JSONL records into the
    /// configured work directory, then reuses the public sorted-run dedupe
    /// engine (`build_runs_sorted` + `merge_runs_sorted`) with the supplied
    /// [`KeyExtractor`]. Built-in `author` and `subreddit` extractors keep the
    /// `MinimalRecord` fast path; `json:/pointer` extractors parse full JSON
    /// only for key extraction. Matching records whose key extractor returns
    /// `Ok(None)` are omitted from the output; use
    /// [`ScanPlan::dedupe_keys_to_lines_with_stats`] to inspect that count.
    pub fn dedupe_keys_to_lines(self, key: &KeyExtractor, out_path: &Path) -> Result<u64> {
        Ok(self
            .dedupe_keys_to_lines_with_stats(key, out_path)?
            .unique_keys)
    }

    /// Like [`ScanPlan::dedupe_keys_to_lines`], but returns matched-record,
    /// unique-key, and missing-key counts for diagnostics.
    pub fn dedupe_keys_to_lines_with_stats(
        self,
        key: &KeyExtractor,
        out_path: &Path,
    ) -> Result<DedupeKeySummary> {
        let plan = self.build()?;
        let parallelism = plan.etl.opts.parallelism;
        with_thread_pool(parallelism, || {
            let manifest_start = RunManifestStart::now();
            let files = plan_pipeline_files(&plan.etl, Some(&plan.query))?;
            let work_dir = plan.etl.ensure_work_dir()?;
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let tmp_dir = work_dir.join(format!("dedupe_{}_{}", std::process::id(), unique));
            crate::util::create_dir_all_with_default_backoff(&tmp_dir)
                .with_context(|| format!("creating dedupe work dir {}", tmp_dir.display()))?;

            let result = (|| -> Result<DedupeKeySummary> {
                let input = tmp_dir.join("matched.ndjson");
                let matched_total = if plan.etl.opts.resume {
                    let checkpoint = materialize_scan_checkpoint(
                        &plan.etl,
                        &plan.query,
                        /*show_progress=*/ true,
                        plan.limit,
                    )?;
                    copy_checkpoint_parts_to_jsonl(
                        &checkpoint.parts,
                        &input,
                        plan.etl.opts.write_buffer_bytes,
                    )?;
                    checkpoint.matched_records
                } else {
                    let f = crate::util::create_with_default_backoff(&input)
                        .with_context(|| format!("create {}", input.display()))?;
                    let writer = Mutex::new(BufWriter::with_capacity(
                        plan.etl.opts.write_buffer_bytes,
                        f,
                    ));
                    let matched_records = AtomicU64::new(0);

                    scan_records(
                        &plan.etl,
                        &plan.query,
                        /*show_progress=*/ true,
                        plan.limit,
                        |_min, _kind, line| {
                            matched_records.fetch_add(1, Ordering::Relaxed);
                            let mut w = writer
                                .lock()
                                .map_err(|_| anyhow!("dedupe writer lock poisoned"))?;
                            w.write_all(line.as_bytes())?;
                            w.write_all(b"\n")?;
                            Ok(())
                        },
                    )?;
                    writer
                        .into_inner()
                        .map_err(|_| anyhow!("dedupe writer lock poisoned"))?
                        .flush()?;
                    matched_records.load(Ordering::Relaxed)
                };

                let key_extractions_failed = AtomicU64::new(0);
                let cfg = dedupe_cfg_from_options(&plan.etl.opts);

                let runs_dir = tmp_dir.join("runs");
                let runs = build_runs_sorted_with_key_stats(
                    &input,
                    &runs_dir,
                    key,
                    &cfg,
                    Some(&key_extractions_failed),
                )?;
                let failed_after_build = key_extractions_failed.load(Ordering::Relaxed);
                if plan.etl.opts.strict_key && failed_after_build > 0 {
                    let summary = DedupeKeySummary {
                        matched_records: matched_total,
                        unique_keys: 0,
                        key_extractions_failed: failed_after_build,
                    };
                    if matched_total > 0 && runs.is_empty() {
                        warn_dedupe_zero_keys(key, matched_total);
                    }
                    warn_dedupe_key_drops(key, &summary);
                    anyhow::bail!(
                        "--strict-key: {} of {} matching record(s) did not contain extractable key {}; aborting dedupe output",
                        failed_after_build,
                        matched_total,
                        dedupe_key_label(key),
                    );
                }

                let mut unique_count = 0_u64;
                merge_runs_sorted_with_key_stats(
                    &runs,
                    out_path,
                    key,
                    &cfg,
                    |current_key, _group, w| {
                        w.write_all(current_key.as_bytes())?;
                        w.write_all(b"\n")?;
                        unique_count += 1;
                        Ok(())
                    },
                    Some(&key_extractions_failed),
                )?;

                let key_extractions_failed = key_extractions_failed.load(Ordering::Relaxed);
                let summary = DedupeKeySummary {
                    matched_records: matched_total,
                    unique_keys: unique_count,
                    key_extractions_failed,
                };
                if matched_total > 0 && unique_count == 0 {
                    warn_dedupe_zero_keys(key, matched_total);
                }
                warn_dedupe_key_drops(key, &summary);

                let manifest = scan_manifest_input(
                    manifest_start,
                    "scan.dedupe_keys_to_lines",
                    "text-lines",
                    &plan.etl,
                    &plan.query,
                    &files,
                    plan.limit,
                    manifest_counts(&[
                        ("matched_records", summary.matched_records),
                        ("unique_keys", summary.unique_keys),
                        ("key_extractions_failed", summary.key_extractions_failed),
                    ]),
                    None,
                    None,
                    serde_json::json!({ "dedupe_key": dedupe_key_label(key) }),
                );
                maybe_write_run_manifest(
                    plan.etl.opts.emit_manifest,
                    manifest,
                    ManifestDestination::File(out_path.to_path_buf()),
                )?;

                Ok(summary)
            })();

            let _ = crate::util::remove_dir_all_with_short_backoff(&tmp_dir);
            result
        })
    }

    pub fn extract_to_json(self, out_path: &Path, pretty: bool) -> Result<()> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        let targets = resolve_target_subs_from(&plan.etl.opts.subreddit, &plan.query.subreddits);
        extract_common(
            &plan.etl,
            &plan.query,
            targets.as_ref(),
            "extract_json_q_tmp",
            out_path,
            Finalize::JsonArray { pretty },
            "json",
            "scan.extract_to_json",
            plan.limit,
        )
    }

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

    pub fn count_by_month(self) -> Result<BTreeMap<YearMonth, u64>> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        let parallelism = plan.etl.opts.parallelism;
        with_thread_pool(parallelism, || {
            let total = Mutex::new(BTreeMap::<YearMonth, u64>::new());
            if plan.etl.opts.resume {
                let checkpoint = materialize_scan_checkpoint(
                    &plan.etl,
                    &plan.query,
                    /*show_progress=*/ false,
                    plan.limit,
                )?;
                for_each_checkpoint_record(
                    &checkpoint.parts,
                    plan.etl.opts.read_buffer_bytes,
                    |min, _line| {
                        if let Some(ts) = min.created_utc {
                            let ym = ym_from_epoch(ts);
                            *total.lock().unwrap().entry(ym).or_insert(0) += 1;
                        }
                        Ok(())
                    },
                )?;
            } else {
                scan_records(
                    &plan.etl,
                    &plan.query,
                    /*show_progress=*/ false,
                    plan.limit,
                    |min, _kind, _line| {
                        if let Some(ts) = min.created_utc {
                            let ym = ym_from_epoch(ts);
                            *total.lock().unwrap().entry(ym).or_insert(0) += 1;
                        }
                        Ok(())
                    },
                )?;
            }
            Ok(total.into_inner().unwrap())
        })
    }

    pub fn author_counts_to_tsv(self, out_path: &Path) -> Result<()> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        let parallelism = plan.etl.opts.parallelism;
        with_thread_pool(parallelism, || {
            let manifest_start = RunManifestStart::now();
            let files = plan_pipeline_files(&plan.etl, Some(&plan.query))?;
            let work_dir = plan.etl.ensure_work_dir()?;
            let kv =
                ShardedKVWriter::create(&work_dir, "author_counts", plan.etl.opts.shard_count)?;
            let scratch_root = kv.scratch_root().to_path_buf();

            let result = (|| -> Result<()> {
                let matched_records = AtomicU64::new(0);
                if plan.etl.opts.resume {
                    let checkpoint = materialize_scan_checkpoint(
                        &plan.etl,
                        &plan.query,
                        /*show_progress=*/ false,
                        plan.limit,
                    )?;
                    for_each_checkpoint_record(
                        &checkpoint.parts,
                        plan.etl.opts.read_buffer_bytes,
                        |min, _line| {
                            if let Some(a) = min.author.as_deref() {
                                let a = a.trim();
                                if a.is_empty() {
                                    return Ok(());
                                }
                                matched_records.fetch_add(1, Ordering::Relaxed);
                                kv.write_kv(a, 1)?;
                            }
                            Ok(())
                        },
                    )?;
                } else {
                    scan_records(
                        &plan.etl,
                        &plan.query,
                        /*show_progress=*/ false,
                        plan.limit,
                        |min, _kind, _line| {
                            if let Some(a) = min.author.as_deref() {
                                let a = a.trim();
                                if a.is_empty() {
                                    return Ok(());
                                }
                                matched_records.fetch_add(1, Ordering::Relaxed);
                                kv.write_kv(a, 1)?;
                            }
                            Ok(())
                        },
                    )?;
                }

                let (shards, _scratch_root) = kv.reduce_sum_with_scratch("author_counts")?;
                concat_tsvs(&shards, out_path, plan.etl.opts.write_buffer_bytes)?;
                let output_rows = count_text_lines(out_path)?;
                let manifest = scan_manifest_input(
                    manifest_start,
                    "scan.author_counts_to_tsv",
                    "tsv",
                    &plan.etl,
                    &plan.query,
                    &files,
                    plan.limit,
                    manifest_counts(&[
                        ("matched_records", matched_records.load(Ordering::Relaxed)),
                        ("output_rows", output_rows),
                    ]),
                    None,
                    None,
                    serde_json::json!({}),
                );
                maybe_write_run_manifest(
                    plan.etl.opts.emit_manifest,
                    manifest,
                    ManifestDestination::File(out_path.to_path_buf()),
                )?;
                Ok(())
            })();
            cleanup_scratch_dir(&scratch_root, "author_counts");
            result
        })
    }

    pub fn build_first_seen_index_to_tsv(self, out_path: &Path) -> Result<()> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        let parallelism = plan.etl.opts.parallelism;
        with_thread_pool(parallelism, || {
            let manifest_start = RunManifestStart::now();
            let files = plan_pipeline_files(&plan.etl, Some(&plan.query))?;
            let work_dir = plan.etl.ensure_work_dir()?;
            let kv = ShardedKVWriter::create(&work_dir, "first_seen", plan.etl.opts.shard_count)?;
            let scratch_root = kv.scratch_root().to_path_buf();

            let result = (|| -> Result<()> {
                let matched_records = AtomicU64::new(0);
                if plan.etl.opts.resume {
                    let checkpoint = materialize_scan_checkpoint(
                        &plan.etl,
                        &plan.query,
                        /*show_progress=*/ false,
                        plan.limit,
                    )?;
                    for_each_checkpoint_record(
                        &checkpoint.parts,
                        plan.etl.opts.read_buffer_bytes,
                        |min, _line| {
                            if let (Some(a), Some(ts)) = (min.author.as_deref(), min.created_utc) {
                                let a = a.trim();
                                if a.is_empty() {
                                    return Ok(());
                                }
                                matched_records.fetch_add(1, Ordering::Relaxed);
                                kv.write_kv(a, ts)?;
                            }
                            Ok(())
                        },
                    )?;
                } else {
                    scan_records(
                        &plan.etl,
                        &plan.query,
                        /*show_progress=*/ false,
                        plan.limit,
                        |min, _kind, _line| {
                            if let (Some(a), Some(ts)) = (min.author.as_deref(), min.created_utc) {
                                let a = a.trim();
                                if a.is_empty() {
                                    return Ok(());
                                }
                                matched_records.fetch_add(1, Ordering::Relaxed);
                                kv.write_kv(a, ts)?;
                            }
                            Ok(())
                        },
                    )?;
                }

                let (shards, _scratch_root) = kv.reduce_min_with_scratch("first_seen")?;
                concat_tsvs(&shards, out_path, plan.etl.opts.write_buffer_bytes)?;
                let output_rows = count_text_lines(out_path)?;
                let manifest = scan_manifest_input(
                    manifest_start,
                    "scan.build_first_seen_index_to_tsv",
                    "tsv",
                    &plan.etl,
                    &plan.query,
                    &files,
                    plan.limit,
                    manifest_counts(&[
                        ("matched_records", matched_records.load(Ordering::Relaxed)),
                        ("output_rows", output_rows),
                    ]),
                    None,
                    None,
                    serde_json::json!({}),
                );
                maybe_write_run_manifest(
                    plan.etl.opts.emit_manifest,
                    manifest,
                    ManifestDestination::File(out_path.to_path_buf()),
                )?;
                Ok(())
            })();
            cleanup_scratch_dir(&scratch_root, "first_seen");
            result
        })
    }

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

// ----------------- extract_spool_monthly helpers -----------------
