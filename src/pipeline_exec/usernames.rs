// Usernames collection paths: the deprecated `RedditETL::usernames` shim and
// the query-aware `ScanPlan::usernames` (plus the JS-like `for_each_username`
// / `try_for_each_username` convenience wrappers).

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
}
