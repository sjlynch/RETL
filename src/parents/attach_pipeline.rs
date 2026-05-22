
impl RedditETL {
    pub fn attach_parents_jsonls_parallel(
        &self,
        inputs: Vec<PathBuf>,
        out_dir: &Path,
        parents: &ParentMaps,
        resume: bool,
    ) -> Result<Vec<PathBuf>> {
        Ok(self
            .attach_parents_jsonls_parallel_with_stats(inputs, out_dir, parents, resume)?
            .0)
    }

    pub fn attach_parents_jsonls_parallel_with_stats(
        &self,
        inputs: Vec<PathBuf>,
        out_dir: &Path,
        parents: &ParentMaps,
        resume: bool,
    ) -> Result<(Vec<PathBuf>, ParentAttachStats)> {
        with_thread_pool(self.opts.parallelism, || {
            let manifest_start = RunManifestStart::now();
            let manifest_inputs = inputs.clone();
            crate::util::create_dir_all_with_default_backoff(out_dir).with_context(|| {
                format!("create parent attach output dir {}", out_dir.display())
            })?;
            let staging_dir = ensure_staging_dir(out_dir)?;
            sweep_stale_inprogress(out_dir, true)?;

            let indexed_inputs: Vec<(usize, PathBuf)> = inputs.into_iter().enumerate().collect();
            ensure_unique_attach_basenames(&indexed_inputs)?;
            let keep_basenames = attach_output_basenames(&indexed_inputs)?;
            prune_stale_attach_outputs(out_dir, &keep_basenames)?;

            let label = self
                .opts
                .progress_label
                .as_deref()
                .unwrap_or("Attaching parents");
            let pb = if self.opts.progress {
                Some(make_count_progress(indexed_inputs.len() as u64, label))
            } else {
                None
            };

            let empty_parent_payloads: HashMap<String, ParentPayload> = HashMap::new();
            let file_ctx = AttachFileCtx {
                legacy_payload: parents.payload_spec.is_legacy_default(),
                parents_c_eager: &parents.comments,
                parents_s_eager: &parents.submissions,
                empty_parent_payloads: &empty_parent_payloads,
                comment_shards: parents.comment_shards.as_ref(),
                submission_shards: parents.submission_shards.as_ref(),
            };
            let parent_cache_fingerprint = attach_parent_cache_fingerprint(parents);
            let resolution_range = attach_resolution_range(self.opts.start, self.opts.end);

            diagnose_initial_attach_shape(&indexed_inputs, self.opts.read_buffer_bytes)?;
            let attached = std::sync::Mutex::new(vec![None; indexed_inputs.len()]);

            crate::concurrency::for_each_file_limited(
                &indexed_inputs,
                self.opts.file_concurrency,
                |(idx, in_path)| -> Result<()> {
                    let name = in_path
                        .file_name()
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "attach_parents input path has no file name: {}",
                                in_path.display()
                            )
                        })?
                        .to_string_lossy()
                        .to_string();
                    let out_path = out_dir.join(name);
                    let inprogress_exists = attach_inprogress_exists(&staging_dir, &out_path)?;
                    let sidecar_path = attach_fingerprint_path(&out_path);
                    let fingerprint = build_attach_fingerprint(
                        in_path,
                        &parent_cache_fingerprint,
                        &resolution_range,
                    );

                    if resume && out_path.exists() && !inprogress_exists {
                        if attach_fingerprint_matches(&sidecar_path, &fingerprint) {
                            if validate_jsonl_file(&out_path) {
                                if let Some(pb) = &pb {
                                    pb.inc(1);
                                }
                                let diagnostics = ParentAttachDiagnostics {
                                    resume_skipped_files: 1,
                                    ..Default::default()
                                };
                                attached.lock().unwrap()[*idx] =
                                    Some((out_path, ParentAttachStats::default(), diagnostics));
                                return Ok(());
                            }
                            tracing::warn!(path=%out_path.display(), "resume: existing attached JSONL is unreadable/corrupt, rebuilding");
                        } else {
                            tracing::debug!(path=%out_path.display(), sidecar=%sidecar_path.display(), "resume: attached JSONL fingerprint missing or stale, rebuilding");
                        }
                    }

                    let is_rc_spool_part = looks_like_rc_spool_path(in_path);
                    let (file_stats, diagnostics) = write_jsonl_atomic(
                        &staging_dir,
                        &out_path,
                        self.opts.write_buffer_bytes,
                        |w| attach_parents_for_one_file(in_path, w, &file_ctx, is_rc_spool_part),
                    )?;

                    write_attach_fingerprint_atomic(
                        &staging_dir,
                        &sidecar_path,
                        &fingerprint,
                        self.opts.write_buffer_bytes,
                    )?;

                    if let Some(pb) = &pb {
                        pb.inc(1);
                    }
                    attached.lock().unwrap()[*idx] = Some((out_path, file_stats, diagnostics));
                    Ok(())
                },
            )?;

            if let Some(pb) = pb {
                let final_msg = format!("{label} done");
                pb.finish_with_message(final_msg);
            }

            let mut stats = ParentAttachStats::default();
            let mut diagnostics = ParentAttachDiagnostics::default();
            let out_paths: Vec<PathBuf> = attached
                .into_inner()
                .unwrap()
                .into_iter()
                .map(|entry| {
                    let (path, file_stats, file_diagnostics) =
                        entry.expect("attach result missing after success");
                    stats.add(file_stats);
                    diagnostics.add(file_diagnostics);
                    path
                })
                .collect();
            warn_if_no_comment_shaped_records(diagnostics);
            warn_if_malformed_parent_ids(diagnostics);

            let manifest = build_attach_manifest(
                &manifest_inputs,
                &out_paths,
                stats,
                diagnostics,
                manifest_start,
                &self.opts,
                &parents.payload_spec,
                resume,
            );
            maybe_write_run_manifest(
                self.opts.emit_manifest,
                manifest,
                ManifestDestination::Directory(out_dir.to_path_buf()),
            )?;
            Ok((out_paths, stats))
        })
    }
}
