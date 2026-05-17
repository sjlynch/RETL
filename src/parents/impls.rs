
impl RedditETL {
    pub fn resolve_parent_maps(
        &self,
        ids: &ParentIds,
        cache_dir: &Path,
        resume: bool,
    ) -> Result<ParentMaps> {
        with_thread_pool(self.opts.parallelism, || {
            let comments_out = cache_dir.join("comments");
            let submissions_out = cache_dir.join("submissions");
            crate::util::create_dir_all_with_default_backoff(&comments_out).with_context(|| {
                format!(
                    "create comments parent cache dir {}",
                    comments_out.display()
                )
            })?;
            crate::util::create_dir_all_with_default_backoff(&submissions_out).with_context(|| {
                format!(
                    "create submissions parent cache dir {}",
                    submissions_out.display()
                )
            })?;

            // Sweep crash leftovers under `<cache>/_staging/`: shard writers
            // stage there now (see `build_id_shard_index`), and any file owned
            // by a no-longer-running PID is a partial flush from a prior
            // crashed run that should not be promoted.
            sweep_stale_inprogress(&comments_out, true)?;
            sweep_stale_inprogress(&submissions_out, true)?;

            let discovered =
                discover_all_checked(&self.opts.comments_dir, &self.opts.submissions_dir)?;
            let files = plan_files_checked(
                &discovered,
                &self.opts.comments_dir,
                &self.opts.submissions_dir,
                crate::config::Sources::Both,
                self.opts.start,
                self.opts.end,
            )
            .with_context(|| {
                format!(
                    "resolve_parent_maps planned zero corpus files for resolver range {:?}..={:?}",
                    self.opts.start, self.opts.end
                )
            })?;
            let parent_ids_fp = parent_ids_fingerprint(ids)?;
            let resolution_range = attach_resolution_range(self.opts.start, self.opts.end);
            let payload_spec = self.opts.parent_payload_spec.clone();

            let total_bytes = total_compressed_size(&files);
            let pb = if self.opts.progress {
                Some(make_progress_bar_labeled(
                    total_bytes,
                    self.opts.progress_label.as_deref(),
                ))
            } else {
                None
            };

            let (comment_shards, submission_shards) = build_id_shard_index(
                &files,
                ids,
                &parent_ids_fp,
                &resolution_range,
                &payload_spec,
                &comments_out,
                &submissions_out,
                resume,
                self.opts.read_buffer_bytes,
                self.opts.write_buffer_bytes,
                self.opts.file_concurrency,
                pb.as_ref(),
            )?;

            if let Some(pb) = pb {
                let final_msg = if let Some(l) = self.opts.progress_label.as_deref() {
                    format!("{l} done")
                } else {
                    "done".to_string()
                };
                pb.finish_with_message(final_msg);
            }

            // Much stricter: only eager-load when plenty of RAM is free.
            let eager_ok = available_memory_fraction() > 0.50;

            let mut comments_map: HashMap<String, String> = HashMap::new();
            let mut submissions_map: HashMap<String, (String, String)> = HashMap::new();

            if eager_ok && payload_spec.is_legacy_default() {
                let load = |dir: &Path| -> Vec<PathBuf> {
                    let mut v: Vec<PathBuf> = crate::util::read_dir_with_default_backoff(dir)
                        .map_err(|e| {
                            tracing::warn!(dir=%dir.display(), error=%e, "failed to eager-list parent cache shards");
                            e
                        })
                        .ok()
                        .into_iter()
                        .flat_map(|entries| entries.into_iter().map(|e| e.path()))
                        .filter(|p| {
                            p.file_name()
                                .and_then(|name| name.to_str())
                                .map(|name| {
                                    name.ends_with(".json")
                                        && !name.ends_with(RESOLVER_SIDECAR_SUFFIX)
                                        && !name.ends_with(INPROGRESS_EXT)
                                })
                                .unwrap_or(false)
                        })
                        .collect();
                    v.sort();
                    v
                };

                for p in load(&comments_out) {
                    let f = crate::util::open_with_default_backoff(&p)?;
                    let r = BufReader::new(f);
                    let m: HashMap<String, String> = serde_json::from_reader(r)?;
                    for (k, v) in m {
                        comments_map.insert(k, v);
                    }
                    if is_low_memory(0.10) {
                        break;
                    }
                }
                for p in load(&submissions_out) {
                    let f = crate::util::open_with_default_backoff(&p)?;
                    let r = BufReader::new(f);
                    let m: HashMap<String, (String, String)> = serde_json::from_reader(r)?;
                    for (k, v) in m {
                        submissions_map.insert(k, v);
                    }
                    if is_low_memory(0.10) {
                        break;
                    }
                }
            }

            Ok(ParentMaps {
                comments: comments_map,
                submissions: submissions_map,
                comment_shards: Some(comment_shards),
                submission_shards: Some(submission_shards),
                payload_spec,
            })
        })
    }

    /// Write resolved parent payloads for direct `t1_...` / `t3_...` IDs as
    /// JSONL. Each emitted object is the same payload shape used under
    /// attached records' `parent` key: selected/full parent fields plus RETL's
    /// `kind` (`comment`/`submission`) and bare `id` metadata. Unresolved IDs
    /// are counted but omitted from the output.
    pub fn write_resolved_parent_payloads_jsonl<I, S>(
        &self,
        prefixed_ids: I,
        out: &Path,
        parents: &ParentMaps,
    ) -> Result<ParentAttachStats>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let parent_dir = output_parent(out);
        crate::util::create_dir_all_with_default_backoff(parent_dir)
            .with_context(|| format!("create direct parent output dir {}", parent_dir.display()))?;
        let staging_dir = ensure_staging_dir(parent_dir)?;
        sweep_stale_inprogress(parent_dir, true)?;

        let legacy_payload = parents.payload_spec.is_legacy_default();
        let empty_parent_payloads: HashMap<String, ParentPayload> = HashMap::new();
        let mut c_cache = WorkerShardCache::<String>::new(COMMENT_SHARD_CACHE_CAP);
        let mut s_cache = WorkerShardCache::<(String, String)>::new(SUBMISSION_SHARD_CACHE_CAP);
        let mut c_payload_cache = WorkerShardCache::<ParentPayload>::new(COMMENT_SHARD_CACHE_CAP);
        let mut s_payload_cache =
            WorkerShardCache::<ParentPayload>::new(SUBMISSION_SHARD_CACHE_CAP);

        write_jsonl_atomic(
            &staging_dir,
            out,
            self.opts.write_buffer_bytes,
            |w| -> Result<ParentAttachStats> {
                let mut stats = ParentAttachStats::default();
                for raw_id in prefixed_ids {
                    let raw_id = raw_id.as_ref().trim();
                    let payload = if let Some(rest) = raw_id.strip_prefix("t1_") {
                        if legacy_payload {
                            load_shard_value(
                                &parents.comments,
                                parents.comment_shards.as_ref(),
                                &mut c_cache,
                                rest,
                                None,
                            )?
                            .map(|body| {
                                let mut payload = ParentPayload::new();
                                payload.insert("kind".into(), Value::String("comment".into()));
                                payload.insert("id".into(), Value::String(rest.to_string()));
                                payload.insert("body".into(), Value::String(body));
                                payload
                            })
                        } else {
                            load_shard_value(
                                &empty_parent_payloads,
                                parents.comment_shards.as_ref(),
                                &mut c_payload_cache,
                                rest,
                                None,
                            )?
                            .map(|mut payload| {
                                payload.insert("kind".into(), Value::String("comment".into()));
                                payload.insert("id".into(), Value::String(rest.to_string()));
                                payload
                            })
                        }
                    } else if let Some(rest) = raw_id.strip_prefix("t3_") {
                        if legacy_payload {
                            load_shard_value(
                                &parents.submissions,
                                parents.submission_shards.as_ref(),
                                &mut s_cache,
                                rest,
                                None,
                            )?
                            .map(|(title, selftext)| {
                                let mut payload = ParentPayload::new();
                                payload.insert("kind".into(), Value::String("submission".into()));
                                payload.insert("id".into(), Value::String(rest.to_string()));
                                payload.insert("title".into(), Value::String(title));
                                payload.insert("selftext".into(), Value::String(selftext));
                                payload
                            })
                        } else {
                            load_shard_value(
                                &empty_parent_payloads,
                                parents.submission_shards.as_ref(),
                                &mut s_payload_cache,
                                rest,
                                None,
                            )?
                            .map(|mut payload| {
                                payload.insert("kind".into(), Value::String("submission".into()));
                                payload.insert("id".into(), Value::String(rest.to_string()));
                                payload
                            })
                        }
                    } else {
                        anyhow::bail!("direct parent ID is not prefixed with t1_ or t3_: {raw_id}");
                    };

                    if let Some(payload) = payload {
                        serde_json::to_writer(&mut *w, &payload)?;
                        w.write_all(b"\n")?;
                        stats.resolved += 1;
                    } else {
                        stats.unresolved += 1;
                    }
                }
                Ok(stats)
            },
        )
    }

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
