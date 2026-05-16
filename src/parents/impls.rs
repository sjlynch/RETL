
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

            let parents_c_eager = &parents.comments;
            let parents_s_eager = &parents.submissions;
            let empty_parent_payloads: HashMap<String, ParentPayload> = HashMap::new();
            let legacy_payload = parents.payload_spec.is_legacy_default();

            let comment_shards = parents.comment_shards.as_ref();
            let submission_shards = parents.submission_shards.as_ref();
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

                    // Per-worker FIFO caches of parsed shard JSON. See
                    // `WorkerShardCache` for why eviction is plain FIFO with no
                    // bump-on-hit.
                    let mut c_cache = WorkerShardCache::<String>::new(COMMENT_SHARD_CACHE_CAP);
                    let mut s_cache =
                        WorkerShardCache::<(String, String)>::new(SUBMISSION_SHARD_CACHE_CAP);
                    let mut c_payload_cache =
                        WorkerShardCache::<ParentPayload>::new(COMMENT_SHARD_CACHE_CAP);
                    let mut s_payload_cache =
                        WorkerShardCache::<ParentPayload>::new(SUBMISSION_SHARD_CACHE_CAP);

                    let is_rc_spool_part = looks_like_rc_spool_path(in_path);
                    let (file_stats, diagnostics) = write_jsonl_atomic(
                        &staging_dir,
                        &out_path,
                        self.opts.write_buffer_bytes,
                        |w| -> Result<(ParentAttachStats, ParentAttachDiagnostics)> {
                            let mut file_stats = ParentAttachStats::default();
                            let mut diagnostics = ParentAttachDiagnostics {
                                files_scanned: 1,
                                ..Default::default()
                            };
                            let f = crate::util::open_with_default_backoff(in_path)?;
                            let mut r = BufReader::new(f);
                            let mut line_buf = String::new();
                            let mut line_no: u64 = 0;

                            loop {
                                let n = read_line_capped(
                                    &mut r,
                                    &mut line_buf,
                                    DEFAULT_MAX_LINE_BYTES,
                                    in_path,
                                )
                                .with_context(|| {
                                    format!(
                                        "read parent attach input {} at line {}",
                                        in_path.display(),
                                        line_no + 1
                                    )
                                })?;
                                if n == 0 {
                                    break;
                                }
                                line_no += 1;
                                if line_buf.is_empty() {
                                    continue;
                                }
                                let mut v: Value =
                                    serde_json::from_str(&line_buf).with_context(|| {
                                        format!(
                                            "malformed JSON in parent attach input {} at line {}",
                                            in_path.display(),
                                            line_no
                                        )
                                    })?;

                                let has_body = v.get("body").is_some();
                                let has_parent_id = v.get("parent_id").is_some();
                                let has_link_id = v.get("link_id").is_some();
                                diagnostics.parsed_records += 1;
                                if is_rc_spool_part {
                                    diagnostics.rc_records += 1;
                                }
                                if has_body {
                                    diagnostics.records_with_body += 1;
                                }
                                if has_parent_id {
                                    diagnostics.records_with_parent_id += 1;
                                }
                                if has_link_id {
                                    diagnostics.records_with_link_id += 1;
                                }

                                let is_comment = is_comment_record_for_parent_attach(&v);
                                if is_comment {
                                    diagnostics.comment_shaped_records += 1;
                                    // Own-month is derived from the consuming record's
                                    // created_utc; used to pick the most-likely parent
                                    // shard before falling back to other months.
                                    let own_ym = v
                                        .get("created_utc")
                                        .and_then(|x| x.as_i64())
                                        .map(ym_from_epoch);

                                    if let Some(parent_id) =
                                        v.get("parent_id").and_then(|x| x.as_str())
                                    {
                                        let mut resolved_parent = false;

                                        if legacy_payload {
                                            let mut parent_obj = serde_json::Map::new();
                                            let mut payload_fields = 0usize;

                                            if let Some(rest) = parent_id.strip_prefix("t1_") {
                                                if let Some(text) = load_shard_value(
                                                    parents_c_eager,
                                                    comment_shards,
                                                    &mut c_cache,
                                                    rest,
                                                    own_ym,
                                                )? {
                                                    parent_obj.insert(
                                                        "kind".into(),
                                                        Value::String("comment".into()),
                                                    );
                                                    parent_obj.insert(
                                                        "id".into(),
                                                        Value::String(rest.to_string()),
                                                    );
                                                    parent_obj
                                                        .insert("body".into(), Value::String(text));
                                                    payload_fields += 1;
                                                }
                                            } else if let Some(rest) = parent_id.strip_prefix("t3_")
                                            {
                                                if let Some((title, selftext)) = load_shard_value(
                                                    parents_s_eager,
                                                    submission_shards,
                                                    &mut s_cache,
                                                    rest,
                                                    own_ym,
                                                )? {
                                                    parent_obj.insert(
                                                        "kind".into(),
                                                        Value::String("submission".into()),
                                                    );
                                                    parent_obj.insert(
                                                        "id".into(),
                                                        Value::String(rest.to_string()),
                                                    );
                                                    parent_obj.insert(
                                                        "title".into(),
                                                        Value::String(title),
                                                    );
                                                    parent_obj.insert(
                                                        "selftext".into(),
                                                        Value::String(selftext),
                                                    );
                                                    payload_fields += 2;
                                                }
                                            }

                                            if payload_fields > 0 {
                                                if let Some(map) = v.as_object_mut() {
                                                    map.insert(
                                                        "parent".into(),
                                                        Value::Object(parent_obj),
                                                    );
                                                }
                                                resolved_parent = true;
                                            }
                                        } else if let Some(rest) = parent_id.strip_prefix("t1_") {
                                            if let Some(mut payload) = load_shard_value(
                                                &empty_parent_payloads,
                                                comment_shards,
                                                &mut c_payload_cache,
                                                rest,
                                                own_ym,
                                            )? {
                                                payload.insert(
                                                    "kind".into(),
                                                    Value::String("comment".into()),
                                                );
                                                payload.insert(
                                                    "id".into(),
                                                    Value::String(rest.to_string()),
                                                );
                                                if let Some(map) = v.as_object_mut() {
                                                    map.insert(
                                                        "parent".into(),
                                                        Value::Object(payload),
                                                    );
                                                }
                                                resolved_parent = true;
                                            }
                                        } else if let Some(rest) = parent_id.strip_prefix("t3_") {
                                            if let Some(mut payload) = load_shard_value(
                                                &empty_parent_payloads,
                                                submission_shards,
                                                &mut s_payload_cache,
                                                rest,
                                                own_ym,
                                            )? {
                                                payload.insert(
                                                    "kind".into(),
                                                    Value::String("submission".into()),
                                                );
                                                payload.insert(
                                                    "id".into(),
                                                    Value::String(rest.to_string()),
                                                );
                                                if let Some(map) = v.as_object_mut() {
                                                    map.insert(
                                                        "parent".into(),
                                                        Value::Object(payload),
                                                    );
                                                }
                                                resolved_parent = true;
                                            }
                                        }

                                        if resolved_parent {
                                            file_stats.resolved += 1;
                                        } else {
                                            file_stats.unresolved += 1;
                                        }
                                    }
                                }

                                serde_json::to_writer(&mut *w, &v)?;
                                w.write_all(b"\n")?;
                            }

                            Ok((file_stats, diagnostics))
                        },
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

            let mut counts = BTreeMap::new();
            counts.insert("input_files".to_string(), manifest_inputs.len() as u64);
            counts.insert("attached_files".to_string(), out_paths.len() as u64);
            counts.insert("parents_resolved".to_string(), stats.resolved);
            counts.insert("parents_unresolved".to_string(), stats.unresolved);
            counts.insert("records_scanned".to_string(), diagnostics.parsed_records);
            let mut manifest = RunManifestInput::new("parents.attach_parents_jsonls_parallel");
            manifest.start = manifest_start;
            manifest.api_operation = Some("RedditETL::attach_parents_jsonls_parallel".to_string());
            manifest.options = serde_json::json!({
                "resume": resume,
                "resolution_range": {
                    "start": self.opts.start.map(|ym| ym.to_string()),
                    "end": self.opts.end.map(|ym| ym.to_string()),
                },
                "parent_payload": {
                    "full_record": parents.payload_spec.is_full_record(),
                    "fields": parents.payload_spec.fields(),
                },
                "base_dir": path_to_stable_string(&self.opts.base_dir),
                "comments_dir": path_to_stable_string(&self.opts.comments_dir),
                "submissions_dir": path_to_stable_string(&self.opts.submissions_dir),
                "parallelism": self.opts.parallelism,
                "file_concurrency": self.opts.file_concurrency,
                "emit_manifest": self.opts.emit_manifest,
            });
            manifest.inputs = file_identities(&manifest_inputs);
            manifest.output_format = "jsonl-directory".to_string();
            manifest.counts = counts;
            manifest.upstream_manifests = discover_upstream_manifests_from_inputs(&manifest_inputs);
            maybe_write_run_manifest(
                self.opts.emit_manifest,
                manifest,
                ManifestDestination::Directory(out_dir.to_path_buf()),
            )?;
            Ok((out_paths, stats))
        })
    }
}
