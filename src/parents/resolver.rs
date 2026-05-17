
#[inline]
fn idset_cache_cap_for_free_memory(free: f64) -> usize {
    if free > IDSET_CACHE_HIGH_FREE_THRESHOLD {
        IDSET_CACHE_HIGH_CAP
    } else if free > IDSET_CACHE_MEDIUM_FREE_THRESHOLD {
        IDSET_CACHE_MEDIUM_CAP
    } else {
        IDSET_CACHE_LOW_CAP
    }
}

fn parent_payload_from_line(line: &str, spec: &ParentPayloadSpec) -> Result<ParentPayload> {
    let v: Value = serde_json::from_str(line)?;
    let Some(obj) = v.as_object() else {
        return Ok(ParentPayload::new());
    };

    if spec.is_full_record() {
        return Ok(obj.clone());
    }

    let mut payload = ParentPayload::new();
    for field in spec.fields() {
        if let Some(value) = obj.get(field) {
            payload.insert(field.clone(), value.clone());
        }
    }
    Ok(payload)
}

fn build_id_shard_index(
    files: &[FileJob],
    ids: &ParentIds,
    parent_ids_fp: &ParentIdsFingerprint,
    resolution_range: &AttachResolutionRange,
    payload_spec: &ParentPayloadSpec,
    comments_out: &Path,
    submissions_out: &Path,
    resume: bool,
    read_buf: usize,
    write_buf: usize,
    file_concurrency: usize,
    pb: Option<&indicatif::ProgressBar>,
) -> Result<(HashMap<YearMonth, PathBuf>, HashMap<YearMonth, PathBuf>)> {
    // Shard-keyed indexes (one entry per processed monthly shard, NOT per id).
    // ~10^6x memory reduction vs. the prior per-id index at corpus scale.
    let comment_shards = parking_lot::Mutex::new(HashMap::<YearMonth, PathBuf>::new());
    let submission_shards = parking_lot::Mutex::new(HashMap::<YearMonth, PathBuf>::new());

    // Process-wide id-shard cache: each (t1|t3) ids_NNNN.txt shard is read
    // and parsed at most once globally instead of once per rayon worker.
    let idset_cap = idset_cache_cap_for_free_memory(available_memory_fraction());
    let idset_cache: Arc<SharedIdsetCache> = Arc::new(SharedIdsetCache::new(idset_cap));

    // Limit file concurrency to reduce RAM spikes while resolving.
    crate::concurrency::for_each_file_limited(files, file_concurrency, |job| -> Result<()> {
        let (out_dir, prefix) = match job.kind {
            FileKind::Comment => (comments_out, "RC"),
            FileKind::Submission => (submissions_out, "RS"),
        };
        let out = out_dir.join(format!("{}_{}.json", prefix, job.ym));
        let sidecar_path = resolver_fingerprint_path(&out);
        let fingerprint =
            build_resolver_fingerprint(job, parent_ids_fp, resolution_range, payload_spec);

        // Always record the shard path for downstream lookups, even on
        // resume — attach() needs the shard map populated regardless of
        // whether this run produced the file.
        let record_shard = |path: PathBuf| match job.kind {
            FileKind::Comment => {
                comment_shards.lock().insert(job.ym, path);
            }
            FileKind::Submission => {
                submission_shards.lock().insert(job.ym, path);
            }
        };

        if resume && out.exists() {
            // Validate both the shard JSON and its resolver fingerprint. The
            // sidecar binds this cache shard to the current ParentIds, source
            // corpus file, source kind/month, resolver format, and requested
            // resolver window; stale-but-parseable shards are rebuilt.
            let valid_json = match crate::util::open_with_default_backoff(&out) {
                Ok(f) if payload_spec.is_legacy_default() => match job.kind {
                    FileKind::Comment => {
                        serde_json::from_reader::<_, HashMap<String, String>>(BufReader::new(f))
                            .is_ok()
                    }
                    FileKind::Submission => serde_json::from_reader::<
                        _,
                        HashMap<String, (String, String)>,
                    >(BufReader::new(f))
                    .is_ok(),
                },
                Ok(f) => {
                    serde_json::from_reader::<_, HashMap<String, ParentPayload>>(BufReader::new(f))
                        .is_ok()
                }
                Err(_) => false,
            };
            if valid_json && resolver_fingerprint_matches(&sidecar_path, &fingerprint) {
                record_shard(out.clone());
                if let Some(pb) = pb {
                    let sz = fs::metadata(&job.path).map(|m| m.len()).unwrap_or(0);
                    pb.inc(sz);
                }
                return Ok(());
            }
            if !valid_json {
                tracing::warn!(path=%out.display(), "resume: existing parent shard is unreadable/corrupt, rebuilding");
            }
        }

        let ensure_contains = |shard_path: &Path, id: &str| -> Result<bool> {
            let set = idset_cache.get_or_load(shard_path)?;
            Ok(set.contains(id))
        };
        let id_needed = |mem: Option<&AHashSet<String>>,
                         sharded: Option<&IdShards>,
                         id: &str|
         -> Result<bool> {
            if mem.map(|ids| ids.contains(id)).unwrap_or(false) {
                return Ok(true);
            }
            if let Some(sh) = sharded {
                let idx = sh.idx(id);
                let p = sh.path_for(idx);
                return ensure_contains(&p, id);
            }
            Ok(false)
        };

        let legacy_payload = payload_spec.is_legacy_default();
        let mut out_map_c: HashMap<String, String> = HashMap::new();
        let mut out_map_s: HashMap<String, (String, String)> = HashMap::new();
        let mut out_payload_c: HashMap<String, ParentPayload> = HashMap::new();
        let mut out_payload_s: HashMap<String, ParentPayload> = HashMap::new();

        let mut line_number = 0u64;
        let completed = for_each_line_with_progress_cfg_no_throttle_status(
            &job.path,
            read_buf,
            |d| {
                if let Some(pb) = pb {
                    pb.inc(d);
                }
            },
            |line| {
                line_number += 1;
                let min = parse_minimal(line)
                    .map_err(|e| malformed_json_error(&job.path, line_number, e))?;
                if let Some(id) = min.id.as_deref() {
                    match job.kind {
                        FileKind::Comment => {
                            let needed = id_needed(
                                ids.t1_ids_mem.as_ref(),
                                ids.t1_ids_sharded.as_ref(),
                                id,
                            )?;

                            if needed {
                                if legacy_payload {
                                    if let Some(body) = min.body.as_deref() {
                                        out_map_c.insert(id.to_string(), body.to_string());
                                    }
                                } else {
                                    let payload = parent_payload_from_line(line, payload_spec)
                                        .map_err(|e| {
                                            malformed_json_error(&job.path, line_number, e)
                                        })?;
                                    out_payload_c.insert(id.to_string(), payload);
                                }
                            }
                        }
                        FileKind::Submission => {
                            let needed = id_needed(
                                ids.t3_ids_mem.as_ref(),
                                ids.t3_ids_sharded.as_ref(),
                                id,
                            )?;

                            if needed {
                                if legacy_payload {
                                    let title =
                                        min.title.as_deref().unwrap_or_default().to_string();
                                    let selftext =
                                        min.selftext.as_deref().unwrap_or_default().to_string();
                                    out_map_s.insert(id.to_string(), (title, selftext));
                                } else {
                                    let payload = parent_payload_from_line(line, payload_spec)
                                        .map_err(|e| {
                                            malformed_json_error(&job.path, line_number, e)
                                        })?;
                                    out_payload_s.insert(id.to_string(), payload);
                                }
                            }
                        }
                    }
                }
                Ok(())
            },
        )
        .with_context(|| format!("scan {}", job.path.display()))?;
        if !completed {
            return Err(anyhow::anyhow!(
                "incomplete zstd decode while resolving parent map from {}",
                job.path.display()
            ));
        }

        // Atomic write: stage under `<out_dir>/_staging/<basename>.retl-<pid>-<nonce>.inprogress`,
        // serialize the shard map into the staged file, flush, then atomically
        // replace the dest. Routing through `_staging/` keeps concurrent shard
        // writers from colliding on a shared sibling temp and lets a
        // subsequent run's sweep recover crash leftovers — see
        // `sweep_stale_inprogress` in `resolve_parent_maps`.
        write_at_path_atomic(&out, write_buf, |w| -> Result<()> {
            match (legacy_payload, job.kind) {
                (true, FileKind::Comment) => serde_json::to_writer(w, &out_map_c)?,
                (true, FileKind::Submission) => serde_json::to_writer(w, &out_map_s)?,
                (false, FileKind::Comment) => serde_json::to_writer(w, &out_payload_c)?,
                (false, FileKind::Submission) => serde_json::to_writer(w, &out_payload_s)?,
            }
            Ok(())
        })
        .with_context(|| format!("write parent shard {}", out.display()))?;

        write_resolver_fingerprint_atomic(&sidecar_path, &fingerprint, write_buf)?;
        record_shard(out.clone());

        Ok(())
    })?;

    Ok((comment_shards.into_inner(), submission_shards.into_inner()))
}
