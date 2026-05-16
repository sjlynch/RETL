
fn opt_ym_string(ym: Option<YearMonth>) -> Option<String> {
    ym.map(|ym| ym.to_string())
}

fn absolutize_for_fingerprint(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()
            .context("resolve current directory for path fingerprint")?
            .join(path))
    }
}

fn canonicalize_or_absolutize(path: &Path) -> Result<PathBuf> {
    match fs::canonicalize(path) {
        Ok(path) => Ok(path),
        Err(_) => absolutize_for_fingerprint(path),
    }
}

fn path_for_fingerprint(path: &Path) -> Result<String> {
    let path = if path.is_dir() {
        canonicalize_or_absolutize(path)?
    } else if let Some(file_name) = path.file_name() {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        canonicalize_or_absolutize(parent)?.join(file_name)
    } else {
        canonicalize_or_absolutize(path)?
    };
    Ok(path.to_string_lossy().into_owned())
}

fn stable_hash_component(bytes: &[u8]) -> String {
    stable_fnv1a_hex(bytes).replace(':', "_")
}

fn corpus_file_fingerprints(files: &[FileJob]) -> Result<Vec<Value>> {
    let mut out = Vec::with_capacity(files.len());
    for job in files {
        let metadata = fs::metadata(&job.path)
            .with_context(|| format!("stat corpus input {}", job.path.display()))?;
        let modified = metadata.modified().ok().map(system_time_parts);
        let (modified_unix_secs, modified_nanos) = match modified {
            Some((secs, nanos)) => (Some(secs), Some(nanos)),
            None => (None, None),
        };
        let prefix = match job.kind {
            FileKind::Comment => "RC",
            FileKind::Submission => "RS",
        };
        out.push(serde_json::json!({
            "key": crate::progress_manifest::month_key(prefix, job.ym),
            "kind": format!("{:?}", job.kind),
            "month": job.ym.to_string(),
            "path": path_for_fingerprint(&job.path)?,
            "len": metadata.len(),
            "modified_unix_secs": modified_unix_secs,
            "modified_nanos": modified_nanos,
        }));
    }
    Ok(out)
}

fn extract_scratch_dir(
    work_dir: &Path,
    tmp_dir_name: &str,
    out_path: &Path,
    resume: bool,
    resume_fingerprint: Option<&str>,
) -> Result<PathBuf> {
    let name = if resume {
        let fingerprint = resume_fingerprint
            .ok_or_else(|| anyhow!("missing resume fingerprint for extract scratch directory"))?;
        let input = serde_json::json!({
            "resume_fingerprint": fingerprint,
            "out_path": path_for_fingerprint(out_path)?,
        });
        let bytes = serde_json::to_vec(&input).context("serialize extract scratch fingerprint")?;
        format!("{tmp_dir_name}_{}", stable_hash_component(&bytes))
    } else {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let seq = EXTRACT_SCRATCH_COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{tmp_dir_name}_{}_{}_{}", std::process::id(), unique, seq)
    };
    Ok(work_dir.join(name))
}

/// Fingerprint every query/config knob and selected corpus file identity that
/// can change the records written by a resumable operation. If it changes
/// between `--resume` runs, stale month parts are discarded instead of being
/// stitched or reduced into the new run.
fn build_resume_fingerprint(
    etl: &RedditETL,
    query: &QuerySpec,
    operation: &str,
    limit: Option<u64>,
    files: &[FileJob],
) -> Result<String> {
    let zst_level = (operation == "partitioned-zst").then_some(etl.opts.zst_level);
    let input = serde_json::json!({
        "operation": operation,
        "corpus_paths": {
            "base_dir": path_for_fingerprint(&etl.opts.base_dir)?,
            "comments_dir": path_for_fingerprint(&etl.opts.comments_dir)?,
            "submissions_dir": path_for_fingerprint(&etl.opts.submissions_dir)?,
        },
        "corpus_files": corpus_file_fingerprints(files)?,
        "sources": format!("{:?}", etl.opts.sources),
        "start": opt_ym_string(etl.opts.start),
        "end": opt_ym_string(etl.opts.end),
        "legacy_subreddit": etl.opts.subreddit.as_ref(),
        "whitelist_fields": etl.opts.whitelist_fields.as_ref(),
        "strict_whitelist": etl.opts.strict_whitelist,
        "human_readable_timestamps": etl.opts.human_readable_timestamps,
        "zst_level": zst_level,
        "limit": limit,
        "query": {
            "subreddits": query.subreddits.as_ref(),
            "ids_in": query.ids_in.as_ref(),
            "comment_ids_in": query.comment_ids_in.as_ref(),
            "submission_ids_in": query.submission_ids_in.as_ref(),
            "authors_in": query.authors_in.as_ref(),
            "authors_out": query.authors_out.as_ref(),
            "exclude_common_bots": query.exclude_common_bots,
            "author_regex": query.author_regex.as_ref().map(|re| re.as_str()),
            "author_regex_pattern": query.author_regex_pattern.as_ref(),
            "min_score": query.min_score,
            "max_score": query.max_score,
            "timestamp_bounds": {
                "created_utc_gte": query.timestamp_bounds.created_utc_gte,
                "created_utc_lt": query.timestamp_bounds.created_utc_lt,
            },
            "keywords_any": query.keywords_any.as_ref(),
            "keywords_all": query.keywords_all.as_ref(),
            "keywords_exclude": query.keywords_exclude.as_ref(),
            "text_regex": query.text_regex.as_ref().map(|re| re.as_str()),
            "text_regex_pattern": query.text_regex_pattern.as_ref(),
            "domains_in": query.domains_in.as_ref(),
            "contains_url": query.contains_url,
            "no_url": query.no_url,
            "json_predicates": query.json_predicates_fingerprint(),
            "filter_pseudo_users": query.filter_pseudo_users,
        }
    });
    let bytes = serde_json::to_vec(&input).context("serialize resume fingerprint input")?;
    Ok(stable_fnv1a_hex(&bytes))
}
