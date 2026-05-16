
pub fn file_identities(paths: &[PathBuf]) -> Vec<FileIdentity> {
    let mut out: Vec<FileIdentity> = paths
        .iter()
        .map(|path| file_identity_with_context(path, None, None))
        .collect();
    out.sort_by(|a, b| a.path.cmp(&b.path));
    out
}

pub fn file_identity(path: &Path) -> FileIdentity {
    file_identity_with_context(path, None, None)
}

pub(crate) fn scan_query_value(query: &QuerySpec, limit: Option<u64>) -> Value {
    json!({
        "subreddits": query.subreddits.as_ref(),
        "authors_in": query.authors_in.as_ref(),
        "authors_out": query.authors_out.as_ref(),
        "exclude_common_bots": query.exclude_common_bots,
        "author_regex": query.author_regex.as_ref().map(|re| re.as_str()),
        "author_regex_pattern": query.author_regex_pattern.as_ref(),
        "min_score": query.min_score,
        "max_score": query.max_score,
        "keywords_any": query.keywords_any.as_ref(),
        "domains_in": query.domains_in.as_ref(),
        "contains_url": query.contains_url,
        "json_predicates": query.json_predicates_fingerprint(),
        "filter_pseudo_users": query.filter_pseudo_users,
        "limit": limit,
    })
}

pub(crate) fn etl_options_value(etl: &ETLOptions, limit: Option<u64>, extra: Value) -> Value {
    json!({
        "sources": sources_label(etl.sources),
        "start": etl.opts_start_string(),
        "end": etl.opts_end_string(),
        "legacy_subreddit": etl.subreddit.as_ref(),
        "whitelist_fields": etl.whitelist_fields.as_ref(),
        "strict_whitelist": etl.strict_whitelist,
        "strict_key": etl.strict_key,
        "parallelism": etl.parallelism,
        "file_concurrency": etl.file_concurrency,
        "shard_count": etl.shard_count,
        "work_dir": etl.work_dir.as_ref().map(|p| path_to_stable_string(p)),
        "read_buffer_bytes": etl.read_buffer_bytes,
        "write_buffer_bytes": etl.write_buffer_bytes,
        "human_readable_timestamps": etl.human_readable_timestamps,
        "zst_level": etl.zst_level,
        "inflight_bytes": etl.inflight_bytes,
        "inflight_groups": etl.inflight_groups,
        "resume": etl.resume,
        "allow_partial": etl.allow_partial,
        "emit_manifest": etl.emit_manifest,
        "parent_payload": {
            "full_record": etl.parent_payload_spec.is_full_record(),
            "fields": etl.parent_payload_spec.fields(),
        },
        "limit": limit,
        "extra": extra,
    })
}

pub fn path_to_stable_string(path: &Path) -> String {
    stable_path(path).to_string_lossy().into_owned()
}
