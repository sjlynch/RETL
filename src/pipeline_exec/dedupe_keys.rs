
fn dedupe_key_label(key: &KeyExtractor) -> String {
    match key {
        KeyExtractor::JsonPointer(ptr) => format!("json:{ptr}"),
        KeyExtractor::AuthorLowerFast => "author".to_string(),
        KeyExtractor::SubredditLowerFast => "subreddit".to_string(),
        KeyExtractor::ByValue(_) => "<custom>".to_string(),
    }
}

fn warn_dedupe_key_drops(key: &KeyExtractor, summary: &DedupeKeySummary) {
    if summary.matched_records == 0 || summary.key_extractions_failed == 0 {
        return;
    }
    let drop_rate = summary.key_drop_rate();
    if drop_rate > DEDUPE_KEY_DROP_WARN_RATE {
        let key_spec = dedupe_key_label(key);
        tracing::warn!(
            key = %key_spec,
            matched_records = summary.matched_records,
            key_extractions_failed = summary.key_extractions_failed,
            drop_rate,
            "dedupe dropped {} matching record(s) without an extractable key ({:.2}% of matches); use --strict-key to fail instead",
            summary.key_extractions_failed,
            drop_rate * 100.0,
        );
    }
}

fn dedupe_cfg_from_options(opts: &ETLOptions) -> DedupeCfg {
    crate::config::warn_if_inflight_pair_pathological(opts.inflight_bytes, opts.inflight_groups);
    let mut cfg = DedupeCfg::from(opts);
    if cfg.inflight_bytes > 0 {
        let per_flush_mb = (cfg.inflight_bytes / 2 / (1024 * 1024)).max(1);
        cfg.min_buf_mb = cfg.min_buf_mb.min(per_flush_mb);
        cfg.max_buf_mb = cfg.max_buf_mb.min(per_flush_mb.max(cfg.min_buf_mb));
    }
    cfg
}

fn warn_dedupe_zero_keys(key: &KeyExtractor, input_records: u64) {
    match key {
        KeyExtractor::JsonPointer(ptr) => {
            let key_spec = format!("json:{ptr}");
            tracing::warn!(
                key = %key_spec,
                input_records,
                "--key {key_spec} matched nothing; check the pointer; the value may not be a scalar"
            );
        }
        KeyExtractor::AuthorLowerFast => tracing::warn!(
            key = "author",
            input_records,
            "--key author extracted zero keys from matching input records"
        ),
        KeyExtractor::SubredditLowerFast => tracing::warn!(
            key = "subreddit",
            input_records,
            "--key subreddit extracted zero keys from matching input records"
        ),
        KeyExtractor::ByValue(_) => tracing::warn!(
            key = "<custom>",
            input_records,
            "custom dedupe key extractor extracted zero keys from matching input records"
        ),
    }
}

// -------- Original operations (back-compat) --------
