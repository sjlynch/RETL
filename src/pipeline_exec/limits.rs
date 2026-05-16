
fn complete_stream_job(job: &FileJob, result: StreamJobResult) -> Result<u64> {
    if result.complete {
        Ok(result.written)
    } else {
        Err(PartialScanError {
            path: job.path.clone(),
            written: result.written,
        }
        .into())
    }
}

fn record_limit_from(limit: Option<u64>) -> Option<Arc<RecordLimit>> {
    limit.map(|n| Arc::new(RecordLimit::new(n)))
}

fn record_limit_from_with_claimed(limit: Option<u64>, claimed: u64) -> Option<Arc<RecordLimit>> {
    limit.map(|n| Arc::new(RecordLimit::new_with_claimed(n, claimed)))
}

fn committed_line_count(months: &HashMap<String, MonthEntry>) -> u64 {
    months.values().map(|entry| entry.lines).sum()
}

fn is_partial_scan_error(e: &anyhow::Error) -> bool {
    e.chain()
        .any(|cause| cause.downcast_ref::<PartialScanError>().is_some())
}

fn cleanup_scratch_dir(path: &Path, label: &str) {
    if let Err(e) = crate::util::remove_dir_all_with_short_backoff(path) {
        tracing::warn!(path=%path.display(), error=%e, %label, "failed to remove scratch dir");
    }
}
