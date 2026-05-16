
pub(crate) fn build_etl(common: &CommonOpts) -> Result<RedditETL> {
    if let (Some(start), Some(end)) = (common.start, common.end) {
        if start > end {
            return Err(ConfigBuildError::InvalidDateRange { start, end }.into());
        }
    }
    let lib_tmp = ensure_dirs(common)?;
    let mut etl = RedditETL::new()
        .base_dir(&common.data_dir)
        .work_dir(&lib_tmp)
        .progress(!common.no_progress)
        .run_manifest(!common.no_manifest)
        .sources(Sources::from(common.source))
        .date_range(common.start, common.end)
        .allow_partial(common.allow_partial);

    if let Some(p) = common.parallelism {
        etl = etl.parallelism(p);
    }
    if let Some(fc) = common.file_concurrency {
        etl = etl.file_concurrency(fc);
    }
    Ok(etl)
}

pub(crate) fn emit_partial_read_report(reporter: &PartialReadReporter) -> Result<()> {
    let report = reporter.snapshot();
    if report.skipped_file_count == 0 {
        return Ok(());
    }
    eprintln!("{}", serde_json::to_string(&report)?);
    Ok(())
}
