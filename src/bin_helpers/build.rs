
pub(crate) fn ensure_dirs(common: &CommonOpts) -> Result<PathBuf> {
    retl::create_dir_all_with_default_backoff(&common.work_dir)
        .with_context(|| format!("creating work_dir {}", common.work_dir.display()))?;
    let lib_tmp = common.work_dir.join("lib_tmp");
    retl::create_dir_all_with_default_backoff(&lib_tmp)
        .with_context(|| format!("creating work_dir {}", lib_tmp.display()))?;
    Ok(lib_tmp)
}
