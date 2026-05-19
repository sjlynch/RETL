// Temporary part discovery and stitching for tabular (CSV/TSV) exports.
// Included into `pipeline_exec` by `mod.rs`; no public module boundary.

fn tabular_part_path(tmp_dir: &Path, key: &str, format: TabularFormat) -> PathBuf {
    tmp_dir.join(format!(".part_{}{}", key, format.row_suffix()))
}

fn tabular_part_paths(tmp_dir: &Path, format: TabularFormat) -> Result<Vec<PathBuf>> {
    let suffix = format.row_suffix();
    let mut paths = Vec::new();
    for entry in crate::util::read_dir_with_default_backoff(tmp_dir)
        .with_context(|| format!("read_dir {}", tmp_dir.display()))?
    {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if name.starts_with(".part_") && name.ends_with(suffix) {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

fn stitch_tabular_parts(
    tmp_dir: &Path,
    out_path: &Path,
    fields: &[String],
    opts: TabularExportOptions,
    format: TabularFormat,
    write_buf: usize,
) -> Result<()> {
    let parts = tabular_part_paths(tmp_dir, format)?;
    write_at_path_atomic(out_path, write_buf, |out| {
        if opts.header {
            write_tabular_header(out, fields, format)?;
        }
        for path in &parts {
            let mut r = BufReader::new(crate::util::open_with_default_backoff(path)?);
            std::io::copy(&mut r, out)?;
        }
        Ok(())
    })
}
