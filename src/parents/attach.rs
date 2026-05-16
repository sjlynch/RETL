
fn attach_inprogress_exists(staging_dir: &Path, final_dest: &Path) -> Result<bool> {
    let file_name = final_dest
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "final_dest has no UTF-8 file name: {}",
                final_dest.display()
            )
        })?;
    if !staging_dir.exists() {
        return Ok(false);
    }
    for entry in fs::read_dir(staging_dir)
        .with_context(|| format!("read staging dir {}", staging_dir.display()))?
    {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.starts_with(file_name) && name.ends_with(INPROGRESS_EXT) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn attach_fingerprint_path(final_dest: &Path) -> PathBuf {
    let mut sidecar = final_dest.as_os_str().to_os_string();
    sidecar.push(ATTACH_SIDECAR_SUFFIX);
    PathBuf::from(sidecar)
}

fn is_owned_spool_part_name(name: &str) -> bool {
    let ym = name
        .strip_prefix("part_RC_")
        .or_else(|| name.strip_prefix("part_RS_"))
        .and_then(|rest| rest.strip_suffix(".jsonl"));
    ym.is_some_and(|ym| ym.parse::<YearMonth>().is_ok())
}

fn owned_attach_output_base_name(name: &str) -> Option<String> {
    if is_owned_spool_part_name(name) {
        return Some(name.to_string());
    }
    let base = name.strip_suffix(ATTACH_SIDECAR_SUFFIX)?;
    is_owned_spool_part_name(base).then(|| base.to_string())
}

fn attach_output_basenames(inputs: &[(usize, PathBuf)]) -> Result<BTreeSet<String>> {
    let mut names = BTreeSet::new();
    for (_idx, input) in inputs {
        let name = input.file_name().ok_or_else(|| {
            anyhow::anyhow!(
                "attach_parents input path has no file name: {}",
                input.display()
            )
        })?;
        names.insert(name.to_string_lossy().to_string());
    }
    Ok(names)
}

fn prune_stale_attach_outputs(out_dir: &Path, keep_basenames: &BTreeSet<String>) -> Result<()> {
    if !out_dir.exists() {
        return Ok(());
    }
    for entry in crate::util::read_dir_with_default_backoff(out_dir)
        .with_context(|| format!("read_dir {}", out_dir.display()))?
    {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(base_name) = owned_attach_output_base_name(name) else {
            continue;
        };
        if keep_basenames.contains(&base_name) {
            continue;
        }
        crate::util::remove_with_short_backoff(&path)
            .with_context(|| format!("remove stale parent attach output {}", path.display()))?;
    }
    Ok(())
}
