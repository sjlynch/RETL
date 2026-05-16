
fn export_part_path(tmp_dir: &Path, key: &str) -> PathBuf {
    tmp_dir.join(format!(".part_{}.jsonl", key))
}

fn load_and_validate_export_manifest(
    tmp_dir: &Path,
    fingerprint: &str,
) -> Result<(HashMap<String, MonthEntry>, HashSet<String>)> {
    let manifest = crate::progress_manifest::load(tmp_dir);
    if manifest.fingerprint.as_deref() != Some(fingerprint) {
        tracing::warn!(
            path=%crate::progress_manifest::manifest_path(tmp_dir).display(),
            stored=?manifest.fingerprint,
            current=%fingerprint,
            "extract resume manifest fingerprint does not match current query/config; discarding cached parts"
        );
        clear_extract_resume_parts(tmp_dir)?;
        return Ok((HashMap::new(), HashSet::new()));
    }
    let mut keep: HashMap<String, MonthEntry> = HashMap::new();
    for (k, v) in manifest.months {
        let part_path = export_part_path(tmp_dir, &k);
        match validate_jsonl_part(&part_path) {
            Ok(entry) if entry.size == v.size => {
                keep.insert(k, entry);
            }
            _ => {
                tracing::info!(key=%k, "dropping stale extract progress entry; month will be re-run")
            }
        }
    }
    let completed_keys: HashSet<String> = keep.keys().cloned().collect();
    Ok((keep, completed_keys))
}

fn validate_jsonl_part(path: &Path) -> Result<MonthEntry> {
    let file = crate::util::open_with_default_backoff(path)
        .with_context(|| format!("opening extract resume part {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut buf = String::new();
    let mut lines = 0_u64;
    loop {
        let n = crate::ndjson::read_line_capped(
            &mut reader,
            &mut buf,
            crate::ndjson::DEFAULT_MAX_LINE_BYTES,
            path,
        )?;
        if n == 0 {
            break;
        }
        if buf.is_empty() {
            continue;
        }
        let _: serde_json::Value = serde_json::from_str(&buf)
            .with_context(|| format!("validating JSONL part {}", path.display()))?;
        lines += 1;
    }
    let size = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    Ok(MonthEntry {
        size,
        lines,
        sha256: None,
    })
}

fn count_text_lines(path: &Path) -> Result<u64> {
    let file = crate::util::open_with_default_backoff(path)
        .with_context(|| format!("opening output to count lines {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut buf = String::new();
    let mut lines = 0_u64;
    loop {
        let n = crate::ndjson::read_line_capped(
            &mut reader,
            &mut buf,
            crate::ndjson::DEFAULT_MAX_LINE_BYTES,
            path,
        )?;
        if n == 0 {
            break;
        }
        if !buf.is_empty() {
            lines += 1;
        }
    }
    Ok(lines)
}
