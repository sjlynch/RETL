// Plain JSONL/spool-to-CSV/TSV conversion entry points.
// Included into `pipeline_exec` by `mod.rs`; no public module boundary.

/// Convert existing plain JSONL files (including RETL spool/parent-enriched
/// parts) into a single CSV file using top-level, dotted, or JSON Pointer
/// field selectors. Missing fields render as empty cells.
pub fn convert_jsonl_to_csv<I, P, J, S>(
    inputs: I,
    out_path: &Path,
    fields: J,
    opts: TabularExportOptions,
) -> Result<u64>
where
    I: IntoIterator<Item = P>,
    P: AsRef<Path>,
    J: IntoIterator<Item = S>,
    S: Into<String>,
{
    convert_jsonl_to_tabular(inputs, out_path, fields, opts, TabularFormat::Csv)
}

/// Convert existing plain JSONL files (including RETL spool/parent-enriched
/// parts) into a single TSV file using top-level, dotted, or JSON Pointer
/// field selectors. Values containing tabs or line breaks are rejected; use
/// CSV for arbitrary Reddit text fields.
pub fn convert_jsonl_to_tsv<I, P, J, S>(
    inputs: I,
    out_path: &Path,
    fields: J,
    opts: TabularExportOptions,
) -> Result<u64>
where
    I: IntoIterator<Item = P>,
    P: AsRef<Path>,
    J: IntoIterator<Item = S>,
    S: Into<String>,
{
    convert_jsonl_to_tabular(inputs, out_path, fields, opts, TabularFormat::Tsv)
}

fn convert_jsonl_to_tabular<I, P, J, S>(
    inputs: I,
    out_path: &Path,
    fields: J,
    opts: TabularExportOptions,
    format: TabularFormat,
) -> Result<u64>
where
    I: IntoIterator<Item = P>,
    P: AsRef<Path>,
    J: IntoIterator<Item = S>,
    S: Into<String>,
{
    let input_paths: Vec<PathBuf> = inputs
        .into_iter()
        .map(|path| path.as_ref().to_path_buf())
        .collect();
    if input_paths.is_empty() {
        anyhow::bail!("convert requires at least one JSONL input file");
    }
    let fields = normalize_tabular_fields(fields)?;
    let selectors = parse_tabular_field_selectors(&fields)?;
    write_at_path_atomic(out_path, 64 * 1024, |out| {
        let mut written = 0_u64;
        if opts.header {
            write_tabular_header(out, &fields, format)?;
        }
        for path in &input_paths {
            let file = crate::util::open_with_default_backoff(path)
                .with_context(|| format!("opening JSONL input {}", path.display()))?;
            let mut reader = BufReader::with_capacity(256 * 1024, file);
            let mut line = String::with_capacity(16 * 1024);
            let mut line_number = 0_u64;
            loop {
                let n = crate::ndjson::read_line_capped(
                    &mut reader,
                    &mut line,
                    crate::ndjson::DEFAULT_MAX_LINE_BYTES,
                    path,
                )
                .with_context(|| {
                    format!(
                        "reading JSONL input {} near line {}",
                        path.display(),
                        line_number + 1
                    )
                })?;
                if n == 0 {
                    break;
                }
                line_number += 1;
                if line.trim().is_empty() {
                    continue;
                }
                let value: Value = serde_json::from_str(&line).with_context(|| {
                    format!("malformed JSON in {} line {}", path.display(), line_number)
                })?;
                let (cells, _matched_indices) = tabular_cells_from_value(&value, &selectors)?;
                write_tabular_row(out, &fields, &cells, format).with_context(|| {
                    format!(
                        "writing {} row for {} line {}",
                        format.label(),
                        path.display(),
                        line_number
                    )
                })?;
                written += 1;
            }
        }
        Ok(written)
    })
}
