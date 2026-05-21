// Plain JSONL/spool-to-CSV/TSV conversion entry points.
// Included into `pipeline_exec` by `mod.rs`; no public module boundary.

/// zstd frame magic bytes (`28 B5 2F FD`). RETL's own corpus and
/// partitioned-zst exports are zstd-compressed, so a `.zst` file is a natural
/// thing to point `convert` at — but `convert` reads inputs strictly as plain
/// JSONL, and serde_json choking on raw compressed bytes only surfaces as a
/// confusing "malformed JSON ... line 1". Detect it up front instead.
const ZSTD_FRAME_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Reject a zstd-compressed `convert` input with a clear capability message.
///
/// Checks the `.zst` extension first (cheap) and then peeks the buffered
/// reader for the zstd frame magic, so extensionless or mislabelled `.zst`
/// files are caught too. Peeking via `fill_buf` does not consume input — the
/// JSONL read path sees the same bytes if the file is in fact plain text.
fn reject_zst_convert_input(
    path: &Path,
    reader: &mut BufReader<std::fs::File>,
) -> Result<()> {
    use std::io::BufRead;
    let looks_zst = path
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("zst"))
        || reader
            .fill_buf()
            .map(|buf| buf.starts_with(&ZSTD_FRAME_MAGIC))
            .unwrap_or(false);
    if looks_zst {
        anyhow::bail!(
            "convert accepts only plain JSONL, but {} is zstd-compressed; \
             decompress the .zst first, or use `retl export --format csv`",
            path.display()
        );
    }
    Ok(())
}

/// Convert existing plain JSONL files (including RETL spool/parent-enriched
/// parts) into a single CSV file using top-level, dotted, or JSON Pointer
/// field selectors. Missing fields render as empty cells.
///
/// Inputs must be plain (uncompressed) JSONL. A `.zst` input — including
/// RETL's own corpus and partitioned-zst exports — is rejected with a clear
/// error; decompress it first or use `retl export --format csv`.
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
///
/// Inputs must be plain (uncompressed) JSONL. A `.zst` input — including
/// RETL's own corpus and partitioned-zst exports — is rejected with a clear
/// error; decompress it first or use `retl export --format tsv`.
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
            reject_zst_convert_input(path, &mut reader)?;
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
