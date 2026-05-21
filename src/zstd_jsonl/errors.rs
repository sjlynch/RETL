
#[inline]
fn warn_decode_skip(path: &Path, e: &anyhow::Error) {
    // Try to print an absolute, canonical path to avoid truncation/ambiguity.
    let abs = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    // Emit a multi-line message to stderr (separate from progress bars) and to tracing.
    let err_chain = format!("{e:#}");
    let msg = format!(
        "Zstd decode error while streaming file\n  path : {}\n  error: {}\n\
         note : This usually indicates file corruption (often late/trailing). \
                Quick integrity sampling may miss trailing corruption. \
                Consider running a Full integrity check or re-downloading this month. \
                Tolerant callers skip this file; strict callers surface it as fatal.",
        abs.display(),
        err_chain
    );
    eprintln!("{}", msg);
    tracing::warn!("{}", msg);
}

// ----------------------------- Parsing ------------------------------------

/// Parse a JSON line into `MinimalRecord` using serde_json.
#[inline]
pub fn parse_minimal(line: &str) -> Result<MinimalRecord> {
    Ok(serde_json::from_str(line)?)
}

/// Build the standardized fatal error for malformed JSONL records.
///
/// Policy: valid zstd frames that contain syntactically invalid JSONL are not
/// treated as corrupt-frame skips. Scan/export/dedupe callers abort the file
/// and surface the path plus 1-based line number so resumable outputs are not
/// marked complete with partial data.
pub fn malformed_json_error(
    path: &Path,
    line_number: u64,
    source: impl std::fmt::Display,
) -> anyhow::Error {
    anyhow!(
        "malformed JSON in {} at line {}: {}",
        path.display(),
        line_number,
        source
    )
}

pub fn zstd_decode_error(path: &Path, source: anyhow::Error) -> anyhow::Error {
    // Do not interpolate the source message into the context: `source` stays
    // in the error chain, so `{:#}` rendering already appends it once. Adding
    // it here would render the underlying zstd message twice.
    source.context(format!(
        "zstd decode error while streaming {}",
        path.display()
    ))
}

// ----------------------------- Streaming ----------------------------------
