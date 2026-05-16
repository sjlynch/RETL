
fn split_record_kind_prefix(s: &str) -> (Option<RecordIdKind>, &str) {
    let bytes = s.as_bytes();
    if bytes.len() >= 3 && bytes[2] == b'_' && bytes[0].eq_ignore_ascii_case(&b't') {
        match bytes[1] {
            b'1' => return (Some(RecordIdKind::Comment), &s[3..]),
            b'3' => return (Some(RecordIdKind::Submission), &s[3..]),
            _ => {}
        }
    }
    (None, s)
}

fn lowercase_ascii_if_needed(s: &str) -> Cow<'_, str> {
    if s.bytes().any(|b| b.is_ascii_uppercase()) {
        Cow::Owned(s.to_lowercase())
    } else {
        Cow::Borrowed(s)
    }
}

/// Normalize a user-provided Reddit record ID selector.
///
/// Inputs are trimmed, `t1_` / `t3_` fullname prefixes are stripped and kept as
/// kind constraints, and bare IDs are lowercased for Reddit's base36 IDs. Other
/// prefixes (for example `t5_`) are treated as part of the bare ID so callers do
/// not accidentally strip unsupported fullname kinds.
pub(crate) fn normalize_record_id_selector(raw: &str) -> NormalizedRecordId {
    let trimmed = raw.trim();
    let (kind, bare) = split_record_kind_prefix(trimmed);
    let bare = bare.trim();
    NormalizedRecordId {
        bare: lowercase_ascii_if_needed(bare).into_owned(),
        kind,
    }
}

fn normalize_record_id_value(raw: &str) -> Cow<'_, str> {
    let trimmed = raw.trim();
    let (_kind, bare) = split_record_kind_prefix(trimmed);
    lowercase_ascii_if_needed(bare.trim())
}

/// Read a newline-delimited record-ID file.
///
/// Each non-empty, non-comment line is one ID selector. Leading/trailing
/// whitespace is trimmed; blank lines and lines whose first non-whitespace
/// character is `#` are ignored. Inline comments are not stripped, so put one
/// bare ID (or `t1_` / `t3_` fullname) per line.
pub fn read_record_ids_file(path: impl AsRef<Path>) -> anyhow::Result<Vec<String>> {
    let path = path.as_ref();
    let file = crate::util::open_with_default_backoff(path)
        .with_context(|| format!("open IDs file {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    let mut ids = Vec::new();
    let mut line_number = 0_u64;

    loop {
        let next_line = line_number + 1;
        let read = crate::ndjson::read_line_capped(
            &mut reader,
            &mut line,
            crate::ndjson::DEFAULT_MAX_LINE_BYTES,
            path,
        )
        .with_context(|| format!("read IDs file {} line {}", path.display(), next_line))?;
        if read == 0 {
            break;
        }
        line_number = next_line;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        ids.push(trimmed.to_string());
    }

    Ok(ids)
}
