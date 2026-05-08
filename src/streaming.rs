//! Streaming primitives: the one-pass record filter/writer (`stream_job`) with progress,
//! and the usernames collector for a single monthly file.

use crate::filters::{matches_minimal, matches_subreddit_basic, within_bounds};
use crate::json_whitelist::WhitelistTokenizer;
use crate::paths::FileJob;
use crate::query::QuerySpec;
use crate::shard::ShardedWriter;
use crate::zstd_jsonl::{
    for_each_line_cfg, for_each_line_cfg_with_skip, for_each_line_with_progress_cfg,
    for_each_line_with_progress_cfg_with_skip, parse_minimal,
};
use anyhow::Result;
use indicatif::ProgressBar;
use serde_json::{Map, Value};
use std::io::Write;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

fn apply_human_timestamp_in_place(map: &mut Map<String, Value>, key: &str) {
    if let Some(v) = map.get_mut(key) {
        if let Some(n) = v.as_i64() {
            if let Ok(dt) = OffsetDateTime::from_unix_timestamp(n) {
                if let Ok(s) = dt.format(&Rfc3339) {
                    *v = Value::String(s);
                }
            }
        }
    }
}

#[doc(hidden)]
pub fn apply_human_timestamps(val: &mut Value) {
    if let Some(obj) = val.as_object_mut() {
        // Convert common timestamp fields if they are numeric.
        apply_human_timestamp_in_place(obj, "created_utc");
        apply_human_timestamp_in_place(obj, "retrieved_on");
        // "edited" can be bool or number. Only convert numeric forms.
        apply_human_timestamp_in_place(obj, "edited");
    }
}

/// Byte-level rewrite of the three timestamp fields directly from the raw JSONL line
/// into `buf`, without going through `serde_json::Value`.
///
/// Looks for the literal byte patterns `"created_utc":`, `"retrieved_on":`, `"edited":`
/// followed by an optional space and an integer, and replaces the integer with an
/// RFC3339 string. Non-integer values (`true`/`false`/`null`/floats) are left untouched.
///
/// Safety on substring matching: the JSON spec requires `"` inside string values to be
/// escaped, so the literal byte sequence `"<key>":` cannot appear inside a string value.
/// That makes a flat byte search safe for the keys we care about.
#[doc(hidden)]
pub fn rewrite_human_timestamps_bytes(line: &str, buf: &mut String) {
    buf.clear();
    buf.reserve(line.len() + 64);
    let bytes = line.as_bytes();
    let len = bytes.len();
    let patterns: &[&[u8]] = &[
        b"\"created_utc\":",
        b"\"retrieved_on\":",
        b"\"edited\":",
    ];

    let mut last = 0usize;
    let mut i = 0usize;
    while i < len {
        if bytes[i] != b'"' {
            i += 1;
            continue;
        }

        let mut matched_len = 0usize;
        for p in patterns {
            if i + p.len() <= len && &bytes[i..i + p.len()] == *p {
                matched_len = p.len();
                break;
            }
        }
        if matched_len == 0 {
            i += 1;
            continue;
        }

        let value_start = i + matched_len;
        // Skip optional whitespace (compact serde JSON has none, but be tolerant).
        let mut j = value_start;
        while j < len && (bytes[j] == b' ' || bytes[j] == b'\t') {
            j += 1;
        }

        let num_start = j;
        if j < len && bytes[j] == b'-' {
            j += 1;
        }
        let digits_start = j;
        while j < len && bytes[j].is_ascii_digit() {
            j += 1;
        }

        // Not an integer (e.g., true/false/null, or just "edited":false).
        if j == digits_start {
            i = value_start;
            continue;
        }
        // Reject fractional / exponent forms — `as_i64` would have rejected them too.
        if j < len {
            let c = bytes[j];
            if c == b'.' || c == b'e' || c == b'E' {
                i = value_start;
                continue;
            }
        }

        let parsed: Option<i64> = std::str::from_utf8(&bytes[num_start..j])
            .ok()
            .and_then(|s| s.parse().ok());
        let Some(n) = parsed else {
            i = value_start;
            continue;
        };

        let formatted = match OffsetDateTime::from_unix_timestamp(n)
            .ok()
            .and_then(|dt| dt.format(&Rfc3339).ok())
        {
            Some(s) => s,
            None => {
                i = value_start;
                continue;
            }
        };

        // Emit everything up to (and including) the `":` of this key, then the quoted
        // RFC3339 timestamp, then jump past the original digits. RFC3339 output
        // contains only characters that are JSON-safe without escaping.
        buf.push_str(&line[last..value_start]);
        buf.push('"');
        buf.push_str(&formatted);
        buf.push('"');
        last = j;
        i = j;
    }

    if last < len {
        buf.push_str(&line[last..]);
    }
}

pub fn stream_job<W: Write + ?Sized>(
    job: &FileJob,
    writer: &mut W,
    targets: Option<&Vec<String>>,
    query: &QuerySpec,
    whitelist: &Option<Vec<String>>,
    pb: Option<ProgressBar>,
    bounds: Option<(crate::date::YearMonth, crate::date::YearMonth)>,
    read_buf_bytes: usize,
    human_timestamps: bool,
) -> Result<u64> {
    let mut written: u64 = 0;
    let mut ts_buf = String::new();
    let mut tok_buf = String::new();

    // Build the streaming tokenizer once per file so the small key-set is
    // hashed exactly once and the buffers above are reused across every line.
    let tokenizer: Option<WhitelistTokenizer> = whitelist
        .as_ref()
        .map(|fields| WhitelistTokenizer::new(fields.iter().map(|s| s.as_str())));

    let mut on_line = |line: &str| -> Result<()> {
        if let Ok(min) = parse_minimal(line) {
            if !matches_minimal(&min, targets, query) { return Ok(()); }
            if !within_bounds(&min, bounds) { return Ok(()); }

            // Fast-path: raw line when no transforms needed.
            if whitelist.is_none() && !human_timestamps {
                writer.write_all(line.as_bytes())?;
                writer.write_all(b"\n")?;
                written += 1;
                return Ok(());
            }

            // Byte-level fast-path: human timestamps only, no whitelist.
            // Rewrites `"created_utc"|"retrieved_on"|"edited":<int>` in place without
            // a `serde_json::Value` round-trip. This is the common export/spool config.
            if whitelist.is_none() && human_timestamps {
                rewrite_human_timestamps_bytes(line, &mut ts_buf);
                writer.write_all(ts_buf.as_bytes())?;
                writer.write_all(b"\n")?;
                written += 1;
                return Ok(());
            }

            // Whitelist branch — preferred path is the streaming tokenizer
            // which copies raw value bytes verbatim and never builds a
            // `serde_json::Value`. If the tokenizer rejects the line as
            // structurally surprising, fall back to the slow Value path so we
            // never regress correctness on an odd record.
            if let Some(tok) = &tokenizer {
                let tok_result = if human_timestamps {
                    // Fused single-pass: project whitelisted keys AND rewrite
                    // the three timestamp keys' integer values to RFC3339 in
                    // one walk over the raw line bytes. Replaces the older
                    // tokenize_into → rewrite_human_timestamps_bytes chain.
                    tok.tokenize_and_rewrite_timestamps_into(line, &mut tok_buf)
                } else {
                    tok.tokenize_into(line, &mut tok_buf)
                };
                if tok_result.is_ok() {
                    writer.write_all(tok_buf.as_bytes())?;
                    writer.write_all(b"\n")?;
                    written += 1;
                    return Ok(());
                }
                // Fall through to slow path on tokenizer error.
            }

            // Slow path: parse Value for whitelisting (and timestamp conversion if both apply).
            let val: Value = serde_json::from_str(line)?;
            let mut out_val = if let Some(fields) = whitelist {
                let mut obj = Map::new();
                if let Some(map) = val.as_object() {
                    for k in fields {
                        if let Some(v) = map.get(k) { obj.insert(k.clone(), v.clone()); }
                    }
                }
                Value::Object(obj)
            } else {
                val
            };

            if human_timestamps {
                apply_human_timestamps(&mut out_val);
            }

            serde_json::to_writer(&mut *writer, &out_val)?;
            writer.write_all(b"\n")?;
            written += 1;
        }
        Ok(())
    };

    if let Some(pb) = pb {
        for_each_line_with_progress_cfg(&job.path, read_buf_bytes, |delta| pb.inc(delta), |s| on_line(s))?;
    } else {
        for_each_line_cfg(&job.path, read_buf_bytes, |s| on_line(s))?;
    }

    Ok(written)
}

/// Process a single monthly file and shard usernames matching `subreddit`.
///
/// On decode error, the file is logged via `warn_decode_skip` and skipped (the
/// outer `Result` stays `Ok`). The optional `on_skip(path, &error)` callback
/// fires once per skipped file so callers can count, alert, or fail-fast — it
/// does not change the swallow-by-default behavior; corrupt files never abort
/// the job.
pub fn process_file_for_usernames_with_skip(
    job: &FileJob,
    read_buf_bytes: usize,
    subreddit: &str,
    shard_writer: &ShardedWriter,
    pb: Option<ProgressBar>,
    mut on_skip: impl FnMut(&std::path::Path, &anyhow::Error),
) -> Result<()> {
    let handle_line = |line: &str| -> Result<()> {
        let min = match parse_minimal(line) { Ok(m) => m, Err(_) => return Ok(()) };
        if !matches_subreddit_basic(&min, subreddit) { return Ok(()); }
        if let Some(author) = min.author.as_deref() {
            let a = author.trim();
            if a.is_empty() || a == "[deleted]" || a == "[removed]" { return Ok(()); }
            shard_writer.write(a)?;
        }
        Ok(())
    };

    if let Some(pb) = pb {
        for_each_line_with_progress_cfg_with_skip(
            &job.path,
            read_buf_bytes,
            |p, e| on_skip(p, e),
            |delta| pb.inc(delta),
            |s| handle_line(s),
        )?;
    } else {
        for_each_line_cfg_with_skip(
            &job.path,
            read_buf_bytes,
            |p, e| on_skip(p, e),
            |s| handle_line(s),
        )?;
    }
    Ok(())
}
