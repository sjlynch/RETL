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
use anyhow::{anyhow, Result};
use indicatif::ProgressBar;
use serde_json::{Map, Value};
use std::io::{self, Write};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

/// The three JSON keys the byte-level rewriter targets, with their `":` suffix
/// pre-baked so the search can flat-scan the line for an anchored byte sequence.
const TIMESTAMP_KEY_PATTERNS: &[&[u8]] = &[
    b"\"created_utc\":",
    b"\"retrieved_on\":",
    b"\"edited\":",
];

/// Number of accepted records sampled before warning/erroring that a whitelist
/// did not match any top-level keys. Large enough to avoid overreacting to a
/// few schema variants, small enough to catch typos near the start of a run.
pub(crate) const WHITELIST_ZERO_MATCH_SAMPLE: u64 = 100;

const WHITELIST_ZERO_MATCH_HINT: &str = "check field names. Comments use `body`/`parent_id`/`link_id`; submissions use `title`/`selftext`/`domain`.";

#[derive(Debug, Default)]
struct WhitelistMatchState {
    seen: u64,
    matched_any: bool,
    reported: bool,
}

/// Shared per-export whitelist sanity checker. It samples accepted records
/// after filtering and reports once if every sampled record projected to `{}`.
#[derive(Debug)]
pub(crate) struct WhitelistMatchTracker {
    strict: bool,
    state: std::sync::Mutex<WhitelistMatchState>,
}

impl WhitelistMatchTracker {
    pub(crate) fn new(strict: bool) -> Self {
        Self { strict, state: std::sync::Mutex::new(WhitelistMatchState::default()) }
    }

    pub(crate) fn observe(&self, dropped_all_whitelisted_keys: bool) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("whitelist validation state lock poisoned"))?;

        if state.seen < WHITELIST_ZERO_MATCH_SAMPLE {
            state.seen += 1;
            if !dropped_all_whitelisted_keys {
                state.matched_any = true;
            }
        }

        if state.seen == WHITELIST_ZERO_MATCH_SAMPLE && !state.matched_any && !state.reported {
            state.reported = true;
            let msg = format!(
                "--whitelist matched zero fields on the first {} records; {}",
                WHITELIST_ZERO_MATCH_SAMPLE, WHITELIST_ZERO_MATCH_HINT
            );
            if self.strict {
                return Err(anyhow!(msg));
            }
            tracing::warn!("{}", msg);
        }

        Ok(())
    }
}

/// Append `buf` followed by a newline to `writer` and bump the running record count.
#[inline]
fn write_and_count<W: Write + ?Sized>(
    writer: &mut W,
    buf: &[u8],
    written: &mut u64,
) -> io::Result<()> {
    writer.write_all(buf)?;
    writer.write_all(b"\n")?;
    *written += 1;
    Ok(())
}

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

/// Find the next occurrence of one of `"created_utc":`, `"retrieved_on":`,
/// `"edited":` in `bytes[start..]` whose value is an integer literal, and
/// return `(value_start, value_end)`:
///
/// - `value_start` is the byte position immediately after the key's `":` —
///   so `bytes[..value_start]` preserves the key and colon verbatim. Any
///   whitespace and optional `-` sign between the colon and the digits sit
///   inside `[value_start..value_end]` and get replaced on rewrite.
/// - `value_end` is one past the last digit.
///
/// Keys whose value is not an integer literal (`null`, `false`, a float in
/// `1.5` / `1e3` form) are skipped and the search continues. Returns `None`
/// when no remaining match exists.
fn find_timestamp_field(bytes: &[u8], start: usize) -> Option<(usize, usize)> {
    let len = bytes.len();
    let mut i = start;
    while i < len {
        if bytes[i] != b'"' {
            i += 1;
            continue;
        }
        let matched_len = TIMESTAMP_KEY_PATTERNS
            .iter()
            .find(|p| i + p.len() <= len && &bytes[i..i + p.len()] == **p)
            .map(|p| p.len())
            .unwrap_or(0);
        if matched_len == 0 {
            i += 1;
            continue;
        }
        let value_start = i + matched_len;

        // Walk past optional whitespace (compact serde never emits any), an
        // optional `-`, then the digit run. Mirrors what `as_i64` accepts.
        let mut j = value_start;
        while j < len && (bytes[j] == b' ' || bytes[j] == b'\t') {
            j += 1;
        }
        if j < len && bytes[j] == b'-' {
            j += 1;
        }
        let digits_start = j;
        while j < len && bytes[j].is_ascii_digit() {
            j += 1;
        }

        // No digits → not an integer (e.g. `false`, `null`).
        // Trailing `.`/`e`/`E` → float; `as_i64` would reject too.
        let is_integer = j > digits_start
            && !matches!(bytes.get(j), Some(b'.') | Some(b'e') | Some(b'E'));
        if !is_integer {
            i = value_start;
            continue;
        }
        return Some((value_start, j));
    }
    None
}

/// Parse a Unix-timestamp `i64` from a slice produced by `find_timestamp_field`.
/// The slice may begin with optional space/tab whitespace and an optional `-`
/// sign followed by ASCII digits. Returns `None` only on i64 overflow — the
/// slice is otherwise guaranteed well-formed by the caller.
///
/// (Spec'd as `u64` in the original task brief, but `i64` is required to keep
/// the negative-epoch test in `tests/human_timestamps_edge_cases.rs` passing.)
fn parse_unix_digits(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok().and_then(|s| {
        s.trim_start_matches(|c: char| c == ' ' || c == '\t')
            .parse::<i64>()
            .ok()
    })
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
    let mut last = 0usize;
    let mut i = 0usize;

    while let Some((value_start, value_end)) = find_timestamp_field(bytes, i) {
        // RFC3339 output contains only characters that are JSON-safe without escaping.
        let formatted = parse_unix_digits(&bytes[value_start..value_end])
            .and_then(|n| OffsetDateTime::from_unix_timestamp(n).ok())
            .and_then(|dt| dt.format(&Rfc3339).ok());
        if let Some(s) = formatted {
            buf.push_str(&line[last..value_start]);
            buf.push('"');
            buf.push_str(&s);
            buf.push('"');
            last = value_end;
        }
        // Advance past the integer either way; nothing inside a digit run can
        // start a new `"<key>":` match, so this is byte-equivalent to the
        // original `i = value_start` on parse/format failure.
        i = value_end;
    }

    if last < bytes.len() {
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
    whitelist_tracker: Option<&WhitelistMatchTracker>,
) -> Result<u64> {
    let mut written: u64 = 0;
    let mut ts_buf = String::new();
    let mut tok_buf = String::new();
    let mut whitelist_error: Option<anyhow::Error> = None;

    // Build the streaming tokenizer once per file so the small key-set is
    // hashed exactly once and the buffers above are reused across every line.
    let tokenizer: Option<WhitelistTokenizer> = whitelist
        .as_ref()
        .map(|fields| WhitelistTokenizer::new(fields.iter().map(|s| s.as_str())));

    let mut on_line = |line: &str| -> Result<()> {
        if whitelist_error.is_some() {
            return Ok(());
        }
        if let Ok(min) = parse_minimal(line) {
            if !matches_minimal(&min, targets, query) { return Ok(()); }
            if !within_bounds(&min, bounds) { return Ok(()); }

            // Fast-path: raw line when no transforms needed.
            if whitelist.is_none() && !human_timestamps {
                write_and_count(writer, line.as_bytes(), &mut written)?;
                return Ok(());
            }

            // Byte-level fast-path: human timestamps only, no whitelist.
            // Rewrites `"created_utc"|"retrieved_on"|"edited":<int>` in place without
            // a `serde_json::Value` round-trip. This is the common export/spool config.
            if whitelist.is_none() && human_timestamps {
                rewrite_human_timestamps_bytes(line, &mut ts_buf);
                write_and_count(writer, ts_buf.as_bytes(), &mut written)?;
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
                    if let Some(tracker) = whitelist_tracker {
                        if let Err(e) = tracker.observe(tok_buf == "{}") {
                            whitelist_error = Some(e);
                            return Ok(());
                        }
                    }
                    write_and_count(writer, tok_buf.as_bytes(), &mut written)?;
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

            if let Some(tracker) = whitelist_tracker {
                let dropped_all = matches!(&out_val, Value::Object(obj) if obj.is_empty());
                if let Err(e) = tracker.observe(dropped_all) {
                    whitelist_error = Some(e);
                    return Ok(());
                }
            }

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

    if let Some(e) = whitelist_error {
        return Err(e);
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
