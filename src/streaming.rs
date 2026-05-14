//! Streaming primitives: the one-pass record filter/writer (`stream_job`) with progress,
//! and the usernames collector for a single monthly file.

use crate::filters::{
    matches_full, matches_minimal, matches_subreddit_basic, within_bounds, DateBounds,
};
use crate::json_whitelist::WhitelistTokenizer;
use crate::paths::FileJob;
use crate::query::QuerySpec;
use crate::shard::ShardedWriter;
use crate::zstd_jsonl::{
    for_each_line_with_opts_status, malformed_json_error, parse_minimal, LineStreamOpts,
    PartialReadPolicy,
};
use anyhow::{anyhow, Result};
use indicatif::ProgressBar;
use serde_json::{Map, Value};
use std::io::{self, Write};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

/// The three JSON keys the byte-level rewriter targets, with their `":` suffix
/// pre-baked so the search can flat-scan the line for an anchored byte sequence.
const TIMESTAMP_KEY_PATTERNS: &[&[u8]] =
    &[b"\"created_utc\":", b"\"retrieved_on\":", b"\"edited\":"];

/// Number of accepted records sampled before warning/erroring that a whitelist
/// did not match any top-level keys. Large enough to avoid overreacting to a
/// few schema variants, small enough to catch typos near the start of a run.
pub(crate) const WHITELIST_ZERO_MATCH_SAMPLE: u64 = 100;

const WHITELIST_ZERO_MATCH_HINT: &str = "check field names. Comments use `body`/`parent_id`/`link_id`; submissions use `title`/`selftext`/`domain`.";

/// One projection emission reported to the [`WhitelistMatchTracker`]. The
/// tracker keeps the fast and slow paths' counters separate so the zero-match
/// verdict reflects production semantics (the fast `WhitelistTokenizer` path)
/// rather than being skewed by tokenizer-fallback lines.
#[derive(Debug, Clone, Copy)]
pub(crate) struct WhitelistEmission {
    /// True when the projected object had no whitelisted fields.
    pub emitted_empty_projection: bool,
    /// True when the slow `serde_json::Value` path produced this emission
    /// (the `WhitelistTokenizer` rejected the line structurally).
    pub used_slow_path: bool,
}

#[derive(Debug, Default)]
struct WhitelistMatchState {
    fast_seen: u64,
    fast_empty: u64,
    slow_seen: u64,
    slow_empty: u64,
    reported: bool,
}

/// Shared per-export whitelist sanity checker. It samples accepted records
/// after filtering and reports once if every sampled record projected to `{}`.
///
/// The fast and slow paths are tracked independently — the warning and
/// `strict_whitelist` failure fire only when the fast path (the dominant
/// production path) accumulates a full sample of empty projections, so a
/// stray tokenizer-fallback line cannot suppress a real zero-match condition
/// nor cause a spurious failure when the fast path is healthy.
#[derive(Debug)]
pub(crate) struct WhitelistMatchTracker {
    strict: bool,
    state: std::sync::Mutex<WhitelistMatchState>,
}

impl WhitelistMatchTracker {
    pub(crate) fn new(strict: bool) -> Self {
        Self {
            strict,
            state: std::sync::Mutex::new(WhitelistMatchState::default()),
        }
    }

    pub(crate) fn observe(&self, emission: WhitelistEmission) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("whitelist validation state lock poisoned"))?;

        if emission.used_slow_path {
            if state.slow_seen < WHITELIST_ZERO_MATCH_SAMPLE {
                state.slow_seen += 1;
                if emission.emitted_empty_projection {
                    state.slow_empty += 1;
                }
            }
        } else if state.fast_seen < WHITELIST_ZERO_MATCH_SAMPLE {
            state.fast_seen += 1;
            if emission.emitted_empty_projection {
                state.fast_empty += 1;
            }
        }

        let fast_sample_full = state.fast_seen == WHITELIST_ZERO_MATCH_SAMPLE;
        let fast_all_empty = state.fast_empty == state.fast_seen;
        if fast_sample_full && fast_all_empty && !state.reported {
            state.reported = true;
            let slow_matched = state.slow_seen.saturating_sub(state.slow_empty);
            let msg = if slow_matched > 0 {
                format!(
                    "--whitelist matched zero fields on the first {} fast-path records \
                     ({} slow-path emissions matched and were excluded from this check); {}",
                    WHITELIST_ZERO_MATCH_SAMPLE, slow_matched, WHITELIST_ZERO_MATCH_HINT
                )
            } else {
                format!(
                    "--whitelist matched zero fields on the first {} records; {}",
                    WHITELIST_ZERO_MATCH_SAMPLE, WHITELIST_ZERO_MATCH_HINT
                )
            };
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

#[doc(hidden)]
pub fn apply_human_timestamps(val: &mut Value) {
    if let Some(obj) = val.as_object_mut() {
        // Convert common timestamp fields if they are numeric. "edited" can
        // be bool or number, so only numeric forms are rewritten.
        for key in ["created_utc", "retrieved_on", "edited"] {
            let Some(v) = obj.get_mut(key) else { continue };
            let Some(n) = v.as_i64() else { continue };
            let Ok(dt) = OffsetDateTime::from_unix_timestamp(n) else {
                continue;
            };
            let Ok(s) = dt.format(&Rfc3339) else { continue };
            *v = Value::String(s);
        }
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
        let is_integer =
            j > digits_start && !matches!(bytes.get(j), Some(b'.') | Some(b'e') | Some(b'E'));
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

fn write_raw_line<W: Write + ?Sized>(writer: &mut W, line: &str, written: &mut u64) -> Result<()> {
    write_and_count(writer, line.as_bytes(), written)?;
    Ok(())
}

fn write_with_timestamps<W: Write + ?Sized>(
    writer: &mut W,
    line: &str,
    timestamp_buf: &mut String,
    written: &mut u64,
) -> Result<()> {
    rewrite_human_timestamps_bytes(line, timestamp_buf);
    write_and_count(writer, timestamp_buf.as_bytes(), written)?;
    Ok(())
}

fn write_with_whitelist<W: Write + ?Sized>(
    writer: &mut W,
    line: &str,
    fields: &[String],
    tokenizer: &WhitelistTokenizer,
    tokenizer_buf: &mut String,
    human_timestamps: bool,
    written: &mut u64,
    path: &std::path::Path,
    line_number: u64,
) -> Result<WhitelistEmission> {
    // Preferred path: the streaming tokenizer copies raw value bytes verbatim
    // and never builds a `serde_json::Value`. If it rejects a structurally
    // surprising line, fall back to the slow Value path so correctness on odd
    // records is preserved.
    let tok_result = if human_timestamps {
        // Fused single-pass: project whitelisted keys AND rewrite the three
        // timestamp keys' integer values to RFC3339 in one walk over the raw
        // line bytes. Replaces the older tokenize_into →
        // rewrite_human_timestamps_bytes chain.
        tokenizer.tokenize_and_rewrite_timestamps_into(line, tokenizer_buf)
    } else {
        tokenizer.tokenize_into(line, tokenizer_buf)
    };

    if tok_result.is_ok() {
        let emitted_empty_projection = tokenizer_buf == "{}";
        write_and_count(writer, tokenizer_buf.as_bytes(), written)?;
        return Ok(WhitelistEmission {
            emitted_empty_projection,
            used_slow_path: false,
        });
    }

    let emitted_empty_projection = write_via_value(
        writer,
        line,
        Some(fields),
        human_timestamps,
        written,
        path,
        line_number,
    )?;
    Ok(WhitelistEmission {
        emitted_empty_projection,
        used_slow_path: true,
    })
}

fn write_via_value<W: Write + ?Sized>(
    writer: &mut W,
    line: &str,
    whitelist: Option<&[String]>,
    human_timestamps: bool,
    written: &mut u64,
    path: &std::path::Path,
    line_number: u64,
) -> Result<bool> {
    let val: Value =
        serde_json::from_str(line).map_err(|e| malformed_json_error(path, line_number, e))?;
    let (mut out_val, emitted_empty_projection) = if let Some(fields) = whitelist {
        let mut obj = Map::new();
        if let Some(map) = val.as_object() {
            for k in fields {
                if let Some(v) = map.get(k) {
                    obj.insert(k.clone(), v.clone());
                }
            }
        }
        let is_empty = obj.is_empty();
        (Value::Object(obj), is_empty)
    } else {
        (val, false)
    };

    if human_timestamps {
        apply_human_timestamps(&mut out_val);
    }

    serde_json::to_writer(&mut *writer, &out_val)?;
    writer.write_all(b"\n")?;
    *written += 1;
    Ok(emitted_empty_projection)
}

#[doc(hidden)]
pub fn project_whitelist_line_for_tests(
    line: &str,
    fields: &[String],
    path: &std::path::Path,
    line_number: u64,
) -> Result<String> {
    let tokenizer = WhitelistTokenizer::new(fields.iter().map(|s| s.as_str()));
    let mut tokenizer_buf = String::new();
    let mut out = Vec::new();
    let mut written = 0_u64;
    let _emission = write_with_whitelist(
        &mut out,
        line,
        fields,
        &tokenizer,
        &mut tokenizer_buf,
        false,
        &mut written,
        path,
        line_number,
    )?;
    Ok(String::from_utf8(out)?.trim_end_matches('\n').to_string())
}

#[derive(Clone, Copy)]
enum StreamWritePath<'a> {
    Raw,
    Timestamps,
    Whitelist {
        fields: &'a [String],
        tokenizer: &'a WhitelistTokenizer,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamJobResult {
    pub written: u64,
    /// False when the zstd decoder reported corruption after delivering zero
    /// or more lines. Callers that publish resumable outputs must not commit
    /// such files as complete.
    pub complete: bool,
}

#[allow(dead_code)]
pub fn stream_job<W: Write + ?Sized>(
    job: &FileJob,
    writer: &mut W,
    targets: Option<&Vec<String>>,
    query: &QuerySpec,
    whitelist: &Option<Vec<String>>,
    pb: Option<ProgressBar>,
    bounds: Option<DateBounds>,
    read_buf_bytes: usize,
    human_timestamps: bool,
    whitelist_tracker: Option<&WhitelistMatchTracker>,
) -> Result<StreamJobResult> {
    stream_job_with_partial_policy(
        job,
        writer,
        targets,
        query,
        whitelist,
        pb,
        bounds,
        read_buf_bytes,
        human_timestamps,
        whitelist_tracker,
        false,
        None,
    )
}

pub(crate) fn stream_job_with_partial_policy<W: Write + ?Sized>(
    job: &FileJob,
    writer: &mut W,
    targets: Option<&Vec<String>>,
    query: &QuerySpec,
    whitelist: &Option<Vec<String>>,
    pb: Option<ProgressBar>,
    bounds: Option<DateBounds>,
    read_buf_bytes: usize,
    human_timestamps: bool,
    whitelist_tracker: Option<&WhitelistMatchTracker>,
    allow_partial: bool,
    partial_reporter: Option<&crate::config::PartialReadReporter>,
) -> Result<StreamJobResult> {
    let mut written: u64 = 0;
    let mut ts_buf = String::new();
    let mut tok_buf = String::new();
    let mut whitelist_error: Option<anyhow::Error> = None;

    // Build the streaming tokenizer once per file so the small key-set is
    // hashed exactly once and the buffers above are reused across every line.
    let tokenizer: Option<WhitelistTokenizer> = whitelist
        .as_ref()
        .map(|fields| WhitelistTokenizer::new(fields.iter().map(|s| s.as_str())));

    let write_path = match whitelist.as_deref() {
        None if human_timestamps => StreamWritePath::Timestamps,
        None => StreamWritePath::Raw,
        Some(fields) => StreamWritePath::Whitelist {
            fields,
            tokenizer: tokenizer
                .as_ref()
                .expect("whitelist tokenizer is built when fields are present"),
        },
    };

    let mut line_number: u64 = 0;
    let mut on_line = |line: &str| -> Result<()> {
        line_number += 1;
        if whitelist_error.is_some() {
            return Ok(());
        }
        let min = match parse_minimal(line) {
            Ok(min) => min,
            Err(_) => match serde_json::from_str::<Value>(line) {
                Ok(_) => return Ok(()),
                Err(e) => return Err(malformed_json_error(&job.path, line_number, e)),
            },
        };
        if !matches_minimal(&min, targets, query) {
            return Ok(());
        }
        if !within_bounds(&min, bounds) {
            return Ok(());
        }
        if query.requires_full_parse() {
            let val: Value = serde_json::from_str(line)
                .map_err(|e| malformed_json_error(&job.path, line_number, e))?;
            if !matches_full(&val, job.kind, query) {
                return Ok(());
            }
        }

        match write_path {
            StreamWritePath::Raw => write_raw_line(writer, line, &mut written),
            StreamWritePath::Timestamps => {
                write_with_timestamps(writer, line, &mut ts_buf, &mut written)
            }
            StreamWritePath::Whitelist { fields, tokenizer } => {
                let emission = write_with_whitelist(
                    writer,
                    line,
                    fields,
                    tokenizer,
                    &mut tok_buf,
                    human_timestamps,
                    &mut written,
                    &job.path,
                    line_number,
                )?;
                if let Some(tracker) = whitelist_tracker {
                    if let Err(e) = tracker.observe(emission) {
                        whitelist_error = Some(e);
                    }
                }
                Ok(())
            }
        }
    };

    let partial_read_policy = if allow_partial {
        PartialReadPolicy::AllowPartial
    } else {
        PartialReadPolicy::Strict
    };
    let mut progress_cb = pb.map(|pb| move |delta| pb.inc(delta));
    let mut skip_cb = |path: &std::path::Path, err: &anyhow::Error| {
        if let Some(reporter) = partial_reporter {
            reporter.record(path, err);
        }
    };
    let complete = for_each_line_with_opts_status(
        &job.path,
        LineStreamOpts {
            read_buf_bytes: Some(read_buf_bytes),
            progress: progress_cb.as_mut().map(|cb| cb as &mut dyn FnMut(u64)),
            on_skip: allow_partial
                .then_some(&mut skip_cb as &mut dyn FnMut(&std::path::Path, &anyhow::Error)),
            partial_read_policy,
            ..Default::default()
        },
        |s| on_line(s),
    )?;

    if let Some(e) = whitelist_error {
        return Err(e);
    }

    Ok(StreamJobResult { written, complete })
}

/// Process a single monthly file and optionally tolerate zstd decode errors.
/// In strict mode (the default policy for corpus scans) decode errors are
/// returned. In `allow_partial` mode the file is logged, reported through
/// `on_skip`, and skipped.
pub fn process_file_for_usernames_with_skip(
    job: &FileJob,
    read_buf_bytes: usize,
    subreddit: &str,
    shard_writer: &ShardedWriter,
    pb: Option<ProgressBar>,
    allow_partial: bool,
    partial_reporter: Option<&crate::config::PartialReadReporter>,
    mut on_skip: impl FnMut(&std::path::Path, &anyhow::Error),
) -> Result<()> {
    let mut line_number: u64 = 0;
    let mut handle_line = |line: &str| -> Result<()> {
        line_number += 1;
        let min = match parse_minimal(line) {
            Ok(min) => min,
            Err(_) => match serde_json::from_str::<Value>(line) {
                Ok(_) => return Ok(()),
                Err(e) => return Err(malformed_json_error(&job.path, line_number, e)),
            },
        };
        if !matches_subreddit_basic(&min, subreddit) {
            return Ok(());
        }
        if let Some(author) = min.author.as_deref() {
            let a = author.trim();
            if a.is_empty() || a == "[deleted]" || a == "[removed]" {
                return Ok(());
            }
            shard_writer.write(a)?;
        }
        Ok(())
    };

    let partial_read_policy = if allow_partial {
        PartialReadPolicy::AllowPartial
    } else {
        PartialReadPolicy::Strict
    };
    let mut progress_cb = pb.map(|pb| move |delta| pb.inc(delta));
    let mut skip_cb = |path: &std::path::Path, err: &anyhow::Error| {
        if let Some(reporter) = partial_reporter {
            reporter.record(path, err);
        }
        on_skip(path, err);
    };
    for_each_line_with_opts_status(
        &job.path,
        LineStreamOpts {
            read_buf_bytes: Some(read_buf_bytes),
            progress: progress_cb.as_mut().map(|cb| cb as &mut dyn FnMut(u64)),
            on_skip: allow_partial
                .then_some(&mut skip_cb as &mut dyn FnMut(&std::path::Path, &anyhow::Error)),
            partial_read_policy,
            ..Default::default()
        },
        |s| handle_line(s),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::date::YearMonth;
    use crate::paths::FileKind;

    fn write_zst(path: &std::path::Path, payload: &[u8]) {
        let f = std::fs::File::create(path).unwrap();
        let mut enc = zstd::stream::write::Encoder::new(f, 3).unwrap();
        std::io::Write::write_all(&mut enc, payload).unwrap();
        enc.finish().unwrap();
    }

    struct FailsAfterWriter {
        remaining: usize,
    }

    impl std::io::Write for FailsAfterWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.remaining == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "writer boom",
                ));
            }
            let n = self.remaining.min(buf.len());
            self.remaining -= n;
            Ok(n)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn stream_job_propagates_writer_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("RC_2020-01.zst");
        write_zst(
            &path,
            b"{\"id\":\"c1\",\"author\":\"alice\",\"subreddit\":\"rust\",\"created_utc\":1577836800}\n",
        );

        let job = FileJob {
            kind: FileKind::Comment,
            ym: YearMonth::new(2020, 1),
            path,
        };
        let query = QuerySpec::default();
        let whitelist: Option<Vec<String>> = None;
        let mut writer = FailsAfterWriter { remaining: 8 };

        let res = stream_job(
            &job,
            &mut writer,
            None,
            &query,
            &whitelist,
            None,
            None,
            16 * 1024,
            false,
            None,
        );

        let err = res.expect_err("writer errors from stream_job must propagate");
        assert!(
            err.to_string().contains("writer boom"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn whitelist_slow_path_reports_empty_projection_independent_of_tokenizer_buffer() {
        let fields = vec!["id".to_string()];
        let tokenizer = WhitelistTokenizer::new(fields.iter().map(|s| s.as_str()));
        let mut tokenizer_buf = String::new();
        let mut out = Vec::new();
        let mut written = 0_u64;

        let fast = write_with_whitelist(
            &mut out,
            r#"{"id":"kept","subreddit":"programming","author":"a"}"#,
            &fields,
            &tokenizer,
            &mut tokenizer_buf,
            false,
            &mut written,
            std::path::Path::new("test.jsonl"),
            1,
        )
        .unwrap();
        assert!(
            !fast.emitted_empty_projection,
            "sanity: first projection contains id"
        );
        assert!(!fast.used_slow_path);
        assert!(tokenizer_buf.contains("kept"));

        // Top-level arrays are valid JSON so the slow Value path can project
        // them, but the byte tokenizer rejects them. The emitted object is
        // empty because there is no top-level object containing `id`; this
        // must be computed from the slow path, not from tokenizer_buf.
        let slow = write_with_whitelist(
            &mut out,
            r#"[{"id":"not-a-top-level-object"}]"#,
            &fields,
            &tokenizer,
            &mut tokenizer_buf,
            false,
            &mut written,
            std::path::Path::new("test.jsonl"),
            2,
        )
        .unwrap();
        assert!(
            slow.emitted_empty_projection,
            "slow-path array projection should be reported as empty"
        );
        assert!(
            slow.used_slow_path,
            "top-level array must take the Value slow path"
        );
        assert_eq!(written, 2);
    }

    #[test]
    fn strict_whitelist_ignores_slow_path_only_emissions() {
        // A run where 100% of slow-path emissions are empty but the fast path
        // is never sampled must NOT trip the strict_whitelist verdict. The
        // tracker's threshold is fast_seen, so slow-path-only activity leaves
        // the verdict deferred.
        let fields = vec!["not_present".to_string()];
        let tokenizer = WhitelistTokenizer::new(fields.iter().map(|s| s.as_str()));
        let tracker = WhitelistMatchTracker::new(true);
        let mut tokenizer_buf = String::new();
        let mut out = Vec::new();
        let mut written = 0_u64;

        for i in 0..(WHITELIST_ZERO_MATCH_SAMPLE * 2) {
            let emission = write_with_whitelist(
                &mut out,
                r#"[{"id":"slow"}]"#,
                &fields,
                &tokenizer,
                &mut tokenizer_buf,
                false,
                &mut written,
                std::path::Path::new("test.jsonl"),
                i + 1,
            )
            .unwrap();
            assert!(emission.used_slow_path);
            assert!(emission.emitted_empty_projection);
            tracker
                .observe(emission)
                .expect("slow-path emissions must not trip strict_whitelist");
        }
    }

    #[test]
    fn strict_whitelist_fires_on_fast_path_empty_projection_sample() {
        // Mirror image of the test above: a fully empty fast-path sample DOES
        // trip the verdict, regardless of slow-path activity in between.
        let fields = vec!["not_present".to_string()];
        let tokenizer = WhitelistTokenizer::new(fields.iter().map(|s| s.as_str()));
        let tracker = WhitelistMatchTracker::new(true);
        let mut tokenizer_buf = String::new();
        let mut out = Vec::new();
        let mut written = 0_u64;
        let mut err = None;

        for i in 0..WHITELIST_ZERO_MATCH_SAMPLE {
            // All fast-path empty projections.
            let emission = write_with_whitelist(
                &mut out,
                r#"{"id":"fast","subreddit":"programming","author":"a"}"#,
                &fields,
                &tokenizer,
                &mut tokenizer_buf,
                false,
                &mut written,
                std::path::Path::new("test.jsonl"),
                i + 1,
            )
            .unwrap();
            assert!(!emission.used_slow_path);
            if let Err(e) = tracker.observe(emission) {
                err = Some(e);
                break;
            }
        }

        let msg = err
            .expect("strict tracker must error after a full fast-path empty sample")
            .to_string();
        assert!(
            msg.contains("--whitelist matched zero fields"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn fast_path_non_empty_emissions_suppress_warning_even_with_empty_slow_path() {
        // Inverse failure mode from the bug: when the fast path is healthy
        // (matches), interleaved slow-path empty projections must not push
        // the tracker into a false warning.
        let fields = vec!["id".to_string()];
        let tokenizer = WhitelistTokenizer::new(fields.iter().map(|s| s.as_str()));
        let tracker = WhitelistMatchTracker::new(true);
        let mut tokenizer_buf = String::new();
        let mut out = Vec::new();
        let mut written = 0_u64;

        for i in 0..WHITELIST_ZERO_MATCH_SAMPLE {
            let line = if i % 2 == 0 {
                r#"{"id":"kept","subreddit":"programming","author":"a"}"#
            } else {
                r#"[{"id":"slow"}]"#
            };
            let emission = write_with_whitelist(
                &mut out,
                line,
                &fields,
                &tokenizer,
                &mut tokenizer_buf,
                false,
                &mut written,
                std::path::Path::new("test.jsonl"),
                i + 1,
            )
            .unwrap();
            tracker
                .observe(emission)
                .expect("healthy fast-path emissions must not trigger strict_whitelist");
        }
    }
}
