use anyhow::{anyhow, Result};
use serde::Deserialize;
use std::fs;
use std::io::{self, BufRead, BufReader, Read};
use std::path::Path;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use zstd::stream::read::Decoder;

use crate::mem::maybe_throttle_low_memory;
use crate::util::open_with_backoff;

/// Prevents "Frame requires too much memory" on large Reddit dumps.
///
/// Late-year Reddit dumps were written with the spec's max window (~2 GiB);
/// the zstd default rejects them with that error. Every decoder in this
/// module passes this through `decoder.window_log_max(...)`.
const ZSTD_WINDOW_LOG_MAX: u32 = 31;

/// Fire the cooperative-throttle callback every 65 536 lines.
///
/// Used as a bit-mask on a wrapping `u32` tick counter
/// (`tick & THROTTLE_SAMPLE_MASK == 0`). Bucketing/dedupe carry their own
/// bounded-channel backpressure, so this throttle is a coarse safety net —
/// sample less often to keep mutex contention out of the hot read loop.
const THROTTLE_SAMPLE_MASK: u32 = 0xFFFF;

/// Default `BufReader` capacity when callers do not specify one.
const DEFAULT_READ_BUF_BYTES: usize = 16 * 1024;

/// Minimal line-level schema for fast filtering.
/// Extra fields are ignored by serde.
/// NOTE: includes `score` to enable fast numeric filters.
/// Includes `selftext`, `body`, `title`, `url`, and `parent_id` so keyword/URL
/// filtering works without a full parse. Includes `domain` (submissions) and
/// `id` (both kinds).
#[derive(Debug, Deserialize)]
pub struct MinimalRecord {
    pub subreddit: Option<String>,
    pub author: Option<String>,
    pub created_utc: Option<i64>,
    pub score: Option<i64>,

    // ID of the record (present on both RC and RS)
    pub id: Option<String>,

    // Optional textual / metadata fields (only present when applicable):
    pub selftext: Option<String>, // submissions
    pub body: Option<String>,     // comments
    pub title: Option<String>,    // submissions
    pub url: Option<String>,      // submissions (outbound URL on link posts)

    #[allow(dead_code)]
    pub parent_id: Option<String>, // comments

    pub domain: Option<String>, // submissions (used by domains_in)
}

// ----------------------------- Helpers for full-error logging ------------------------------------

#[inline]
fn warn_decode_skip(path: &Path, e: &anyhow::Error) {
    // Try to print an absolute, canonical path to avoid truncation/ambiguity.
    let abs = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    // Emit a multi-line message to stderr (separate from progress bars) and to tracing.
    let msg = format!(
        "Skipping zstd file after decode error\n  path : {}\n  error: {}\n\
         note : This usually indicates file corruption (often late/trailing). \
                Quick integrity sampling may miss trailing corruption. \
                Consider running a Full integrity check or re-downloading this month. \
                The pipeline will skip this file and continue.",
        abs.display(),
        e
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

// ----------------------------- Streaming ----------------------------------

/// Options for [`for_each_line_with_opts`].
///
/// Every field has a sensible default (`None`/`true`); callers fill in only
/// the knobs they actually use. Lifetime `'a` ties the trait-object callbacks
/// to the caller's stack.
pub struct LineStreamOpts<'a> {
    /// `BufReader` capacity in bytes. `None` → [`DEFAULT_READ_BUF_BYTES`].
    pub read_buf_bytes: Option<usize>,
    /// If `Some`, called with the delta of compressed bytes read after each
    /// line, plus a final flush at EOF. On a decode error the file's full
    /// length is reported so progress bars stay monotonic.
    pub progress: Option<&'a mut dyn FnMut(u64)>,
    /// If `Some`, called once when the file is skipped due to a decode error.
    /// The error is also logged via tracing/stderr; the outer call still
    /// returns `Ok(())`.
    pub on_skip: Option<&'a mut dyn FnMut(&Path, &anyhow::Error)>,
    /// Sample [`maybe_throttle_low_memory`] every
    /// [`THROTTLE_SAMPLE_MASK`]+1 lines. Set `false` for stages that briefly
    /// allocate a lot (e.g., parent-cache builds) where the backoff would
    /// otherwise dominate runtime.
    pub throttle: bool,
}

impl<'a> Default for LineStreamOpts<'a> {
    fn default() -> Self {
        Self {
            read_buf_bytes: None,
            progress: None,
            on_skip: None,
            throttle: true,
        }
    }
}

/// Stream a zstd JSONL file line-by-line using `opts`, calling `on_line`
/// with each raw `&str` (newline already stripped).
///
/// We request `window_log_max(ZSTD_WINDOW_LOG_MAX)` up front to avoid
/// "Frame requires too much memory" on very large frames. If decoding
/// still fails (e.g., checksum/corruption), we log a single warning,
/// invoke `opts.on_skip` (if set), advance progress to the file's size
/// (if a progress callback is set), and report the skipped/partial read to
/// callers that need resume correctness.
pub fn for_each_line_with_opts_status(
    path: &Path,
    opts: LineStreamOpts<'_>,
    mut on_line: impl FnMut(&str) -> Result<()>,
) -> Result<bool> {
    let LineStreamOpts {
        read_buf_bytes,
        mut progress,
        mut on_skip,
        throttle,
    } = opts;
    let result = for_each_line_attempt(
        path,
        read_buf_bytes,
        progress.as_deref_mut(),
        throttle,
        &mut on_line,
    );
    match result {
        Ok(()) => Ok(true),
        Err(LineStreamAttemptError::Open(e)) => Err(e),
        Err(LineStreamAttemptError::Decode(e)) => {
            warn_decode_skip(path, &e);
            if let Some(cb) = on_skip.as_deref_mut() {
                cb(path, &e);
            }
            // Keep progress bars monotonic on skip.
            if let Some(cb) = progress.as_deref_mut() {
                if let Ok(meta) = fs::metadata(path) {
                    cb(meta.len());
                }
            }
            Ok(false)
        }
        Err(LineStreamAttemptError::Callback(e)) => Err(e),
    }
}

/// Back-compat wrapper for callers that tolerate corrupt zstd inputs by
/// warning and continuing.
pub fn for_each_line_with_opts(
    path: &Path,
    opts: LineStreamOpts<'_>,
    on_line: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    for_each_line_with_opts_status(path, opts, on_line).map(|_| ())
}

/// Tunable line-stream entry point used throughout the library.
///
/// Thin shim over [`for_each_line_with_opts`] for callers that only need a
/// custom `BufReader` capacity.
pub fn for_each_line_cfg(
    path: &Path,
    read_buf_bytes: usize,
    on_line: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    for_each_line_with_opts(
        path,
        LineStreamOpts {
            read_buf_bytes: Some(read_buf_bytes),
            ..Default::default()
        },
        on_line,
    )
}

/// Like [`for_each_line_cfg`] but returns `Ok(false)` when a zstd decode error
/// was tolerated after zero or more lines had already been delivered.
pub fn for_each_line_cfg_status(
    path: &Path,
    read_buf_bytes: usize,
    on_line: impl FnMut(&str) -> Result<()>,
) -> Result<bool> {
    for_each_line_with_opts_status(
        path,
        LineStreamOpts {
            read_buf_bytes: Some(read_buf_bytes),
            ..Default::default()
        },
        on_line,
    )
}

/// Like [`for_each_line_cfg`] but reports skip events to the caller.
pub fn for_each_line_cfg_with_skip(
    path: &Path,
    read_buf_bytes: usize,
    mut on_skip: impl FnMut(&Path, &anyhow::Error),
    on_line: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    for_each_line_with_opts(
        path,
        LineStreamOpts {
            read_buf_bytes: Some(read_buf_bytes),
            on_skip: Some(&mut on_skip),
            ..Default::default()
        },
        on_line,
    )
}

/// Like [`for_each_line_cfg`] but additionally calls
/// `on_progress(delta_bytes_read)` after each line and at EOF.
///
/// On corruption, advances progress by the file's size before returning
/// `Ok(())` so progress bars stay monotonic.
pub fn for_each_line_with_progress_cfg(
    path: &Path,
    read_buf_bytes: usize,
    mut on_progress: impl FnMut(u64),
    on_line: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    for_each_line_with_opts(
        path,
        LineStreamOpts {
            read_buf_bytes: Some(read_buf_bytes),
            progress: Some(&mut on_progress),
            ..Default::default()
        },
        on_line,
    )
}

/// Like [`for_each_line_with_progress_cfg`] but returns `Ok(false)` when a
/// zstd decode error was tolerated.
pub fn for_each_line_with_progress_cfg_status(
    path: &Path,
    read_buf_bytes: usize,
    mut on_progress: impl FnMut(u64),
    on_line: impl FnMut(&str) -> Result<()>,
) -> Result<bool> {
    for_each_line_with_opts_status(
        path,
        LineStreamOpts {
            read_buf_bytes: Some(read_buf_bytes),
            progress: Some(&mut on_progress),
            ..Default::default()
        },
        on_line,
    )
}

/// Progress-aware streaming **without** the per-line memory throttle.
///
/// Useful for stages that briefly use more RAM (e.g., building parent
/// caches) where the backoff would otherwise dominate runtime.
pub fn for_each_line_with_progress_cfg_no_throttle(
    path: &Path,
    read_buf_bytes: usize,
    mut on_progress: impl FnMut(u64),
    on_line: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    for_each_line_with_opts(
        path,
        LineStreamOpts {
            read_buf_bytes: Some(read_buf_bytes),
            progress: Some(&mut on_progress),
            throttle: false,
            ..Default::default()
        },
        on_line,
    )
}

/// Like [`for_each_line_with_progress_cfg`] but reports skip events to the caller.
pub fn for_each_line_with_progress_cfg_with_skip(
    path: &Path,
    read_buf_bytes: usize,
    mut on_skip: impl FnMut(&Path, &anyhow::Error),
    mut on_progress: impl FnMut(u64),
    on_line: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    for_each_line_with_opts(
        path,
        LineStreamOpts {
            read_buf_bytes: Some(read_buf_bytes),
            progress: Some(&mut on_progress),
            on_skip: Some(&mut on_skip),
            ..Default::default()
        },
        on_line,
    )
}

/// A `Read` wrapper that counts compressed bytes read.
struct CountingReader<R: Read> {
    inner: R,
    counter: Arc<AtomicU64>,
}
impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.counter.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }
}

enum LineStreamAttemptError {
    Open(anyhow::Error),
    Decode(anyhow::Error),
    Callback(anyhow::Error),
}

/// Single inner read loop. The two former `_attempt` paths only differed on
/// whether the file was wrapped in `CountingReader` to drive a progress
/// callback — we always wrap it now (the atomic add lands once per
/// `BufReader` refill, ~every 16 KiB, which is noise next to zstd decode
/// cost), and only invoke the callback when one is set.
///
/// The two lifetime parameters decouple the *borrow* lifetime from the
/// trait-object lifetime; without that, lifetime elision would tie the
/// trait object's lifetime to the borrow, which then forces the caller's
/// borrow to span the entire owning struct's lifetime and conflicts with
/// reusing the same callback on the post-error fallback path.
fn for_each_line_attempt<'borrow, 'cb: 'borrow>(
    path: &Path,
    read_buf_bytes: Option<usize>,
    mut on_progress: Option<&'borrow mut (dyn FnMut(u64) + 'cb)>,
    throttle: bool,
    on_line: &mut impl FnMut(&str) -> Result<()>,
) -> std::result::Result<(), LineStreamAttemptError> {
    let file = open_with_backoff(path, 16, 50).map_err(|e| {
        LineStreamAttemptError::Open(
            anyhow::Error::new(e).context(format!("open zstd input {}", path.display())),
        )
    })?;
    let counter = Arc::new(AtomicU64::new(0));
    let cnt = CountingReader {
        inner: file,
        counter: counter.clone(),
    };

    let mut decoder = Decoder::new(cnt).map_err(|e| LineStreamAttemptError::Decode(e.into()))?;
    decoder
        .window_log_max(ZSTD_WINDOW_LOG_MAX)
        .map_err(|e| LineStreamAttemptError::Decode(e.into()))?;

    let cap = read_buf_bytes.unwrap_or(DEFAULT_READ_BUF_BYTES);
    let mut reader = BufReader::with_capacity(cap, decoder);

    let mut buf = String::with_capacity(DEFAULT_READ_BUF_BYTES);
    let mut last: u64 = 0;
    // Sampled cooperative memory backoff: maybe_throttle_low_memory acquires a
    // global Mutex (see src/mem.rs). Now that bucketing/dedupe carry their own
    // bounded backpressure channels, this throttle is a coarse safety net —
    // sample every THROTTLE_SAMPLE_MASK+1 lines to keep mutex contention out
    // of the hot read loop.
    let mut tick: u32 = 0;
    loop {
        buf.clear();
        let n = reader
            .read_line(&mut buf)
            .map_err(|e| LineStreamAttemptError::Decode(e.into()))?;
        if n == 0 {
            // Final progress flush at EOF.
            if let Some(cb) = on_progress.as_deref_mut() {
                let cur = counter.load(Ordering::Relaxed);
                if cur > last {
                    cb(cur - last);
                }
            }
            break;
        }
        if buf.ends_with('\n') {
            let _ = buf.pop();
            if buf.ends_with('\r') {
                let _ = buf.pop();
            }
        }
        // Per-line progress delta (preserves prior cadence: drained before
        // the user's `on_line` callback runs so "bytes read" never lags
        // "lines seen").
        if let Some(cb) = on_progress.as_deref_mut() {
            let cur = counter.load(Ordering::Relaxed);
            if cur > last {
                cb(cur - last);
                last = cur;
            }
        }
        on_line(&buf).map_err(LineStreamAttemptError::Callback)?;
        if throttle {
            tick = tick.wrapping_add(1);
            if tick & THROTTLE_SAMPLE_MASK == 0 {
                maybe_throttle_low_memory(0.10);
            }
        }
    }
    Ok(())
}

pub use crate::integrity::{quick_validate_zst, validate_zst_full};

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;

    fn write_zst_with_checksum(path: &Path, payload: &[u8]) {
        let f = fs::File::create(path).unwrap();
        let mut enc = zstd::stream::write::Encoder::new(f, 3).unwrap();
        enc.include_checksum(true).unwrap();
        enc.write_all(payload).unwrap();
        enc.finish().unwrap();
    }

    /// Bit-flipping a byte mid-stream in a checksum-bearing zstd file must:
    ///   - cause `validate_zst_full` (Full integrity) to return an error
    ///   - cause `for_each_line_cfg` (the normal scanning path) to log a
    ///     warning and return Ok (skip the file, don't abort the run)
    #[test]
    fn bit_flipped_frame_fails_full_but_scan_skips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("flipped.zst");

        // Enough payload that there's plenty of compressed data mid-stream.
        let mut payload = Vec::new();
        for i in 0..2000 {
            payload.extend_from_slice(
                format!(
                    "{{\"id\":\"r{}\",\"author\":\"u{}\",\"subreddit\":\"x\"}}\n",
                    i, i
                )
                .as_bytes(),
            );
        }
        write_zst_with_checksum(&path, &payload);

        // Sanity: original file validates cleanly.
        validate_zst_full(&path).expect("pristine file should validate");

        // Flip a couple of bytes well past the frame header and well before
        // the trailing checksum. Two bytes XOR'd with 0xFF makes the entropy
        // decoder reliably produce an error rather than (rarely) decoding
        // garbage that happens to checksum-match.
        let mut bytes = fs::read(&path).unwrap();
        let n = bytes.len();
        let off = n / 2;
        bytes[off] ^= 0xFF;
        bytes[off + 1] ^= 0xFF;
        fs::write(&path, &bytes).unwrap();

        // Full integrity must catch this.
        assert!(
            validate_zst_full(&path).is_err(),
            "validate_zst_full must reject a bit-flipped frame"
        );

        // The normal scan path must skip gracefully (warn + continue), not bubble.
        let mut lines_seen = 0usize;
        let res = for_each_line_cfg(&path, 16 * 1024, |_line| {
            lines_seen += 1;
            Ok(())
        });
        assert!(
            res.is_ok(),
            "for_each_line_cfg should skip a corrupt file gracefully, got {:?}",
            res
        );

        let status = for_each_line_cfg_status(&path, 16 * 1024, |_line| Ok(()))
            .expect("status API should not bubble corrupt-frame decode errors");
        assert!(!status, "status API must report the corrupt file as incomplete");

        // The *_with_skip variant must also skip gracefully AND surface the
        // skip event to the caller via `on_skip`. The path passed to the
        // callback must match the file we tried to read, and the captured
        // error must be non-empty.
        let mut skip_calls: Vec<(PathBuf, String)> = Vec::new();
        let mut lines_seen2 = 0usize;
        let res = for_each_line_cfg_with_skip(
            &path,
            16 * 1024,
            |p, e| skip_calls.push((p.to_path_buf(), e.to_string())),
            |_line| {
                lines_seen2 += 1;
                Ok(())
            },
        );
        assert!(
            res.is_ok(),
            "for_each_line_cfg_with_skip should skip a corrupt file gracefully, got {:?}",
            res
        );
        assert_eq!(
            skip_calls.len(),
            1,
            "on_skip must fire exactly once for a corrupt file, got {:?}",
            skip_calls
        );
        let (skipped_path, err_msg) = &skip_calls[0];
        assert_eq!(
            skipped_path, &path,
            "on_skip must receive the original path"
        );
        assert!(
            !err_msg.is_empty(),
            "on_skip must receive a non-empty error description"
        );
    }

    /// On a healthy file, the *_with_skip variant must NOT invoke `on_skip`
    /// and must still deliver every line to `on_line`. This guards against
    /// regressions where the skip callback fires on the happy path.
    #[test]
    fn healthy_file_does_not_trigger_on_skip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("healthy.zst");

        let mut payload = Vec::new();
        for i in 0..50 {
            payload.extend_from_slice(format!("{{\"id\":\"r{}\"}}\n", i).as_bytes());
        }
        write_zst_with_checksum(&path, &payload);

        let mut skip_count = 0usize;
        let mut lines_seen = 0usize;
        let res = for_each_line_cfg_with_skip(
            &path,
            16 * 1024,
            |_, _| skip_count += 1,
            |_line| {
                lines_seen += 1;
                Ok(())
            },
        );
        assert!(res.is_ok(), "healthy file must scan without error");
        assert_eq!(skip_count, 0, "on_skip must not fire on healthy files");
        assert_eq!(
            lines_seen, 50,
            "all lines must be delivered on healthy files"
        );
    }

    #[test]
    fn missing_file_open_error_propagates_without_skip_callback() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.zst");

        let mut skip_count = 0usize;
        let err =
            for_each_line_cfg_with_skip(&path, 16 * 1024, |_, _| skip_count += 1, |_line| Ok(()))
                .expect_err("missing input must be a fatal open error");

        assert_eq!(skip_count, 0, "on_skip must not fire for open errors");
        let msg = err.to_string();
        assert!(msg.contains("open zstd input"), "unexpected error: {msg}");
        assert!(msg.contains("missing.zst"), "unexpected error: {msg}");
    }

    #[test]
    fn callback_error_propagates_without_skip_callback() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("callback-error.zst");
        write_zst_with_checksum(&path, b"{\"id\":\"r1\"}\n");

        let err = for_each_line_cfg(&path, 16 * 1024, |_line| {
            Err(anyhow::anyhow!("callback boom"))
        })
        .expect_err("callback errors must propagate from for_each_line_cfg");
        assert!(
            err.to_string().contains("callback boom"),
            "unexpected error: {err}"
        );

        let mut skip_count = 0usize;
        let err = for_each_line_cfg_with_skip(
            &path,
            16 * 1024,
            |_, _| skip_count += 1,
            |_line| Err(anyhow::anyhow!("callback boom")),
        )
        .expect_err("callback errors must propagate from skip variants");
        assert!(
            err.to_string().contains("callback boom"),
            "unexpected error: {err}"
        );
        assert_eq!(
            skip_count, 0,
            "on_skip must not fire for caller callback errors"
        );
    }
}
