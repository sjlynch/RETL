use anyhow::Result;
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

/// Minimal line-level schema for fast filtering.
/// Extra fields are ignored by serde.
/// NOTE: includes `score` to enable fast numeric filters.
/// Includes `selftext`, `body`, `title`, `parent_id` so keyword/URL filtering works
/// without a full parse. Includes `domain` (submissions) and `id` (both kinds).
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

    #[allow(dead_code)]
    pub parent_id: Option<String>,// comments

    pub domain: Option<String>,   // submissions (used by domains_in)
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

// ----------------------------- Streaming ----------------------------------

/// Stream a zstd JSONL file line-by-line; call `on_line` with raw &str (default buffers).
///
/// We request `window_log_max(31)` up front to avoid "Frame requires too much memory"
/// on very large frames. If it still fails (e.g., checksum/corruption), log a single
/// warning and skip the file (do not abort the run).
#[allow(dead_code)]
pub fn for_each_line(path: &Path, mut on_line: impl FnMut(&str) -> Result<()>) -> Result<()> {
    match for_each_line_attempt(path, &mut on_line, Some(31), None) {
        Ok(()) => Ok(()),
        Err(e) => {
            warn_decode_skip(path, &e);
            Ok(())
        }
    }
}

/// Tunable version: choose BufReader capacity via `read_buf_bytes`.
/// Same corruption handling and skip semantics as `for_each_line`.
pub fn for_each_line_cfg(
    path: &Path,
    read_buf_bytes: usize,
    mut on_line: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    match for_each_line_attempt(path, &mut on_line, Some(31), Some(read_buf_bytes)) {
        Ok(()) => Ok(()),
        Err(e) => {
            warn_decode_skip(path, &e);
            Ok(())
        }
    }
}

fn for_each_line_attempt(
    path: &Path,
    on_line: &mut impl FnMut(&str) -> Result<()>,
    window_log_max: Option<u32>,
    read_buf_bytes: Option<usize>,
) -> Result<()> {
    let file = open_with_backoff(path, 16, 50)?;
    let mut decoder = Decoder::new(file)?;
    if let Some(log) = window_log_max {
        decoder.window_log_max(log)?;
    }
    let cap = read_buf_bytes.unwrap_or(16 * 1024);
    let mut reader = BufReader::with_capacity(cap, decoder);

    let mut buf = String::with_capacity(16 * 1024);
    loop {
        buf.clear();
        let n = reader.read_line(&mut buf)?;
        if n == 0 {
            break;
        }
        if buf.ends_with('\n') {
            let _ = buf.pop();
            if buf.ends_with('\r') { let _ = buf.pop(); }
        }
        on_line(&buf)?;
        // Cooperative memory backoff
        maybe_throttle_low_memory(0.10);
    }
    Ok(())
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

/// Same as `for_each_line` but calls `on_progress(delta_bytes_read)` after each line
/// and allows a custom BufReader capacity via `read_buf_bytes`.
/// On corruption, logs a warning, **advances the progress by the file's size**,
/// and skips the file.
pub fn for_each_line_with_progress_cfg(
    path: &Path,
    read_buf_bytes: usize,
    mut on_progress: impl FnMut(u64),
    mut on_line: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    match for_each_line_attempt_with_progress(path, Some(read_buf_bytes), &mut on_progress, &mut on_line, Some(31), /*throttle=*/true) {
        Ok(()) => Ok(()),
        Err(e) => {
            warn_decode_skip(path, &e);
            if let Ok(meta) = fs::metadata(path) {
                on_progress(meta.len());
            }
            Ok(())
        }
    }
}

/// NEW: progress-aware streaming **without** per-line memory throttle.
/// Useful for stages that briefly use more RAM (e.g., building parent caches)
/// where the backoff would otherwise dominate runtime.
pub fn for_each_line_with_progress_cfg_no_throttle(
    path: &Path,
    read_buf_bytes: usize,
    mut on_progress: impl FnMut(u64),
    mut on_line: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    match for_each_line_attempt_with_progress(path, Some(read_buf_bytes), &mut on_progress, &mut on_line, Some(31), /*throttle=*/false) {
        Ok(()) => Ok(()),
        Err(e) => {
            warn_decode_skip(path, &e);
            if let Ok(meta) = fs::metadata(path) {
                on_progress(meta.len());
            }
            Ok(())
        }
    }
}

fn for_each_line_attempt_with_progress(
    path: &Path,
    read_buf_bytes: Option<usize>,
    on_progress: &mut impl FnMut(u64),
    on_line: &mut impl FnMut(&str) -> Result<()>,
    window_log_max: Option<u32>,
    throttle: bool,
) -> Result<()> {
    let file = open_with_backoff(path, 16, 50)?;
    let counter = Arc::new(AtomicU64::new(0));
    let cnt = CountingReader { inner: file, counter: counter.clone() };

    let mut decoder = Decoder::new(cnt)?;
    if let Some(log) = window_log_max {
        decoder.window_log_max(log)?;
    }
    let cap = read_buf_bytes.unwrap_or(16 * 1024);
    let mut reader = BufReader::with_capacity(cap, decoder);

    let mut buf = String::with_capacity(16 * 1024);
    let mut last = 0u64;
    loop {
        buf.clear();
        let n = reader.read_line(&mut buf)?;
        if n == 0 {
            // final progress flush
            let cur = counter.load(Ordering::Relaxed);
            if cur > last {
                on_progress(cur - last);
            }
            break;
        }
        if buf.ends_with('\n') {
            let _ = buf.pop();
            if buf.ends_with('\r') { let _ = buf.pop(); }
        }
        // progress
        let cur = counter.load(Ordering::Relaxed);
        if cur > last {
            on_progress(cur - last);
            last = cur;
        }
        on_line(&buf)?;
        if throttle {
            maybe_throttle_low_memory(0.10);
        }
    }
    Ok(())
}

// ----------------------------- Integrity checks ----------------------------------

/// QUICK check: attempt to decode up to `max_decompressed_bytes` and stop.
pub fn quick_validate_zst(path: &Path, max_decompressed_bytes: u64) -> Result<()> {
    let file = open_with_backoff(path, 16, 50)?;
    let mut decoder = Decoder::new(file)?;
    decoder.window_log_max(31)?;
    let mut limited = decoder.take(max_decompressed_bytes);
    // Discard output; we only care about whether decoding produces an error.
    io::copy(&mut limited, &mut io::sink())?;
    Ok(())
}

/// FULL check: decode the entire stream to EOF.
pub fn validate_zst_full(path: &Path) -> Result<()> {
    let file = open_with_backoff(path, 16, 50)?;
    let mut decoder = Decoder::new(file)?;
    decoder.window_log_max(31)?;
    io::copy(&mut decoder, &mut io::sink())?;
    Ok(())
}
