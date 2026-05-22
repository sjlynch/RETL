use crate::util::replace_file_atomic_backoff;
use anyhow::{Context, Result};
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

/// Default upper bound (16 MiB) on a single NDJSON line.
///
/// Reddit comment/submission bodies are typically <10 KiB, but the JSONL
/// format itself admits arbitrarily large lines. Without a cap, the standard
/// `BufRead::read_line` will grow its buffer without limit, so a single
/// hostile or accidentally-mangled multi-GiB line can OOM the worker
/// (silently, as an allocator panic across the rayon pool) instead of
/// surfacing as a structured error.
pub const DEFAULT_MAX_LINE_BYTES: usize = 16 * 1024 * 1024;

/// Minimal NDJSON reader with buffering and empty-line trimming.
/// Uses robust open-with-backoff for Windows-friendliness.
///
/// Each line is bounded by `max_line_bytes` (default
/// [`DEFAULT_MAX_LINE_BYTES`]). Exceeding the cap surfaces as
/// `io::ErrorKind::InvalidData` rather than allowing unbounded buffer
/// growth.
pub struct NdjsonReader {
    rdr: BufReader<File>,
    path: PathBuf,
    max_line_bytes: usize,
}

impl NdjsonReader {
    pub fn open(path: &Path, buf_bytes: usize) -> io::Result<Self> {
        Self::open_with_max(path, buf_bytes, DEFAULT_MAX_LINE_BYTES)
    }

    /// Open with an explicit per-line byte cap. Use this when the caller
    /// knows the input may contain unusually large records and wants to
    /// raise (or tighten) the default 16 MiB ceiling.
    pub fn open_with_max(path: &Path, buf_bytes: usize, max_line_bytes: usize) -> io::Result<Self> {
        let f = crate::util::open_with_default_backoff(path)?;
        Ok(Self {
            rdr: BufReader::with_capacity(buf_bytes.max(8 * 1024), f),
            path: path.to_path_buf(),
            max_line_bytes,
        })
    }

    /// Builder-style override of the per-line byte cap.
    pub fn with_max_line_bytes(mut self, max_line_bytes: usize) -> Self {
        self.max_line_bytes = max_line_bytes;
        self
    }

    /// Read the next line into `buf`. Returns the number of raw bytes read
    /// (including the line terminator) or 0 on EOF.
    /// Strips trailing `\r?\n`. Empty or whitespace-only lines are returned
    /// as empty strings.
    ///
    /// Returns `io::ErrorKind::InvalidData` if the line exceeds the
    /// configured `max_line_bytes`.
    pub fn read_line(&mut self, buf: &mut String) -> io::Result<usize> {
        read_line_capped(&mut self.rdr, buf, self.max_line_bytes, &self.path)
    }
}

/// Marker payload for a *record-level* invalid line surfaced by
/// [`read_line_capped`]: either the per-line byte cap was exceeded or the line
/// was not valid UTF-8. Both cases are wrapped in an `io::Error` with
/// `io::ErrorKind::InvalidData` and this type as the inner error.
///
/// The zstd line streamer (`for_each_line_attempt`) downcasts an `io::Error`'s
/// payload to this type to tell a record-level problem apart from a genuine
/// zstd-frame decode error. Without the marker, an invalid-UTF-8 line was
/// misclassified as a tolerated corrupt-frame skip under
/// `PartialReadPolicy::AllowPartial`; with it, both the cap and the UTF-8
/// violation stay fatal regardless of partial-read policy. `Display` forwards
/// the human-readable message verbatim, so `io::Error::to_string()` is
/// unchanged from a plain `io::Error::new(InvalidData, msg)`.
#[derive(Debug)]
pub(crate) struct InvalidLineError(String);

impl InvalidLineError {
    /// Wrap `msg` as an `InvalidData` `io::Error` carrying this marker.
    fn io_error(msg: String) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, InvalidLineError(msg))
    }
}

impl std::fmt::Display for InvalidLineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for InvalidLineError {}

/// Read one line from `reader` into `buf`, enforcing `max_bytes` as a hard
/// upper bound on the raw line length (terminator included).
///
/// Trailing `\r?\n` is stripped from `buf`. Returns the number of raw bytes
/// consumed (0 on EOF). A cap violation **or** an invalid-UTF-8 line returns
/// `io::ErrorKind::InvalidData` with an `InvalidLineError` payload and a
/// message naming the offending path; on a cap violation the reader position
/// is left at the bytes that would have overflowed (callers should abort the
/// file rather than try to re-sync, because the next bytes are still part of
/// the same oversized line).
pub fn read_line_capped<R: BufRead>(
    reader: &mut R,
    buf: &mut String,
    max_bytes: usize,
    path: &Path,
) -> io::Result<usize> {
    // Reuse the caller's `String` allocation as the byte accumulator instead
    // of allocating a fresh `Vec` per line: take its backing buffer out, fill
    // and validate it, then hand it straight back. `into_bytes` and
    // `String::from_utf8` both move the allocation without copying, so a
    // caller looping over a file pays no per-line allocation.
    let mut bytes: Vec<u8> = std::mem::take(buf).into_bytes();
    bytes.clear();
    loop {
        let available = match reader.fill_buf() {
            Ok(b) => b,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        if available.is_empty() {
            break;
        }
        let (take, done) = match available.iter().position(|&b| b == b'\n') {
            Some(i) => (i + 1, true),
            None => (available.len(), false),
        };
        if bytes.len().saturating_add(take) > max_bytes {
            return Err(InvalidLineError::io_error(format!(
                "JSONL line in {} exceeds max_line_bytes={} (read {} so far; next chunk would push to {}). \
                 Bump NdjsonReader::with_max_line_bytes or raise the caller's per-line cap to accept larger records.",
                path.display(),
                max_bytes,
                bytes.len(),
                bytes.len().saturating_add(take)
            )));
        }
        bytes.extend_from_slice(&available[..take]);
        reader.consume(take);
        if done {
            break;
        }
    }
    if bytes.is_empty() {
        // `buf` already holds the empty `String` left by `mem::take`.
        return Ok(0);
    }
    let raw_len = bytes.len();
    // Invalid UTF-8 is a record-level fault, not zstd-frame corruption: mark it
    // with `InvalidLineError` (same as the cap violation above) so the zstd
    // streamer classifies it as a fatal `InvalidLine` rather than a tolerated
    // decode skip.
    *buf = String::from_utf8(bytes).map_err(|e| {
        InvalidLineError::io_error(format!(
            "JSONL line in {} is not valid UTF-8: {}",
            path.display(),
            e
        ))
    })?;
    if buf.ends_with('\n') {
        buf.pop();
        if buf.ends_with('\r') {
            buf.pop();
        }
    }
    Ok(raw_len)
}

/// Minimal NDJSON writer with buffering and robust file creation.
/// You are responsible for writing `\n` terminators.
pub struct NdjsonWriter {
    path: PathBuf,
    w: Option<BufWriter<File>>,
}

impl NdjsonWriter {
    pub fn create(path: &Path, buf_bytes: usize) -> io::Result<Self> {
        let f = crate::util::create_with_default_backoff(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            w: Some(BufWriter::with_capacity(buf_bytes.max(8 * 1024), f)),
        })
    }

    #[inline]
    pub fn write_line(&mut self, s: &str) -> io::Result<()> {
        if let Some(w) = &mut self.w {
            w.write_all(s.as_bytes())?;
            w.write_all(b"\n")?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> io::Result<()> {
        if let Some(mut w) = self.w.take() {
            w.flush()?;
        }
        Ok(())
    }

    /// Flushes and atomically promotes the temp file to `final_path`.
    /// Use when the writer was created on a temp location.
    pub fn finish_atomic(mut self, final_path: &Path) -> Result<()> {
        if let Some(mut w) = self.w.take() {
            w.flush()
                .with_context(|| format!("flush {}", self.path.display()))?;
        }
        replace_file_atomic_backoff(&self.path, final_path)
    }
}

/// Stream a plain JSONL file line-by-line, calling `on_line` with each raw line
/// (trailing `\r?\n` stripped, empty lines included).
///
/// Mirrors `zstd_jsonl::for_each_line_cfg` swallow-and-warn semantics for
/// transient read failures: opens via `open_with_backoff`, and a mid-file
/// `read_line` error is logged at warn level and ends iteration without
/// aborting the caller. Returns `Ok(Some(io_error))` when a read error was
/// tolerated (so the file was only partially consumed) and `Ok(None)` on a
/// clean read, letting callers (e.g. aggregator shard build) decide whether
/// to drop or merge partial input.
///
/// File-open errors and `on_line` errors are propagated to the caller; only
/// per-line I/O errors are swallowed and surfaced in the returned `Option`.
/// Each line is bounded by [`DEFAULT_MAX_LINE_BYTES`]; a line that exceeds
/// the cap is treated as a tolerated mid-file read error so a single
/// oversized record cannot OOM the worker.
pub fn for_each_jsonl_line_cfg(
    path: &Path,
    read_buf_bytes: usize,
    mut on_line: impl FnMut(&str) -> Result<()>,
) -> Result<Option<io::Error>> {
    let f = crate::util::open_with_default_backoff(path)?;
    let mut reader = BufReader::with_capacity(read_buf_bytes.max(8 * 1024), f);
    let mut buf = String::with_capacity(16 * 1024);
    let mut read_error = None;
    loop {
        match read_line_capped(&mut reader, &mut buf, DEFAULT_MAX_LINE_BYTES, path) {
            Ok(0) => break,
            Ok(_) => {
                on_line(&buf)?;
            }
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "tolerated JSONL read error mid-file; skipping rest of file"
                );
                read_error = Some(e);
                break;
            }
        }
    }
    Ok(read_error)
}
