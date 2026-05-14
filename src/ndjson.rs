use crate::util::{create_with_backoff, open_with_backoff, replace_file_atomic_backoff};
use anyhow::{Context, Result};
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

/// Minimal NDJSON reader with buffering and empty-line trimming.
/// Uses robust open-with-backoff for Windows-friendliness.
pub struct NdjsonReader {
    rdr: BufReader<File>,
}

impl NdjsonReader {
    pub fn open(path: &Path, buf_bytes: usize) -> io::Result<Self> {
        let f = open_with_backoff(path, 16, 50)?;
        Ok(Self { rdr: BufReader::with_capacity(buf_bytes.max(8 * 1024), f) })
    }

    /// Read the next line into `buf`. Returns the number of bytes read (0 on EOF).
    /// Strips trailing `\r?\n`. Empty or whitespace-only lines are returned as empty strings.
    pub fn read_line(&mut self, buf: &mut String) -> io::Result<usize> {
        buf.clear();
        let n = self.rdr.read_line(buf)?;
        if n == 0 { return Ok(0); }
        if buf.ends_with('\n') {
            buf.pop();
            if buf.ends_with('\r') { buf.pop(); }
        }
        Ok(n)
    }
}

/// Minimal NDJSON writer with buffering and robust file creation.
/// You are responsible for writing `\n` terminators.
pub struct NdjsonWriter {
    path: PathBuf,
    w: Option<BufWriter<File>>,
}

impl NdjsonWriter {
    pub fn create(path: &Path, buf_bytes: usize) -> io::Result<Self> {
        let f = create_with_backoff(path, 16, 50)?;
        Ok(Self { path: path.to_path_buf(), w: Some(BufWriter::with_capacity(buf_bytes.max(8 * 1024), f)) })
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
            w.flush().with_context(|| format!("flush {}", self.path.display()))?;
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
pub fn for_each_jsonl_line_cfg(
    path: &Path,
    read_buf_bytes: usize,
    mut on_line: impl FnMut(&str) -> Result<()>,
) -> Result<Option<io::Error>> {
    let f = open_with_backoff(path, 16, 50)?;
    let mut reader = BufReader::with_capacity(read_buf_bytes.max(8 * 1024), f);
    let mut buf = String::with_capacity(16 * 1024);
    let mut read_error = None;
    loop {
        buf.clear();
        match reader.read_line(&mut buf) {
            Ok(0) => break,
            Ok(_) => {
                if buf.ends_with('\n') {
                    buf.pop();
                    if buf.ends_with('\r') { buf.pop(); }
                }
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
