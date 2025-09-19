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
