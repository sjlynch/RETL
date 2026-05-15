use crate::ndjson::{read_line_capped, DEFAULT_MAX_LINE_BYTES};
use crate::util::{open_with_backoff, remove_dir_all_with_backoff};
use anyhow::Result;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

/// Maximum consecutive read errors tolerated on a single shard file before
/// `UsernameStream` gives up on it and advances to the next file. Prevents the
/// previous behavior where a hard read error spun the iterator forever.
const MAX_READ_RETRIES_PER_FILE: usize = 3;

#[derive(Debug)]
enum ReadStep {
    Yielded(String),
    Empty,
    Eof,
    Retry,
    GiveUp,
}

/// A streaming merger that yields deduped usernames from deduped shards.
pub struct UsernameStream {
    files: Vec<PathBuf>,
    cleanup_roots: Vec<PathBuf>,
    current_idx: usize,
    reader: Option<BufReader<File>>,
    buf: String,
    current_file_errors: usize,
}

impl UsernameStream {
    pub fn from_deduped_files(files: Vec<PathBuf>) -> Result<Self> {
        Self::from_deduped_files_with_cleanup(files, Vec::new())
    }

    pub(crate) fn from_deduped_files_with_cleanup(
        mut files: Vec<PathBuf>,
        cleanup_roots: Vec<PathBuf>,
    ) -> Result<Self> {
        files.sort();
        Ok(Self {
            files,
            cleanup_roots,
            current_idx: 0,
            reader: None,
            buf: String::with_capacity(8 * 1024),
            current_file_errors: 0,
        })
    }

    fn cleanup_scratch(&mut self) {
        self.reader = None;
        for root in std::mem::take(&mut self.cleanup_roots) {
            if let Err(e) = remove_dir_all_with_backoff(&root, 8, 50) {
                tracing::warn!(path=%root.display(), error=%e, "UsernameStream: failed to remove scratch dir");
            }
        }
    }

    /// Try to open the next file. Advances `current_idx` regardless of outcome,
    /// so a failure yields `Some(Err(_))` once and a subsequent call moves on
    /// to the file after the bad one instead of looping on the same path.
    fn open_next(&mut self) -> Option<Result<()>> {
        if self.current_idx >= self.files.len() {
            return None;
        }
        let path = self.files[self.current_idx].clone();
        self.current_idx += 1;
        match open_with_backoff(&path, 16, 50) {
            Ok(f) => {
                self.reader = Some(BufReader::new(f));
                self.current_file_errors = 0;
                Some(Ok(()))
            }
            Err(e) => Some(Err(anyhow::Error::from(e)
                .context(format!("open shard for streaming: {}", path.display())))),
        }
    }

    /// Yield the next username, surfacing per-file open failures to the caller.
    ///
    /// `None` is a clean end-of-stream. `Some(Err(_))` means opening the next
    /// shard failed; the stream has already advanced past it, so calling
    /// `try_next` again attempts the file after it. The lossy
    /// `Iterator::next` impl wraps this method and logs+continues on errors.
    pub fn try_next(&mut self) -> Option<Result<String>> {
        loop {
            if self.reader.is_none() {
                match self.open_next() {
                    None => {
                        self.cleanup_scratch();
                        return None;
                    }
                    Some(Ok(())) => {}
                    Some(Err(e)) => return Some(Err(e)),
                }
            }
            if let Some(reader) = &mut self.reader {
                match Self::step(&mut self.buf, &mut self.current_file_errors, reader) {
                    ReadStep::Yielded(s) => return Some(Ok(s)),
                    ReadStep::Empty => continue,
                    ReadStep::Eof => {
                        self.reader = None;
                        continue;
                    }
                    ReadStep::Retry => continue,
                    ReadStep::GiveUp => {
                        // open_next advanced current_idx already, so the file
                        // we just exhausted retries on is at current_idx - 1.
                        let path = self
                            .files
                            .get(self.current_idx.saturating_sub(1))
                            .map(|p| p.display().to_string())
                            .unwrap_or_else(|| "<unknown>".to_string());
                        tracing::warn!(
                            path = %path,
                            attempts = self.current_file_errors,
                            "UsernameStream: read errors exceeded retry bound; advancing to next file",
                        );
                        self.reader = None;
                        continue;
                    }
                }
            }
        }
    }

    fn step<R: BufRead>(buf: &mut String, errors: &mut usize, reader: &mut R) -> ReadStep {
        match read_line_capped(
            reader,
            buf,
            DEFAULT_MAX_LINE_BYTES,
            Path::new("username shard"),
        ) {
            Ok(0) => ReadStep::Eof,
            Ok(_) => {
                if buf.is_empty() {
                    ReadStep::Empty
                } else {
                    ReadStep::Yielded(buf.clone())
                }
            }
            Err(_) => {
                *errors += 1;
                if *errors >= MAX_READ_RETRIES_PER_FILE {
                    ReadStep::GiveUp
                } else {
                    ReadStep::Retry
                }
            }
        }
    }
}

impl Drop for UsernameStream {
    fn drop(&mut self) {
        self.cleanup_scratch();
    }
}

impl Iterator for UsernameStream {
    type Item = String;

    /// Lossy convenience layer over [`UsernameStream::try_next`]: on per-file
    /// open failures, log a warning and advance to the next file. Read errors
    /// are already logged and skipped inside `try_next`.
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.try_next()? {
                Ok(s) => return Some(s),
                Err(e) => {
                    // `{:#}` walks the anyhow context chain so the path added
                    // in `open_next` is included alongside the I/O error.
                    let chained = format!("{:#}", e);
                    tracing::warn!(
                        error = %chained,
                        "UsernameStream: open failed; advancing to next file",
                    );
                    continue;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::{self, Cursor, Read};
    use tempfile::tempdir;

    /// A `Read` impl that always errors. Wrapped in `BufReader` to drive
    /// `UsernameStream::step` down the `Err(_)` branch deterministically.
    struct ErroringReader;
    impl Read for ErroringReader {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "synthetic read failure",
            ))
        }
    }

    #[test]
    fn streams_lines_in_order_across_files() {
        let dir = tempdir().unwrap();
        let p1 = dir.path().join("shard_0000.txt");
        let p2 = dir.path().join("shard_0001.txt");
        fs::write(&p1, "alice\nbob\n").unwrap();
        fs::write(&p2, "carol\n\ndave\r\n").unwrap();

        let stream = UsernameStream::from_deduped_files(vec![p2.clone(), p1.clone()]).unwrap();
        let got: Vec<String> = stream.collect();
        // Files are sorted on construction, so shard_0000 comes first, and
        // empty lines are skipped; trailing \r\n is stripped.
        assert_eq!(got, vec!["alice", "bob", "carol", "dave"]);
    }

    #[test]
    fn step_yields_then_eofs() {
        let mut reader = Cursor::new(b"hello\nworld\n".to_vec());
        let mut buf = String::new();
        let mut errors = 0usize;

        match UsernameStream::step(&mut buf, &mut errors, &mut reader) {
            ReadStep::Yielded(s) => assert_eq!(s, "hello"),
            other => panic!("expected Yielded, got {:?}", other),
        }
        match UsernameStream::step(&mut buf, &mut errors, &mut reader) {
            ReadStep::Yielded(s) => assert_eq!(s, "world"),
            other => panic!("expected Yielded, got {:?}", other),
        }
        match UsernameStream::step(&mut buf, &mut errors, &mut reader) {
            ReadStep::Eof => {}
            other => panic!("expected Eof, got {:?}", other),
        }
        assert_eq!(errors, 0, "no errors should have been counted");
    }

    #[test]
    fn lossy_iterator_skips_files_that_fail_to_open() {
        // 'a_' < 'b_' lexically, so after sorting the missing file is opened
        // first. The lossy `Iterator` impl must log+advance and still surface
        // the real file's lines instead of silently terminating.
        let dir = tempdir().unwrap();
        let missing = dir.path().join("a_missing.txt"); // never created
        let real = dir.path().join("b_real.txt");
        fs::write(&real, "alice\nbob\n").unwrap();

        let stream =
            UsernameStream::from_deduped_files(vec![real.clone(), missing.clone()]).unwrap();
        let got: Vec<String> = stream.collect();
        assert_eq!(got, vec!["alice", "bob"]);
    }

    #[test]
    fn try_next_surfaces_open_errors_and_caller_can_continue() {
        // Sandwich a missing path between two real files. `try_next` must
        // yield real lines, then surface a single Err for the missing file
        // (with the path in the context chain), then continue past it.
        let dir = tempdir().unwrap();
        let real_first = dir.path().join("a_real.txt");
        let missing = dir.path().join("b_missing.txt");
        let real_last = dir.path().join("c_real.txt");
        fs::write(&real_first, "alice\n").unwrap();
        fs::write(&real_last, "carol\n").unwrap();

        let mut stream =
            UsernameStream::from_deduped_files(vec![real_first, missing, real_last]).unwrap();

        match stream.try_next() {
            Some(Ok(s)) => assert_eq!(s, "alice"),
            other => panic!("expected Ok(\"alice\"), got {:?}", other),
        }
        match stream.try_next() {
            Some(Err(e)) => {
                let chained = format!("{:#}", e);
                assert!(
                    chained.contains("b_missing.txt"),
                    "error chain should include failing path; got: {chained}"
                );
            }
            other => panic!("expected Some(Err) for missing file, got {:?}", other),
        }
        match stream.try_next() {
            Some(Ok(s)) => assert_eq!(s, "carol"),
            other => panic!("expected Ok(\"carol\"), got {:?}", other),
        }
        match stream.try_next() {
            None => {}
            other => panic!("expected clean EOF, got {:?}", other),
        }
    }

    #[test]
    fn try_next_eventually_drains_all_files_with_only_open_errors() {
        // All files are missing — every call returns Some(Err), then None.
        // Guards against a regression where a failed open didn't advance
        // current_idx and the stream looped on the same path forever.
        let dir = tempdir().unwrap();
        let p1 = dir.path().join("missing_1.txt");
        let p2 = dir.path().join("missing_2.txt");

        let mut stream = UsernameStream::from_deduped_files(vec![p1, p2]).unwrap();
        assert!(matches!(stream.try_next(), Some(Err(_))));
        assert!(matches!(stream.try_next(), Some(Err(_))));
        assert!(stream.try_next().is_none());
    }

    #[test]
    fn step_bounds_retries_on_persistent_read_error() {
        let mut reader = BufReader::new(ErroringReader);
        let mut buf = String::new();
        let mut errors = 0usize;

        // First (MAX-1) errors should signal Retry, leaving the bound intact.
        for attempt in 1..MAX_READ_RETRIES_PER_FILE {
            match UsernameStream::step(&mut buf, &mut errors, &mut reader) {
                ReadStep::Retry => {}
                other => panic!("attempt {attempt}: expected Retry, got {:?}", other),
            }
            assert_eq!(errors, attempt);
        }

        // The MAX-th error must trip the bound and signal GiveUp so the
        // iterator advances off the bad file instead of looping forever.
        match UsernameStream::step(&mut buf, &mut errors, &mut reader) {
            ReadStep::GiveUp => {}
            other => panic!("expected GiveUp at bound, got {:?}", other),
        }
        assert_eq!(errors, MAX_READ_RETRIES_PER_FILE);
    }
}
