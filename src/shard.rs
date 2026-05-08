use ahash::RandomState;
use anyhow::{Context, Result};
use parking_lot::Mutex;
use rayon::prelude::*;
use std::collections::HashSet;
use std::fs::{self, File};
use std::hash::{BuildHasher, Hash, Hasher};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

/// Disk-backed sharded dedup writer: concurrent-safe.
pub struct ShardedWriter {
    base_dir: PathBuf,
    shards: Vec<Mutex<BufWriter<File>>>,
    count: usize,
    state: RandomState, // seeded for deterministic sharding
}

impl ShardedWriter {
    pub fn create(work_dir: &Path, prefix: &str, count: usize) -> Result<Self> {
        let count = count.max(1);
        let shards_dir = work_dir.join(format!("{prefix}_shards"));
        fs::create_dir_all(&shards_dir)?;

        let mut shards = Vec::with_capacity(count);
        for i in 0..count {
            let path = shards_dir.join(format!("shard_{:04}.tmp", i));
            let file = File::create(path)?;
            shards.push(Mutex::new(BufWriter::new(file)));
        }

        let state = RandomState::with_seeds(
            0x1234_5678_9abc_def0,
            0x0fed_cba9_8765_4321,
            0xdead_beef_cafe_babe,
            0x0bad_f00d_face_feed,
        );

        Ok(Self {
            base_dir: shards_dir,
            shards,
            count,
            state,
        })
    }

    #[inline]
    fn shard_index(&self, s: &str) -> usize {
        let mut hasher = self.state.build_hasher();
        s.hash(&mut hasher);
        (hasher.finish() as usize) % self.count
    }

    pub fn write(&self, key: &str) -> Result<()> {
        let idx = self.shard_index(key);
        let mut guard = self.shards[idx].lock();
        guard.write_all(key.as_bytes())?;
        guard.write_all(b"\n")?;
        Ok(())
    }

    pub fn flush_all(&self) -> Result<()> {
        for w in &self.shards {
            w.lock().flush()?;
        }
        Ok(())
    }

    /// Deduplicate each shard independently and return ordered deduped files.
    pub fn dedup(self, prefix: &str) -> Result<Vec<PathBuf>> {
        self.flush_all()?;
        drop(self.shards); // ensure writers are closed

        let dedup_dir = self.base_dir.parent().unwrap().join(format!("{prefix}_dedup"));
        fs::create_dir_all(&dedup_dir)?;

        let shard_paths: Vec<PathBuf> = (0..self.count)
            .map(|i| self.base_dir.join(format!("shard_{:04}.tmp", i)))
            .collect();

        let deduped: Vec<PathBuf> = shard_paths
            .par_iter()
            .map(|shard_path| -> Result<PathBuf> {
                let out_path = dedup_dir.join(
                    shard_path.file_name().unwrap().to_string_lossy().replace(".tmp", ".txt")
                );
                dedup_single_shard(shard_path, &out_path)?;
                Ok(out_path)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(deduped)
    }
}

fn dedup_single_shard(input: &Path, output: &Path) -> Result<()> {
    let in_file = File::open(input)
        .with_context(|| format!("open shard for dedup: {}", input.display()))?;
    let mut reader = BufReader::new(in_file);

    let out_file = File::create(output)
        .with_context(|| format!("create dedup output: {}", output.display()))?;
    let mut writer = BufWriter::new(out_file);

    let mut seen: HashSet<String> = HashSet::with_capacity(64_000);

    let mut buf = String::with_capacity(8 * 1024);
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
        if !buf.is_empty() {
            if seen.insert(buf.clone()) {
                writer.write_all(buf.as_bytes())?;
                writer.write_all(b"\n")?;
            }
        }
    }
    writer.flush()?;
    Ok(())
}

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
    current_idx: usize,
    reader: Option<BufReader<File>>,
    buf: String,
    current_file_errors: usize,
}

impl UsernameStream {
    pub fn from_deduped_files(mut files: Vec<PathBuf>) -> Result<Self> {
        files.sort();
        Ok(Self {
            files,
            current_idx: 0,
            reader: None,
            buf: String::with_capacity(8 * 1024),
            current_file_errors: 0,
        })
    }

    fn open_next(&mut self) -> Result<bool> {
        if self.current_idx >= self.files.len() {
            return Ok(false);
        }
        let f = File::open(&self.files[self.current_idx])?;
        self.reader = Some(BufReader::new(f));
        self.current_idx += 1;
        self.current_file_errors = 0;
        Ok(true)
    }

    fn step<R: BufRead>(buf: &mut String, errors: &mut usize, reader: &mut R) -> ReadStep {
        buf.clear();
        match reader.read_line(buf) {
            Ok(0) => ReadStep::Eof,
            Ok(_) => {
                if buf.ends_with('\n') {
                    let _ = buf.pop();
                    if buf.ends_with('\r') { let _ = buf.pop(); }
                }
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

impl Iterator for UsernameStream {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.reader.is_none() {
                if self.open_next().ok()? == false {
                    return None;
                }
            }
            if let Some(reader) = &mut self.reader {
                match Self::step(&mut self.buf, &mut self.current_file_errors, reader) {
                    ReadStep::Yielded(s) => return Some(s),
                    ReadStep::Empty => continue,
                    ReadStep::Eof => {
                        self.reader = None;
                        continue;
                    }
                    ReadStep::Retry => continue,
                    ReadStep::GiveUp => {
                        // open_next post-increments current_idx, so the file we
                        // just exhausted retries on is at current_idx - 1.
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{self, Cursor, Read};
    use tempfile::tempdir;

    /// A `Read` impl that always errors. Wrapped in `BufReader` to drive
    /// `UsernameStream::step` down the `Err(_)` branch deterministically.
    struct ErroringReader;
    impl Read for ErroringReader {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::Other, "synthetic read failure"))
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
