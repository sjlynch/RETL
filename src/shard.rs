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

/// A streaming merger that yields deduped usernames from deduped shards.
pub struct UsernameStream {
    files: Vec<PathBuf>,
    current_idx: usize,
    reader: Option<BufReader<File>>,
    buf: String,
}

impl UsernameStream {
    pub fn from_deduped_files(mut files: Vec<PathBuf>) -> Result<Self> {
        files.sort();
        Ok(Self {
            files,
            current_idx: 0,
            reader: None,
            buf: String::with_capacity(8 * 1024),
        })
    }

    fn open_next(&mut self) -> Result<bool> {
        if self.current_idx >= self.files.len() {
            return Ok(false);
        }
        let f = File::open(&self.files[self.current_idx])?;
        self.reader = Some(BufReader::new(f));
        self.current_idx += 1;
        Ok(true)
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
                self.buf.clear();
                match reader.read_line(&mut self.buf) {
                    Ok(0) => {
                        self.reader = None; // EOF; advance to next file
                        continue;
                    }
                    Ok(_) => {
                        if self.buf.ends_with('\n') {
                            let _ = self.buf.pop();
                            if self.buf.ends_with('\r') { let _ = self.buf.pop(); }
                        }
                        if !self.buf.is_empty() {
                            return Some(self.buf.clone());
                        }
                    }
                    Err(_) => continue,
                }
            }
        }
    }
}
