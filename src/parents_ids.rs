use crate::parents::ParentIds;
use crate::pipeline::RedditETL;
use crate::progress::make_progress_bar_labeled;
use crate::shard_common;
use crate::util::with_thread_pool;
use crate::zstd_jsonl::malformed_json_error;
use ahash::{AHashSet, RandomState};
use anyhow::{Context, Result};
use rayon::prelude::*;
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

static PARENT_ID_SCRATCH_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub(crate) struct IdScratchRoot {
    path: PathBuf,
}

impl IdScratchRoot {
    fn create(work_dir: &Path) -> Result<Arc<Self>> {
        let parent = work_dir.join("parent_ids");
        fs::create_dir_all(&parent)
            .with_context(|| format!("create parent-id scratch parent {}", parent.display()))?;

        let pid = std::process::id();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let counter = PARENT_ID_SCRATCH_COUNTER.fetch_add(1, Ordering::Relaxed);

        for attempt in 0..1024usize {
            let path = parent.join(format!("run-p{pid}-{nanos:x}-{counter:x}-{attempt:x}"));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Arc::new(Self { path })),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => {
                    return Err(e).with_context(|| {
                        format!("create parent-id scratch root {}", path.display())
                    });
                }
            }
        }

        Err(anyhow::anyhow!(
            "could not allocate a unique parent-id scratch root under {}",
            parent.display()
        ))
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for IdScratchRoot {
    fn drop(&mut self) {
        if let Err(e) = fs::remove_dir_all(&self.path) {
            if e.kind() != io::ErrorKind::NotFound {
                tracing::debug!(
                    path = %self.path.display(),
                    error = %e,
                    "failed to remove parent-id scratch root"
                );
            }
        }
    }
}

pub(crate) struct IdShardWriter {
    scratch_root: Arc<IdScratchRoot>,
    base_dir: PathBuf,
    count: usize,
    rs: RandomState,
    kind: &'static str,
    writers: Vec<parking_lot::Mutex<BufWriter<File>>>,
}
impl IdShardWriter {
    pub(crate) fn create(
        scratch_root: Arc<IdScratchRoot>,
        kind: &'static str,
        count: usize,
    ) -> Result<Self> {
        let dir = scratch_root.path().join(format!("{kind}_ids_shards"));
        fs::create_dir_all(&dir)?;
        let mut writers = Vec::with_capacity(count);
        for i in 0..count {
            let p = dir.join(format!("{kind}_ids_{:04}.tmp", i));
            writers.push(parking_lot::Mutex::new(BufWriter::new(File::create(p)?)));
        }
        let rs = shard_common::seeded_state("parent_ids");
        Ok(Self {
            scratch_root,
            base_dir: dir,
            count,
            rs,
            kind,
            writers,
        })
    }
    #[inline]
    fn idx(&self, id: &str) -> usize {
        shard_common::shard_index(&self.rs, id, self.count)
    }
    #[inline]
    pub(crate) fn write(&self, id: &str) -> Result<()> {
        let i = self.idx(id);
        let mut w = self.writers[i].lock();
        w.write_all(id.as_bytes())?;
        w.write_all(b"\n")?;
        Ok(())
    }
    fn flush(&self) -> Result<()> {
        for w in &self.writers {
            w.lock().flush()?;
        }
        Ok(())
    }
    pub(crate) fn dedup(self) -> Result<IdShards> {
        self.flush()?;
        let IdShardWriter {
            scratch_root,
            base_dir,
            count,
            rs,
            kind,
            writers,
        } = self;
        drop(writers);

        let out_dir = base_dir.parent().unwrap().join(format!("{kind}_ids_dedup"));
        fs::create_dir_all(&out_dir)?;

        let tmp_paths: Vec<PathBuf> = (0..count)
            .map(|i| base_dir.join(format!("{kind}_ids_{:04}.tmp", i)))
            .collect();
        tmp_paths.par_iter().try_for_each(|p| -> Result<()> {
            let out = out_dir.join(
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .replace(".tmp", ".txt"),
            );
            dedup_one(p, &out)?;
            Ok(())
        })?;

        if let Err(e) = fs::remove_dir_all(&base_dir) {
            if e.kind() != io::ErrorKind::NotFound {
                tracing::debug!(
                    path = %base_dir.display(),
                    error = %e,
                    "failed to remove parent-id raw shard scratch"
                );
            }
        }

        Ok(IdShards {
            dir: out_dir,
            count,
            rs,
            kind: kind.to_string(),
            _scratch_root: scratch_root,
        })
    }
}

pub(crate) struct IdShards {
    pub(crate) dir: PathBuf,
    pub(crate) count: usize,
    pub(crate) rs: RandomState,
    pub(crate) kind: String,
    pub(crate) _scratch_root: Arc<IdScratchRoot>,
}
impl IdShards {
    #[inline]
    pub(crate) fn idx(&self, id: &str) -> usize {
        shard_common::shard_index(&self.rs, id, self.count)
    }
    #[inline]
    pub(crate) fn path_for(&self, idx: usize) -> PathBuf {
        self.dir.join(format!("{}_ids_{:04}.txt", self.kind, idx))
    }
}

fn dedup_one(input: &Path, output: &Path) -> Result<()> {
    let f = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut r = BufReader::new(f);
    let mut set: AHashSet<String> = AHashSet::with_capacity(64_000);

    let mut buf = String::with_capacity(16 * 1024);
    loop {
        buf.clear();
        let n = r.read_line(&mut buf)?;
        if n == 0 {
            break;
        }
        if buf.ends_with('\n') {
            buf.pop();
            if buf.ends_with('\r') {
                buf.pop();
            }
        }
        if !buf.is_empty() {
            set.insert(buf.clone());
        }
    }

    let out = File::create(output)?;
    let mut w = BufWriter::new(out);
    for s in set {
        w.write_all(s.as_bytes())?;
        w.write_all(b"\n")?;
    }
    w.flush()?;
    Ok(())
}

/// Per-rayon-worker FIFO cache of parsed parent shard JSON files.
///
/// Eviction is plain FIFO with **no bump-on-hit** — same as the prior inline
/// implementation. Bumping on hit would require an `O(n)` `VecDeque` rescan
/// and is unnecessary in practice because shard load order is dominated by
/// own-month locality (the same shards stay hot naturally).
pub(crate) struct WorkerShardCache<V> {
    cache: HashMap<PathBuf, HashMap<String, V>>,
    order: VecDeque<PathBuf>,
    cap: usize,
}

impl<V: serde::de::DeserializeOwned + Clone> WorkerShardCache<V> {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            cache: HashMap::new(),
            order: VecDeque::new(),
            cap,
        }
    }

    pub(crate) fn get(&mut self, path: &Path, id: &str) -> Result<Option<V>> {
        if !self.cache.contains_key(path) {
            if self.cache.len() >= self.cap {
                if let Some(old) = self.order.pop_front() {
                    self.cache.remove(&old);
                }
            }
            let file = File::open(path)
                .with_context(|| format!("open parent shard {}", path.display()))?;
            let rdr = BufReader::new(file);
            let map: HashMap<String, V> = serde_json::from_reader(rdr)
                .with_context(|| format!("parse parent shard {}", path.display()))?;
            self.cache.insert(path.to_path_buf(), map);
            self.order.push_back(path.to_path_buf());
        }
        Ok(self.cache.get(path).and_then(|m| m.get(id).cloned()))
    }
}

/// Process-wide cache for membership-id shards (set of t1_/t3_ ids per shard
/// file). Shared across rayon workers so each shard is read+parsed at most
/// once globally rather than once per worker.
pub(crate) struct SharedIdsetCache {
    inner: parking_lot::RwLock<SharedIdsetState>,
    cap: usize,
}

struct SharedIdsetState {
    map: HashMap<PathBuf, Arc<AHashSet<String>>>,
    order: VecDeque<PathBuf>,
}

impl SharedIdsetCache {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            inner: parking_lot::RwLock::new(SharedIdsetState {
                map: HashMap::new(),
                order: VecDeque::new(),
            }),
            cap: cap.max(1),
        }
    }

    pub(crate) fn get_or_load(&self, path: &Path) -> Result<Arc<AHashSet<String>>> {
        if let Some(v) = self.inner.read().map.get(path) {
            return Ok(v.clone());
        }
        // Load outside the lock so concurrent workers loading different shards
        // don't serialize on the I/O.
        let f = File::open(path).with_context(|| format!("open idset shard {}", path.display()))?;
        let r = BufReader::new(f);
        let mut set: AHashSet<String> = AHashSet::with_capacity(64_000);
        for line in r.lines() {
            let mut s = line.with_context(|| format!("read idset shard {}", path.display()))?;
            if s.ends_with('\r') {
                s.pop();
            }
            if !s.is_empty() {
                set.insert(s);
            }
        }
        let arc = Arc::new(set);

        let mut g = self.inner.write();
        // Another worker may have raced us to load the same shard; prefer
        // their copy so callers don't see different Arc identities.
        if let Some(existing) = g.map.get(path) {
            return Ok(existing.clone());
        }
        if g.map.len() >= self.cap {
            if let Some(old) = g.order.pop_front() {
                g.map.remove(&old);
            }
        }
        g.map.insert(path.to_path_buf(), arc.clone());
        g.order.push_back(path.to_path_buf());
        Ok(arc)
    }
}

impl RedditETL {
    pub fn collect_parent_ids_from_jsonls<I>(&self, jsonl_paths: I) -> Result<ParentIds>
    where
        I: IntoIterator<Item = PathBuf>,
    {
        let paths: Vec<PathBuf> = jsonl_paths.into_iter().collect();
        if paths.is_empty() {
            return Ok(ParentIds::new());
        }

        with_thread_pool(self.opts.parallelism, || {
            let work_dir = self.ensure_work_dir()?;
            let scratch_root = IdScratchRoot::create(&work_dir)?;

            // Fewer shards reduces disk thrash later; we keep a wider FIFO in memory.
            let shard_count = 256usize;
            let t1_writer = IdShardWriter::create(scratch_root.clone(), "t1", shard_count)?;
            let t3_writer = IdShardWriter::create(scratch_root, "t3", shard_count)?;

            let total_bytes: u64 = paths
                .iter()
                .map(|p| fs::metadata(p).map(|m| m.len()).unwrap_or(0))
                .sum();
            let pb = if self.opts.progress {
                Some(make_progress_bar_labeled(
                    total_bytes,
                    self.opts.progress_label.as_deref(),
                ))
            } else {
                None
            };

            let read_buf = self.opts.read_buffer_bytes;

            let parent_ref_count = AtomicUsize::new(0);

            paths.par_iter().try_for_each(|p| -> Result<()> {
                let f = File::open(p)
                    .with_context(|| format!("open parent-id spool input {}", p.display()))?;
                let mut r = BufReader::with_capacity(read_buf, f);
                let mut buf = String::with_capacity(64 * 1024);
                let mut line_number = 0u64;
                loop {
                    buf.clear();
                    let n = r.read_line(&mut buf).with_context(|| {
                        format!(
                            "read parent-id spool input {} near line {}",
                            p.display(),
                            line_number + 1
                        )
                    })?;
                    if n == 0 {
                        break;
                    }
                    line_number += 1;
                    if buf.ends_with('\n') {
                        buf.pop();
                        if buf.ends_with('\r') {
                            buf.pop();
                        }
                    }
                    if buf.is_empty() {
                        if let Some(pb) = &pb {
                            pb.inc(n as u64);
                        }
                        continue;
                    }

                    let v: Value = serde_json::from_str(&buf)
                        .map_err(|e| malformed_json_error(p, line_number, e))?;
                    if let Some(parent_id) = v.get("parent_id").and_then(|x| x.as_str()) {
                        parent_ref_count.fetch_add(1, Ordering::Relaxed);
                        if let Some(rest) = parent_id.strip_prefix("t1_") {
                            t1_writer.write(rest)?;
                        } else if let Some(rest) = parent_id.strip_prefix("t3_") {
                            t3_writer.write(rest)?;
                        }
                    }
                    if let Some(link_id) = v.get("link_id").and_then(|x| x.as_str()) {
                        parent_ref_count.fetch_add(1, Ordering::Relaxed);
                        if let Some(rest) = link_id.strip_prefix("t3_") {
                            t3_writer.write(rest)?;
                        }
                    }

                    if let Some(pb) = &pb {
                        pb.inc(n as u64);
                    }
                }
                Ok(())
            })?;

            let t1_shards = t1_writer.dedup()?;
            let t3_shards = t3_writer.dedup()?;

            if let Some(pb) = pb {
                let final_msg = if let Some(l) = self.opts.progress_label.as_deref() {
                    format!("{l} done")
                } else {
                    "done".to_string()
                };
                pb.finish_with_message(final_msg);
            }

            let parent_refs = parent_ref_count.load(Ordering::Relaxed);
            if parent_refs == 0 {
                tracing::warn!(
                    input_files = paths.len(),
                    "collect_parent_ids_from_jsonls found zero parent_id/link_id fields across the spool; parents pipeline requires parent_id and link_id to survive any --whitelist/.whitelist_fields"
                );
            }

            Ok(ParentIds::from_shards(t1_shards, t3_shards))
        })
    }
}
