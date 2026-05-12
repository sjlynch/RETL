use crate::parents::ParentIds;
use crate::pipeline::RedditETL;
use crate::progress::make_progress_bar_labeled;
use crate::shard_common;
use crate::util::with_thread_pool;
use ahash::{AHashSet, RandomState};
use anyhow::{Context, Result};
use rayon::prelude::*;
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) struct IdShardWriter {
    base_dir: PathBuf,
    count: usize,
    rs: RandomState,
    kind: &'static str,
    writers: Vec<parking_lot::Mutex<BufWriter<File>>>,
}
impl IdShardWriter {
    pub(crate) fn create(base_dir: &Path, kind: &'static str, count: usize) -> Result<Self> {
        let dir = base_dir.join(format!("{kind}_ids_shards"));
        fs::create_dir_all(&dir)?;
        let mut writers = Vec::with_capacity(count);
        for i in 0..count {
            let p = dir.join(format!("{kind}_ids_{:04}.tmp", i));
            writers.push(parking_lot::Mutex::new(BufWriter::new(File::create(p)?)));
        }
        let rs = shard_common::seeded_state("parent_ids");
        Ok(Self { base_dir: dir, count, rs, kind, writers })
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
        for w in &self.writers { w.lock().flush()?; }
        Ok(())
    }
    pub(crate) fn dedup(self) -> Result<IdShards> {
        self.flush()?;
        let IdShardWriter { base_dir, count, rs, kind, writers } = self;
        drop(writers);

        let out_dir = base_dir.parent().unwrap().join(format!("{kind}_ids_dedup"));
        fs::create_dir_all(&out_dir)?;

        let tmp_paths: Vec<PathBuf> = (0..count).map(|i| base_dir.join(format!("{kind}_ids_{:04}.tmp", i))).collect();
        tmp_paths.par_iter().try_for_each(|p| -> Result<()> {
            let out = out_dir.join(p.file_name().unwrap().to_string_lossy().replace(".tmp", ".txt"));
            dedup_one(p, &out)?;
            Ok(())
        })?;

        Ok(IdShards { dir: out_dir, count, rs, kind: kind.to_string() })
    }
}

pub(crate) struct IdShards {
    pub(crate) dir: PathBuf,
    pub(crate) count: usize,
    pub(crate) rs: RandomState,
    pub(crate) kind: String,
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
        if n == 0 { break; }
        if buf.ends_with('\n') { buf.pop(); if buf.ends_with('\r') { buf.pop(); } }
        if !buf.is_empty() { set.insert(buf.clone()); }
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
        let f = File::open(path)
            .with_context(|| format!("open idset shard {}", path.display()))?;
        let r = BufReader::new(f);
        let mut set: AHashSet<String> = AHashSet::with_capacity(64_000);
        for line in r.lines() {
            let mut s = line.with_context(|| format!("read idset shard {}", path.display()))?;
            if s.ends_with('\r') { s.pop(); }
            if !s.is_empty() { set.insert(s); }
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
            let ids_root = work_dir.join("parent_ids");
            fs::create_dir_all(&ids_root)?;

            // Fewer shards reduces disk thrash later; we keep a wider FIFO in memory.
            let shard_count = 256usize;
            let t1_writer = IdShardWriter::create(&ids_root, "t1", shard_count)?;
            let t3_writer = IdShardWriter::create(&ids_root, "t3", shard_count)?;

            let total_bytes: u64 = paths.iter().map(|p| fs::metadata(p).map(|m| m.len()).unwrap_or(0)).sum();
            let pb = if self.opts.progress {
                Some(make_progress_bar_labeled(total_bytes, self.opts.progress_label.as_deref()))
            } else { None };

            let read_buf = self.opts.read_buffer_bytes;

            // Surface I/O / parse errors instead of silently dropping them. Each
            // worker writes a tracing::warn! on failure and bumps the shared
            // counter; the total is reported back via tracing at the end.
            let err_count = AtomicUsize::new(0);

            paths.par_iter().for_each(|p| {
                let res = (|| -> Result<()> {
                    let f = File::open(p).with_context(|| format!("open {}", p.display()))?;
                    let mut r = BufReader::with_capacity(read_buf, f);
                    let mut buf = String::with_capacity(64 * 1024);
                    loop {
                        buf.clear();
                        let n = r.read_line(&mut buf)
                            .with_context(|| format!("read {}", p.display()))?;
                        if n == 0 { break; }
                        if buf.ends_with('\n') { buf.pop(); if buf.ends_with('\r') { buf.pop(); } }
                        if buf.is_empty() { continue; }

                        let v: Value = match serde_json::from_str(&buf) {
                            Ok(x) => x,
                            Err(_) => {
                                // Single malformed line should not abort the file;
                                // count it but do not bail.
                                err_count.fetch_add(1, Ordering::Relaxed);
                                if let Some(pb) = &pb { pb.inc(n as u64); }
                                continue;
                            }
                        };
                        if let Some(parent_id) = v.get("parent_id").and_then(|x| x.as_str()) {
                            if let Some(rest) = parent_id.strip_prefix("t1_") {
                                t1_writer.write(rest)?;
                            } else if let Some(rest) = parent_id.strip_prefix("t3_") {
                                t3_writer.write(rest)?;
                            }
                        }
                        if let Some(link_id) = v.get("link_id").and_then(|x| x.as_str()) {
                            if let Some(rest) = link_id.strip_prefix("t3_") {
                                t3_writer.write(rest)?;
                            }
                        }

                        if let Some(pb) = &pb { pb.inc(n as u64); }
                    }
                    Ok(())
                })();

                if let Err(e) = res {
                    err_count.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(path=%p.display(), error=%e, "collect_parent_ids: skipped file due to error");
                }
            });

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

            let errors = err_count.load(Ordering::Relaxed);
            if errors > 0 {
                tracing::warn!(error_count = errors, "collect_parent_ids_from_jsonls completed with errors");
            }

            Ok(ParentIds::from_shards(t1_shards, t3_shards))
        })
    }
}
