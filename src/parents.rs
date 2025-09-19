use crate::paths::{discover_all, plan_files, FileKind};
use crate::progress::{make_count_progress, make_progress_bar_labeled, total_compressed_size};
use crate::util::init_tracing_once;
use crate::pipeline::RedditETL;
use crate::mem::{available_memory_fraction, is_low_memory};
use crate::zstd_jsonl::{
    for_each_line_with_progress_cfg_no_throttle,
    parse_minimal,
};
use anyhow::{Context, Result};
use rayon::prelude::*;
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::fs::File;
use std::hash::{BuildHasher, Hash, Hasher};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use ahash::{AHashSet, RandomState};

struct IdShardWriter {
    base_dir: PathBuf,
    count: usize,
    rs: RandomState,
    kind: &'static str,
    writers: Vec<parking_lot::Mutex<BufWriter<File>>>,
}
impl IdShardWriter {
    fn create(base_dir: &Path, kind: &'static str, count: usize) -> Result<Self> {
        let dir = base_dir.join(format!("{kind}_ids_shards"));
        fs::create_dir_all(&dir)?;
        let mut writers = Vec::with_capacity(count);
        for i in 0..count {
            let p = dir.join(format!("{kind}_ids_{:04}.tmp", i));
            writers.push(parking_lot::Mutex::new(BufWriter::new(File::create(p)?)));
        }
        let rs = RandomState::with_seeds(
            0x2200_1100_3300_4400,
            0x5500_6600_7700_8800,
            0x9900_aa00_bb00_cc00,
            0xdd00_ee00_ff00_0123,
        );
        Ok(Self { base_dir: dir, count, rs, kind, writers })
    }
    #[inline]
    fn idx(&self, id: &str) -> usize {
        let mut h = self.rs.build_hasher();
        id.hash(&mut h);
        (h.finish() as usize) % self.count
    }
    #[inline]
    fn write(&self, id: &str) -> Result<()> {
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
    fn dedup(self) -> Result<IdShards> {
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

struct IdShards {
    dir: PathBuf,
    count: usize,
    rs: RandomState,
    kind: String,
}
impl IdShards {
    #[inline]
    fn idx(&self, id: &str) -> usize {
        let mut h = self.rs.build_hasher();
        id.hash(&mut h);
        (h.finish() as usize) % self.count
    }
    #[inline]
    fn path_for(&self, idx: usize) -> PathBuf {
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

pub struct ParentIds {
    // Private fields to avoid exposing private types and to keep API clean.
    t1_ids_mem: Option<AHashSet<String>>,
    t3_ids_mem: Option<AHashSet<String>>,
    t1_ids_sharded: Option<IdShards>,
    t3_ids_sharded: Option<IdShards>,
}
impl ParentIds {
    pub fn new() -> Self {
        Self {
            t1_ids_mem: Some(AHashSet::new()),
            t3_ids_mem: Some(AHashSet::new()),
            t1_ids_sharded: None,
            t3_ids_sharded: None,
        }
    }
    #[inline]
    pub fn contains_t1<'a>(&self, id: &'a str, loader: &mut impl FnMut(&Path) -> Result<&AHashSet<String>>) -> Result<bool> {
        if let Some(mem) = &self.t1_ids_mem { return Ok(mem.contains(id)); }
        if let Some(sh) = &self.t1_ids_sharded {
            let idx = sh.idx(id);
            let p = sh.path_for(idx);
            let set = loader(&p)?;
            return Ok(set.contains(id));
        }
        Ok(false)
    }
    #[inline]
    pub fn contains_t3<'a>(&self, id: &'a str, loader: &mut impl FnMut(&Path) -> Result<&AHashSet<String>>) -> Result<bool> {
        if let Some(mem) = &self.t3_ids_mem { return Ok(mem.contains(id)); }
        if let Some(sh) = &self.t3_ids_sharded {
            let idx = sh.idx(id);
            let p = sh.path_for(idx);
            let set = loader(&p)?;
            return Ok(set.contains(id));
        }
        Ok(false)
    }
}

pub struct ParentMaps {
    pub comments: HashMap<String, String>,
    pub submissions: HashMap<String, (String, String)>,
    pub comment_index: Option<HashMap<String, PathBuf>>,
    pub submission_index: Option<HashMap<String, PathBuf>>,
}

impl RedditETL {
    pub fn collect_parent_ids_from_jsonls<I>(&self, jsonl_paths: I) -> Result<ParentIds>
    where
        I: IntoIterator<Item = PathBuf>,
    {
        init_tracing_once();

        let paths: Vec<PathBuf> = jsonl_paths.into_iter().collect();
        if paths.is_empty() {
            // FIX: return the correct type (ParentIds) when inputs are empty.
            return Ok(ParentIds::new());
        }

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

        paths.par_iter().for_each(|p| {
            let _ = (|| -> Result<()> {
                let f = File::open(p).with_context(|| format!("open {}", p.display()))?;
                let mut r = BufReader::with_capacity(read_buf, f);
                let mut buf = String::with_capacity(64 * 1024);
                loop {
                    buf.clear();
                    let n = r.read_line(&mut buf)?;
                    if n == 0 { break; }
                    if buf.ends_with('\n') { buf.pop(); if buf.ends_with('\r') { buf.pop(); } }
                    if buf.is_empty() { continue; }

                    let v: Value = match serde_json::from_str(&buf) { Ok(x) => x, Err(_) => {
                        if let Some(pb) = &pb { pb.inc(n as u64); }
                        continue;
                    }};
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

        Ok(ParentIds {
            t1_ids_mem: None,
            t3_ids_mem: None,
            t1_ids_sharded: Some(t1_shards),
            t3_ids_sharded: Some(t3_shards),
        })
    }

    pub fn resolve_parent_maps(&self, ids: &ParentIds, cache_dir: &Path, resume: bool) -> Result<ParentMaps> {
        init_tracing_once();

        let comments_out = cache_dir.join("comments");
        let submissions_out = cache_dir.join("submissions");
        fs::create_dir_all(&comments_out)?;
        fs::create_dir_all(&submissions_out)?;

        let discovered = discover_all(&self.opts.comments_dir, &self.opts.submissions_dir);
        let files = plan_files(&discovered, crate::config::Sources::Both, self.opts.start, self.opts.end);
        if files.is_empty() {
            // Correct return type for this function is ParentMaps.
            return Ok(ParentMaps {
                comments: HashMap::new(),
                submissions: HashMap::new(),
                comment_index: Some(HashMap::new()),
                submission_index: Some(HashMap::new()),
            });
        }

        let total_bytes = total_compressed_size(&files);
        let pb = if self.opts.progress {
            Some(make_progress_bar_labeled(total_bytes, self.opts.progress_label.as_deref()))
        } else { None };

        let read_buf = self.opts.read_buffer_bytes;

        let comment_idx = parking_lot::Mutex::new(HashMap::<String, PathBuf>::new());
        let submission_idx = parking_lot::Mutex::new(HashMap::<String, PathBuf>::new());

        // Limit file concurrency to reduce RAM spikes while resolving.
        crate::concurrency::for_each_file_limited(&files, self.opts.file_concurrency, |job| -> Result<()> {
            let (out_dir, prefix) = match job.kind {
                FileKind::Comment => (&comments_out, "RC"),
                FileKind::Submission => (&submissions_out, "RS"),
            };
            let out = out_dir.join(format!("{}_{}.json", prefix, job.ym));

            if resume && out.exists() {
                if let Some(pb) = &pb {
                    let sz = fs::metadata(&job.path).map(|m| m.len()).unwrap_or(0);
                    pb.inc(sz);
                }
                return Ok(());
            }

            // Worker-local caches for membership id shards, FIFO (no O(n) LRU update cost).
            let mut idset_t1_cache: HashMap<PathBuf, AHashSet<String>> = HashMap::new();
            let mut idset_t1_order: VecDeque<PathBuf> = VecDeque::new();
            let mut idset_t3_cache: HashMap<PathBuf, AHashSet<String>> = HashMap::new();
            let mut idset_t3_order: VecDeque<PathBuf> = VecDeque::new();

            let free = available_memory_fraction();
            let idset_t1_cap = if free > 0.50 { 256 } else if free > 0.20 { 128 } else { 64 };
            let idset_t3_cap = idset_t1_cap;

            let ensure_contains = |path: &Path,
                                   cache: &mut HashMap<PathBuf, AHashSet<String>>,
                                   order: &mut VecDeque<PathBuf>,
                                   cap: usize,
                                   id: &str| -> Result<bool> {
                if !cache.contains_key(path) {
                    if cache.len() >= cap {
                        if let Some(old) = order.pop_front() {
                            cache.remove(&old);
                        }
                    }
                    let f = File::open(path)
                        .with_context(|| format!("open idset shard {}", path.display()))?;
                    let r = BufReader::new(f);
                    let mut set = AHashSet::with_capacity(64_000);
                    for line in r.lines() {
                        let mut s = line?;
                        if s.ends_with('\r') { s.pop(); }
                        if !s.is_empty() { set.insert(s); }
                    }
                    cache.insert(path.to_path_buf(), set);
                    order.push_back(path.to_path_buf());
                }
                Ok(cache.get(path).map_or(false, |set| set.contains(id)))
            };

            let mut out_map_c: HashMap<String, String> = HashMap::new();
            let mut out_map_s: HashMap<String, (String, String)> = HashMap::new();

            let _ = for_each_line_with_progress_cfg_no_throttle(
                &job.path,
                read_buf,
                |d| { if let Some(pb) = &pb { pb.inc(d); } },
                |line| {
                    let min = match parse_minimal(line) {
                        Ok(v) => v,
                        Err(_) => return Ok(()),
                    };
                    if let Some(id) = min.id.as_deref() {
                        match job.kind {
                            FileKind::Comment => {
                                let needed = if let Some(sh) = &ids.t1_ids_sharded {
                                    let idx = sh.idx(id);
                                    let p = sh.path_for(idx);
                                    ensure_contains(&p, &mut idset_t1_cache, &mut idset_t1_order, idset_t1_cap, id)?
                                } else if let Some(mem) = &ids.t1_ids_mem {
                                    mem.contains(id)
                                } else { false };

                                if needed {
                                    if let Some(body) = min.body.as_deref() {
                                        out_map_c.insert(id.to_string(), body.to_string());
                                    }
                                }
                            }
                            FileKind::Submission => {
                                let needed = if let Some(sh) = &ids.t3_ids_sharded {
                                    let idx = sh.idx(id);
                                    let p = sh.path_for(idx);
                                    ensure_contains(&p, &mut idset_t3_cache, &mut idset_t3_order, idset_t3_cap, id)?
                                } else if let Some(mem) = &ids.t3_ids_mem {
                                    mem.contains(id)
                                } else { false };

                                if needed {
                                    let title = min.title.as_deref().unwrap_or_default().to_string();
                                    let selftext = min.selftext.as_deref().unwrap_or_default().to_string();
                                    out_map_s.insert(id.to_string(), (title, selftext));
                                }
                            }
                        }
                    }
                    Ok(())
                }
            );

            if let Err(e) = (|| -> Result<()> {
                let f = File::create(&out)?;
                let mut w = BufWriter::new(f);
                match job.kind {
                    FileKind::Comment => {
                        {
                            let mut idx = comment_idx.lock();
                            for k in out_map_c.keys() { idx.insert(k.clone(), out.clone()); }
                        }
                        serde_json::to_writer(&mut w, &out_map_c)?
                    }
                    FileKind::Submission => {
                        {
                            let mut idx = submission_idx.lock();
                            for k in out_map_s.keys() { idx.insert(k.clone(), out.clone()); }
                        }
                        serde_json::to_writer(&mut w, &out_map_s)?
                    }
                }
                w.flush()?;
                Ok(())
            })() {
                tracing::warn!(path=%out.display(), error=%e, "failed writing parent shard");
            }

            Ok(())
        })?;

        if let Some(pb) = pb {
            let final_msg = if let Some(l) = self.opts.progress_label.as_deref() {
                format!("{l} done")
            } else {
                "done".to_string()
            };
            pb.finish_with_message(final_msg);
        }

        // Much stricter: only eager-load when plenty of RAM is free.
        let eager_ok = available_memory_fraction() > 0.50;

        let mut comments_map: HashMap<String, String> = HashMap::new();
        let mut submissions_map: HashMap<String, (String, String)> = HashMap::new();

        if eager_ok {
            let load = |dir: &Path| -> Vec<PathBuf> {
                let mut v: Vec<PathBuf> = fs::read_dir(dir).ok()
                    .into_iter().flat_map(|it| it.filter_map(|e| e.ok().map(|e| e.path()))).collect();
                v.sort();
                v
            };

            for p in load(&comments_out) {
                let f = File::open(&p)?;
                let r = BufReader::new(f);
                let m: HashMap<String, String> = serde_json::from_reader(r)?;
                for (k, v) in m { comments_map.insert(k, v); }
                if is_low_memory(0.10) { break; }
            }
            for p in load(&submissions_out) {
                let f = File::open(&p)?;
                let r = BufReader::new(f);
                let m: HashMap<String, (String, String)> = serde_json::from_reader(r)?;
                for (k, v) in m { submissions_map.insert(k, v); }
                if is_low_memory(0.10) { break; }
            }
        }

        Ok(ParentMaps {
            comments: comments_map,
            submissions: submissions_map,
            comment_index: Some(comment_idx.into_inner()),
            submission_index: Some(submission_idx.into_inner()),
        })
    }

    pub fn attach_parents_jsonls_parallel(
        &self,
        inputs: Vec<PathBuf>,
        out_dir: &Path,
        parents: &ParentMaps,
        resume: bool,
    ) -> Result<Vec<PathBuf>> {
        init_tracing_once();
        fs::create_dir_all(out_dir)?;

        let label = self.opts.progress_label.as_deref().unwrap_or("Attaching parents");
        let pb = if self.opts.progress { Some(make_count_progress(inputs.len() as u64, label)) } else { None };

        let parents_c_eager = &parents.comments;
        let parents_s_eager = &parents.submissions;

        let idx_c = parents.comment_index.as_ref();
        let idx_s = parents.submission_index.as_ref();

        let out_paths: Vec<PathBuf> = inputs
            .par_iter()
            .map(|in_path| -> Result<PathBuf> {
                let name = in_path.file_name().unwrap().to_string_lossy().to_string();
                let out_path = out_dir.join(name);

                if resume && out_path.exists() {
                    if let Some(pb) = &pb { pb.inc(1); }
                    return Ok(out_path);
                }

                let mut c_cache: HashMap<PathBuf, HashMap<String, String>> = HashMap::new();
                let mut c_order: VecDeque<PathBuf> = VecDeque::new();
                let c_cap: usize = 8;

                let mut s_cache: HashMap<PathBuf, HashMap<String, (String, String)>> = HashMap::new();
                let mut s_order: VecDeque<PathBuf> = VecDeque::new();
                let s_cap: usize = 6;

                let mut load_comments_shard_value = |comment_id: &str| -> Result<Option<String>> {
                    if let Some(v) = parents_c_eager.get(comment_id) {
                        return Ok(Some(v.clone()));
                    }
                    if let Some(idx) = idx_c {
                        if let Some(p) = idx.get(comment_id) {
                            if !c_cache.contains_key(p) {
                                if c_cache.len() >= c_cap {
                                    if let Some(old) = c_order.pop_front() {
                                        c_cache.remove(&old);
                                    }
                                }
                                let file = File::open(p)?;
                                let rdr = BufReader::new(file);
                                let map: HashMap<String, String> = serde_json::from_reader(rdr)?;
                                c_cache.insert(p.clone(), map);
                                c_order.push_back(p.clone());
                            } else {
                                let pos = c_order.iter().position(|x| x == p);
                                if let Some(i) = pos { c_order.remove(i); }
                                c_order.push_back(p.clone());
                            }
                            return Ok(c_cache.get(p).and_then(|m| m.get(comment_id).cloned()));
                        }
                    }
                    Ok(None)
                };

                let mut load_submissions_shard_value = |submission_id: &str| -> Result<Option<(String, String)>> {
                    if let Some(v) = parents_s_eager.get(submission_id) {
                        return Ok(Some(v.clone()));
                    }
                    if let Some(idx) = idx_s {
                        if let Some(p) = idx.get(submission_id) {
                            if !s_cache.contains_key(p) {
                                if s_cache.len() >= s_cap {
                                    if let Some(old) = s_order.pop_front() {
                                        s_cache.remove(&old);
                                    }
                                }
                                let file = File::open(p)?;
                                let rdr = BufReader::new(file);
                                let map: HashMap<String, (String, String)> = serde_json::from_reader(rdr)?;
                                s_cache.insert(p.clone(), map);
                                s_order.push_back(p.clone());
                            } else {
                                let pos = s_order.iter().position(|x| x == p);
                                if let Some(i) = pos { s_order.remove(i); }
                                s_order.push_back(p.clone());
                            }
                            return Ok(s_cache.get(p).and_then(|m| m.get(submission_id).cloned()));
                        }
                    }
                    Ok(None)
                };

                let f = File::open(in_path)?;
                let r = BufReader::new(f);
                let out = File::create(&out_path)?;
                let mut w = BufWriter::new(out);

                for line in r.lines() {
                    let line = line?;
                    if line.is_empty() { continue; }
                    let mut v: Value = match serde_json::from_str(&line) { Ok(x) => x, Err(_) => continue };
                    let is_comment = v.get("body").is_some() && v.get("parent_id").is_some();

                    if is_comment {
                        if let Some(parent_id) = v.get("parent_id").and_then(|x| x.as_str()) {
                            let mut parent_obj = serde_json::Map::new();

                            if let Some(rest) = parent_id.strip_prefix("t1_") {
                                if let Some(text) = load_comments_shard_value(rest)? {
                                    parent_obj.insert("kind".into(), Value::String("comment".into()));
                                    parent_obj.insert("id".into(), Value::String(rest.to_string()));
                                    parent_obj.insert("body".into(), Value::String(text));
                                }
                            } else if let Some(rest) = parent_id.strip_prefix("t3_") {
                                if let Some((title, selftext)) = load_submissions_shard_value(rest)? {
                                    parent_obj.insert("kind".into(), Value::String("submission".into()));
                                    parent_obj.insert("id".into(), Value::String(rest.to_string()));
                                    parent_obj.insert("title".into(), Value::String(title));
                                    parent_obj.insert("selftext".into(), Value::String(selftext));
                                }
                            }

                            if let Some(map) = v.as_object_mut() {
                                map.insert("parent".into(), Value::Object(parent_obj));
                            }
                        }
                    }

                    serde_json::to_writer(&mut w, &v)?;
                    w.write_all(b"\n")?;
                }
                w.flush()?;
                if let Some(pb) = &pb { pb.inc(1); }
                Ok(out_path)
            })
            .collect::<Result<Vec<_>>>()?;

        if let Some(pb) = pb {
            let final_msg = format!("{label} done");
            pb.finish_with_message(final_msg);
        }
        Ok(out_paths)
    }
}
