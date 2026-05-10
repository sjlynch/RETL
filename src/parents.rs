use crate::date::YearMonth;
use crate::filters::ym_from_epoch;
use crate::parents_ids::{IdShards, SharedIdsetCache, WorkerShardCache};
use crate::paths::{discover_all, plan_files, FileKind};
use crate::progress::{make_count_progress, make_progress_bar_labeled, total_compressed_size};
use crate::pipeline::RedditETL;
use crate::mem::{available_memory_fraction, is_low_memory};
use crate::util::replace_file_atomic_backoff;
use crate::zstd_jsonl::{
    for_each_line_with_progress_cfg_no_throttle,
    parse_minimal,
};
use anyhow::{Context, Result};
use rayon::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ahash::AHashSet;

/// Per-worker FIFO cache caps for `attach_parents_jsonls_parallel`.
/// FIFO eviction (no bump-on-hit) is intentional: see comment in
/// `attach_parents_jsonls_parallel`.
const COMMENT_SHARD_CACHE_CAP: usize = 8;
const SUBMISSION_SHARD_CACHE_CAP: usize = 6;

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
    pub(crate) fn from_shards(t1: IdShards, t3: IdShards) -> Self {
        Self {
            t1_ids_mem: None,
            t3_ids_mem: None,
            t1_ids_sharded: Some(t1),
            t3_ids_sharded: Some(t3),
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

/// Parent shard cache lookup tables.
///
/// Replaces the prior per-id index (one entry PER parent id — tens of millions
/// at corpus scale). Now keyed by month: the resolver writes one shard JSON per
/// (YearMonth, FileKind), and consumers resolve a parent id to its shard via
/// the child record's own-month metadata + the parent's prefix-derived FileKind.
pub struct ParentMaps {
    pub comments: HashMap<String, String>,
    pub submissions: HashMap<String, (String, String)>,
    pub comment_shards: Option<HashMap<YearMonth, PathBuf>>,
    pub submission_shards: Option<HashMap<YearMonth, PathBuf>>,
}

impl ParentMaps {
    /// Helper: pick the shard that is most likely to own `_id`, given its
    /// FileKind (from the parent_id prefix) and the consuming record's own
    /// month. Reddit threads almost always live in a single month, so the
    /// child's own month is a strong prior for the parent's shard.
    pub fn shard_for(&self, kind: FileKind, own_month: YearMonth) -> Option<&PathBuf> {
        let map = match kind {
            FileKind::Comment => self.comment_shards.as_ref()?,
            FileKind::Submission => self.submission_shards.as_ref()?,
        };
        map.get(&own_month)
    }
}

impl RedditETL {
    pub fn resolve_parent_maps(&self, ids: &ParentIds, cache_dir: &Path, resume: bool) -> Result<ParentMaps> {
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
                comment_shards: Some(HashMap::new()),
                submission_shards: Some(HashMap::new()),
            });
        }

        let total_bytes = total_compressed_size(&files);
        let pb = if self.opts.progress {
            Some(make_progress_bar_labeled(total_bytes, self.opts.progress_label.as_deref()))
        } else { None };

        let read_buf = self.opts.read_buffer_bytes;

        // Shard-keyed indexes (one entry per processed monthly shard, NOT per id).
        // ~10^6x memory reduction vs. the prior per-id index at corpus scale.
        let comment_shards = parking_lot::Mutex::new(HashMap::<YearMonth, PathBuf>::new());
        let submission_shards = parking_lot::Mutex::new(HashMap::<YearMonth, PathBuf>::new());

        // Process-wide id-shard cache: each (t1|t3) ids_NNNN.txt shard is read
        // and parsed at most once globally instead of once per rayon worker.
        let free = available_memory_fraction();
        let idset_cap = if free > 0.50 { 512 } else if free > 0.20 { 256 } else { 128 };
        let idset_cache: Arc<SharedIdsetCache> = Arc::new(SharedIdsetCache::new(idset_cap));

        // Track per-file errors so we can warn-and-continue rather than
        // silently dropping bad files (or failing the whole job).
        let resolve_err_count = AtomicUsize::new(0);

        // Limit file concurrency to reduce RAM spikes while resolving.
        crate::concurrency::for_each_file_limited(&files, self.opts.file_concurrency, |job| -> Result<()> {
            let (out_dir, prefix) = match job.kind {
                FileKind::Comment => (&comments_out, "RC"),
                FileKind::Submission => (&submissions_out, "RS"),
            };
            let out = out_dir.join(format!("{}_{}.json", prefix, job.ym));

            // Always record the shard path for downstream lookups, even on
            // resume — attach() needs the shard map populated regardless of
            // whether this run produced the file.
            let record_shard = |path: PathBuf| {
                match job.kind {
                    FileKind::Comment => {
                        comment_shards.lock().insert(job.ym, path);
                    }
                    FileKind::Submission => {
                        submission_shards.lock().insert(job.ym, path);
                    }
                }
            };

            if resume && out.exists() {
                // Validate that the existing JSON parses; treat unreadable /
                // corrupt files as missing and rebuild them.
                let valid = match File::open(&out) {
                    Ok(f) => match job.kind {
                        FileKind::Comment => serde_json::from_reader::<_, HashMap<String, String>>(BufReader::new(f)).is_ok(),
                        FileKind::Submission => serde_json::from_reader::<_, HashMap<String, (String, String)>>(BufReader::new(f)).is_ok(),
                    },
                    Err(_) => false,
                };
                if valid {
                    record_shard(out.clone());
                    if let Some(pb) = &pb {
                        let sz = fs::metadata(&job.path).map(|m| m.len()).unwrap_or(0);
                        pb.inc(sz);
                    }
                    return Ok(());
                } else {
                    tracing::warn!(path=%out.display(), "resume: existing parent shard is unreadable/corrupt, rebuilding");
                    let _ = fs::remove_file(&out);
                }
            }

            let ensure_contains = |shard_path: &Path, id: &str| -> Result<bool> {
                let set = idset_cache.get_or_load(shard_path)?;
                Ok(set.contains(id))
            };

            let mut out_map_c: HashMap<String, String> = HashMap::new();
            let mut out_map_s: HashMap<String, (String, String)> = HashMap::new();

            for_each_line_with_progress_cfg_no_throttle(
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
                                    ensure_contains(&p, id)?
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
                                    ensure_contains(&p, id)?
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
            )
            .with_context(|| format!("scan {}", job.path.display()))?;

            // Atomic write: serialize to a sibling .tmp first, fsync the data
            // path via BufWriter::flush, then atomically replace the dest.
            // Prevents readers from observing torn / half-written shard JSON
            // after a crash mid-write.
            let tmp = out.with_extension("json.tmp");
            let write_res = (|| -> Result<()> {
                let f = File::create(&tmp)
                    .with_context(|| format!("create tmp {}", tmp.display()))?;
                let mut w = BufWriter::new(f);
                match job.kind {
                    FileKind::Comment => serde_json::to_writer(&mut w, &out_map_c)?,
                    FileKind::Submission => serde_json::to_writer(&mut w, &out_map_s)?,
                }
                w.flush()?;
                replace_file_atomic_backoff(&tmp, &out)?;
                Ok(())
            })();

            match write_res {
                Ok(_) => {
                    record_shard(out.clone());
                }
                Err(e) => {
                    resolve_err_count.fetch_add(1, Ordering::Relaxed);
                    // Best-effort cleanup so a stale .tmp doesn't linger.
                    let _ = fs::remove_file(&tmp);
                    tracing::warn!(path=%out.display(), error=%e, "failed writing parent shard");
                }
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

        let resolve_errs = resolve_err_count.load(Ordering::Relaxed);
        if resolve_errs > 0 {
            tracing::warn!(error_count = resolve_errs, "resolve_parent_maps completed with shard write errors");
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
            comment_shards: Some(comment_shards.into_inner()),
            submission_shards: Some(submission_shards.into_inner()),
        })
    }

    pub fn attach_parents_jsonls_parallel(
        &self,
        inputs: Vec<PathBuf>,
        out_dir: &Path,
        parents: &ParentMaps,
        resume: bool,
    ) -> Result<Vec<PathBuf>> {
        fs::create_dir_all(out_dir)?;

        let label = self.opts.progress_label.as_deref().unwrap_or("Attaching parents");
        let pb = if self.opts.progress { Some(make_count_progress(inputs.len() as u64, label)) } else { None };

        let parents_c_eager = &parents.comments;
        let parents_s_eager = &parents.submissions;

        let comment_shards = parents.comment_shards.as_ref();
        let submission_shards = parents.submission_shards.as_ref();

        let out_paths: Vec<PathBuf> = inputs
            .par_iter()
            .map(|in_path| -> Result<PathBuf> {
                let name = in_path.file_name().unwrap().to_string_lossy().to_string();
                let out_path = out_dir.join(name);

                if resume && out_path.exists() {
                    if let Some(pb) = &pb { pb.inc(1); }
                    return Ok(out_path);
                }

                // Per-worker FIFO caches of parsed shard JSON. See
                // `WorkerShardCache` for why eviction is plain FIFO with no
                // bump-on-hit.
                let mut c_cache = WorkerShardCache::<String>::new(COMMENT_SHARD_CACHE_CAP);
                let mut s_cache = WorkerShardCache::<(String, String)>::new(SUBMISSION_SHARD_CACHE_CAP);

                // Resolve a comment-parent id by looking in the own-month
                // shard first; if absent, fall back to scanning other months
                // for the same kind. The fallback rarely fires on real data
                // (Reddit threads almost always live in a single month).
                let mut load_comments_shard_value = |comment_id: &str, own_ym: Option<YearMonth>| -> Result<Option<String>> {
                    if let Some(v) = parents_c_eager.get(comment_id) {
                        return Ok(Some(v.clone()));
                    }
                    let Some(idx) = comment_shards else { return Ok(None); };

                    if let Some(ym) = own_ym {
                        if let Some(p) = idx.get(&ym) {
                            if let Some(v) = c_cache.get(p, comment_id)? {
                                return Ok(Some(v));
                            }
                        }
                    }
                    for (ym, p) in idx {
                        if Some(*ym) == own_ym { continue; }
                        if let Some(v) = c_cache.get(p, comment_id)? {
                            return Ok(Some(v));
                        }
                    }
                    Ok(None)
                };

                let mut load_submissions_shard_value = |submission_id: &str, own_ym: Option<YearMonth>| -> Result<Option<(String, String)>> {
                    if let Some(v) = parents_s_eager.get(submission_id) {
                        return Ok(Some(v.clone()));
                    }
                    let Some(idx) = submission_shards else { return Ok(None); };

                    if let Some(ym) = own_ym {
                        if let Some(p) = idx.get(&ym) {
                            if let Some(v) = s_cache.get(p, submission_id)? {
                                return Ok(Some(v));
                            }
                        }
                    }
                    for (ym, p) in idx {
                        if Some(*ym) == own_ym { continue; }
                        if let Some(v) = s_cache.get(p, submission_id)? {
                            return Ok(Some(v));
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
                        // Own-month is derived from the consuming record's
                        // created_utc; used to pick the most-likely parent
                        // shard before falling back to other months.
                        let own_ym = v.get("created_utc")
                            .and_then(|x| x.as_i64())
                            .map(ym_from_epoch);

                        if let Some(parent_id) = v.get("parent_id").and_then(|x| x.as_str()) {
                            let mut parent_obj = serde_json::Map::new();

                            if let Some(rest) = parent_id.strip_prefix("t1_") {
                                if let Some(text) = load_comments_shard_value(rest, own_ym)? {
                                    parent_obj.insert("kind".into(), Value::String("comment".into()));
                                    parent_obj.insert("id".into(), Value::String(rest.to_string()));
                                    parent_obj.insert("body".into(), Value::String(text));
                                }
                            } else if let Some(rest) = parent_id.strip_prefix("t3_") {
                                if let Some((title, selftext)) = load_submissions_shard_value(rest, own_ym)? {
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
