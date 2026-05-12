use crate::atomic_write::{
    ensure_staging_dir, sweep_stale_inprogress, write_jsonl_atomic, INPROGRESS_EXT,
};
use crate::date::YearMonth;
use crate::filters::ym_from_epoch;
use crate::mem::{available_memory_fraction, is_low_memory};
use crate::parents_ids::{IdShards, SharedIdsetCache, WorkerShardCache};
use crate::paths::{discover_all, plan_files, FileJob, FileKind};
use crate::pipeline::RedditETL;
use crate::progress::{make_count_progress, make_progress_bar_labeled, total_compressed_size};
use crate::util::{replace_file_atomic_backoff, with_thread_pool};
use crate::zstd_jsonl::{for_each_line_with_progress_cfg_no_throttle, parse_minimal};
use anyhow::{Context, Result};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use ahash::AHashSet;

/// Per-worker FIFO cache caps for `attach_parents_jsonls_parallel`.
/// FIFO eviction (no bump-on-hit) is intentional: see comment in
/// `attach_parents_jsonls_parallel`.
const COMMENT_SHARD_CACHE_CAP: usize = 8;
const SUBMISSION_SHARD_CACHE_CAP: usize = 6;
const IDSET_CACHE_HIGH_FREE_THRESHOLD: f64 = 0.50;
const IDSET_CACHE_MEDIUM_FREE_THRESHOLD: f64 = 0.20;
const IDSET_CACHE_HIGH_CAP: usize = 512;
const IDSET_CACHE_MEDIUM_CAP: usize = 256;
const IDSET_CACHE_LOW_CAP: usize = 128;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ParentAttachStats {
    pub resolved: u64,
    pub unresolved: u64,
}

impl ParentAttachStats {
    pub fn total(self) -> u64 {
        self.resolved + self.unresolved
    }

    pub fn unresolved_rate(self) -> f64 {
        let total = self.total();
        if total == 0 {
            0.0
        } else {
            self.unresolved as f64 / total as f64
        }
    }

    fn add(&mut self, other: Self) {
        self.resolved += other.resolved;
        self.unresolved += other.unresolved;
    }
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
    pub(crate) fn from_shards(t1: IdShards, t3: IdShards) -> Self {
        Self {
            t1_ids_mem: None,
            t3_ids_mem: None,
            t1_ids_sharded: Some(t1),
            t3_ids_sharded: Some(t3),
        }
    }
    #[inline]
    pub fn contains_t1<'a>(
        &self,
        id: &'a str,
        loader: &mut impl FnMut(&Path) -> Result<&AHashSet<String>>,
    ) -> Result<bool> {
        if let Some(mem) = &self.t1_ids_mem {
            return Ok(mem.contains(id));
        }
        if let Some(sh) = &self.t1_ids_sharded {
            let idx = sh.idx(id);
            let p = sh.path_for(idx);
            let set = loader(&p)?;
            return Ok(set.contains(id));
        }
        Ok(false)
    }
    #[inline]
    pub fn contains_t3<'a>(
        &self,
        id: &'a str,
        loader: &mut impl FnMut(&Path) -> Result<&AHashSet<String>>,
    ) -> Result<bool> {
        if let Some(mem) = &self.t3_ids_mem {
            return Ok(mem.contains(id));
        }
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

#[inline]
fn idset_cache_cap_for_free_memory(free: f64) -> usize {
    if free > IDSET_CACHE_HIGH_FREE_THRESHOLD {
        IDSET_CACHE_HIGH_CAP
    } else if free > IDSET_CACHE_MEDIUM_FREE_THRESHOLD {
        IDSET_CACHE_MEDIUM_CAP
    } else {
        IDSET_CACHE_LOW_CAP
    }
}

fn build_id_shard_index(
    files: &[FileJob],
    ids: &ParentIds,
    comments_out: &Path,
    submissions_out: &Path,
    resume: bool,
    read_buf: usize,
    file_concurrency: usize,
    pb: Option<&indicatif::ProgressBar>,
) -> Result<(HashMap<YearMonth, PathBuf>, HashMap<YearMonth, PathBuf>)> {
    // Shard-keyed indexes (one entry per processed monthly shard, NOT per id).
    // ~10^6x memory reduction vs. the prior per-id index at corpus scale.
    let comment_shards = parking_lot::Mutex::new(HashMap::<YearMonth, PathBuf>::new());
    let submission_shards = parking_lot::Mutex::new(HashMap::<YearMonth, PathBuf>::new());

    // Process-wide id-shard cache: each (t1|t3) ids_NNNN.txt shard is read
    // and parsed at most once globally instead of once per rayon worker.
    let idset_cap = idset_cache_cap_for_free_memory(available_memory_fraction());
    let idset_cache: Arc<SharedIdsetCache> = Arc::new(SharedIdsetCache::new(idset_cap));

    // Track per-file errors so we can warn-and-continue rather than
    // silently dropping bad files (or failing the whole job).
    let resolve_err_count = AtomicUsize::new(0);

    // Limit file concurrency to reduce RAM spikes while resolving.
    crate::concurrency::for_each_file_limited(files, file_concurrency, |job| -> Result<()> {
        let (out_dir, prefix) = match job.kind {
            FileKind::Comment => (comments_out, "RC"),
            FileKind::Submission => (submissions_out, "RS"),
        };
        let out = out_dir.join(format!("{}_{}.json", prefix, job.ym));

        // Always record the shard path for downstream lookups, even on
        // resume — attach() needs the shard map populated regardless of
        // whether this run produced the file.
        let record_shard = |path: PathBuf| match job.kind {
            FileKind::Comment => {
                comment_shards.lock().insert(job.ym, path);
            }
            FileKind::Submission => {
                submission_shards.lock().insert(job.ym, path);
            }
        };

        if resume && out.exists() {
            // Validate that the existing JSON parses; treat unreadable /
            // corrupt files as missing and rebuild them.
            let valid = match File::open(&out) {
                Ok(f) => match job.kind {
                    FileKind::Comment => {
                        serde_json::from_reader::<_, HashMap<String, String>>(BufReader::new(f))
                            .is_ok()
                    }
                    FileKind::Submission => serde_json::from_reader::<
                        _,
                        HashMap<String, (String, String)>,
                    >(BufReader::new(f))
                    .is_ok(),
                },
                Err(_) => false,
            };
            if valid {
                record_shard(out.clone());
                if let Some(pb) = pb {
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
            |d| {
                if let Some(pb) = pb {
                    pb.inc(d);
                }
            },
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
                            } else {
                                false
                            };

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
                            } else {
                                false
                            };

                            if needed {
                                let title = min.title.as_deref().unwrap_or_default().to_string();
                                let selftext =
                                    min.selftext.as_deref().unwrap_or_default().to_string();
                                out_map_s.insert(id.to_string(), (title, selftext));
                            }
                        }
                    }
                }
                Ok(())
            },
        )
        .with_context(|| format!("scan {}", job.path.display()))?;

        // Atomic write: serialize to a sibling .tmp first, fsync the data
        // path via BufWriter::flush, then atomically replace the dest.
        // Prevents readers from observing torn / half-written shard JSON
        // after a crash mid-write.
        let tmp = out.with_extension("json.tmp");
        let write_res = (|| -> Result<()> {
            let f = File::create(&tmp).with_context(|| format!("create tmp {}", tmp.display()))?;
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

    let resolve_errs = resolve_err_count.load(Ordering::Relaxed);
    if resolve_errs > 0 {
        tracing::warn!(
            error_count = resolve_errs,
            "resolve_parent_maps completed with shard write errors"
        );
    }

    Ok((comment_shards.into_inner(), submission_shards.into_inner()))
}

fn attach_inprogress_path(staging_dir: &Path, final_dest: &Path) -> Result<PathBuf> {
    let file_name = final_dest
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("final_dest has no file name: {}", final_dest.display()))?;
    let mut staged = staging_dir.join(file_name);
    staged.as_mut_os_string().push(INPROGRESS_EXT);
    Ok(staged)
}

fn validate_jsonl_file(path: &Path) -> bool {
    let Ok(f) = File::open(path) else {
        return false;
    };
    let r = BufReader::new(f);
    for line in r.lines() {
        let Ok(line) = line else {
            return false;
        };
        if line.trim().is_empty() {
            continue;
        }
        if serde_json::from_str::<Value>(&line).is_err() {
            return false;
        }
    }
    true
}

fn load_shard_value<V>(
    eager_values: &HashMap<String, V>,
    shards: Option<&HashMap<YearMonth, PathBuf>>,
    cache: &mut WorkerShardCache<V>,
    key: &str,
    own_ym: Option<YearMonth>,
) -> Result<Option<V>>
where
    V: serde::de::DeserializeOwned + Clone,
{
    if let Some(v) = eager_values.get(key) {
        return Ok(Some(v.clone()));
    }
    let Some(idx) = shards else {
        return Ok(None);
    };

    if let Some(ym) = own_ym {
        if let Some(p) = idx.get(&ym) {
            if let Some(v) = cache.get(p, key)? {
                return Ok(Some(v));
            }
        }
    }
    for (ym, p) in idx {
        if Some(*ym) == own_ym {
            continue;
        }
        if let Some(v) = cache.get(p, key)? {
            return Ok(Some(v));
        }
    }
    Ok(None)
}

impl RedditETL {
    pub fn resolve_parent_maps(
        &self,
        ids: &ParentIds,
        cache_dir: &Path,
        resume: bool,
    ) -> Result<ParentMaps> {
        with_thread_pool(self.opts.parallelism, || {
            let comments_out = cache_dir.join("comments");
            let submissions_out = cache_dir.join("submissions");
            fs::create_dir_all(&comments_out)?;
            fs::create_dir_all(&submissions_out)?;

            let discovered = discover_all(&self.opts.comments_dir, &self.opts.submissions_dir);
            let files = plan_files(
                &discovered,
                crate::config::Sources::Both,
                self.opts.start,
                self.opts.end,
            );
            if files.is_empty() {
                tracing::warn!(
                    comments_dir = %self.opts.comments_dir.display(),
                    submissions_dir = %self.opts.submissions_dir.display(),
                    start = ?self.opts.start,
                    end = ?self.opts.end,
                    "resolve_parent_maps planned zero corpus files; returning empty parent maps"
                );
                return Ok(ParentMaps {
                    comments: HashMap::new(),
                    submissions: HashMap::new(),
                    comment_shards: Some(HashMap::new()),
                    submission_shards: Some(HashMap::new()),
                });
            }

            let total_bytes = total_compressed_size(&files);
            let pb = if self.opts.progress {
                Some(make_progress_bar_labeled(
                    total_bytes,
                    self.opts.progress_label.as_deref(),
                ))
            } else {
                None
            };

            let (comment_shards, submission_shards) = build_id_shard_index(
                &files,
                ids,
                &comments_out,
                &submissions_out,
                resume,
                self.opts.read_buffer_bytes,
                self.opts.file_concurrency,
                pb.as_ref(),
            )?;

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
                    let mut v: Vec<PathBuf> = fs::read_dir(dir)
                        .ok()
                        .into_iter()
                        .flat_map(|it| it.filter_map(|e| e.ok().map(|e| e.path())))
                        .collect();
                    v.sort();
                    v
                };

                for p in load(&comments_out) {
                    let f = File::open(&p)?;
                    let r = BufReader::new(f);
                    let m: HashMap<String, String> = serde_json::from_reader(r)?;
                    for (k, v) in m {
                        comments_map.insert(k, v);
                    }
                    if is_low_memory(0.10) {
                        break;
                    }
                }
                for p in load(&submissions_out) {
                    let f = File::open(&p)?;
                    let r = BufReader::new(f);
                    let m: HashMap<String, (String, String)> = serde_json::from_reader(r)?;
                    for (k, v) in m {
                        submissions_map.insert(k, v);
                    }
                    if is_low_memory(0.10) {
                        break;
                    }
                }
            }

            Ok(ParentMaps {
                comments: comments_map,
                submissions: submissions_map,
                comment_shards: Some(comment_shards),
                submission_shards: Some(submission_shards),
            })
        })
    }

    pub fn attach_parents_jsonls_parallel(
        &self,
        inputs: Vec<PathBuf>,
        out_dir: &Path,
        parents: &ParentMaps,
        resume: bool,
    ) -> Result<Vec<PathBuf>> {
        Ok(self
            .attach_parents_jsonls_parallel_with_stats(inputs, out_dir, parents, resume)?
            .0)
    }

    pub fn attach_parents_jsonls_parallel_with_stats(
        &self,
        inputs: Vec<PathBuf>,
        out_dir: &Path,
        parents: &ParentMaps,
        resume: bool,
    ) -> Result<(Vec<PathBuf>, ParentAttachStats)> {
        with_thread_pool(self.opts.parallelism, || {
            fs::create_dir_all(out_dir)?;
            let staging_dir = ensure_staging_dir(out_dir)?;
            sweep_stale_inprogress(out_dir, true)?;

            let label = self
                .opts
                .progress_label
                .as_deref()
                .unwrap_or("Attaching parents");
            let pb = if self.opts.progress {
                Some(make_count_progress(inputs.len() as u64, label))
            } else {
                None
            };

            let parents_c_eager = &parents.comments;
            let parents_s_eager = &parents.submissions;

            let comment_shards = parents.comment_shards.as_ref();
            let submission_shards = parents.submission_shards.as_ref();

            let indexed_inputs: Vec<(usize, PathBuf)> = inputs.into_iter().enumerate().collect();
            let attached = std::sync::Mutex::new(vec![None; indexed_inputs.len()]);

            crate::concurrency::for_each_file_limited(
                &indexed_inputs,
                self.opts.file_concurrency,
                |(idx, in_path)| -> Result<()> {
                    let name = in_path.file_name().unwrap().to_string_lossy().to_string();
                    let out_path = out_dir.join(name);
                    let inprogress_path = attach_inprogress_path(&staging_dir, &out_path)?;

                    if resume && out_path.exists() && !inprogress_path.exists() {
                        if validate_jsonl_file(&out_path) {
                            if let Some(pb) = &pb {
                                pb.inc(1);
                            }
                            attached.lock().unwrap()[*idx] =
                                Some((out_path, ParentAttachStats::default()));
                            return Ok(());
                        }
                        tracing::warn!(path=%out_path.display(), "resume: existing attached JSONL is unreadable/corrupt, rebuilding");
                    }

                    // Per-worker FIFO caches of parsed shard JSON. See
                    // `WorkerShardCache` for why eviction is plain FIFO with no
                    // bump-on-hit.
                    let mut c_cache = WorkerShardCache::<String>::new(COMMENT_SHARD_CACHE_CAP);
                    let mut s_cache =
                        WorkerShardCache::<(String, String)>::new(SUBMISSION_SHARD_CACHE_CAP);

                    let file_stats = write_jsonl_atomic(
                        &staging_dir,
                        &out_path,
                        self.opts.write_buffer_bytes,
                        |w| -> Result<ParentAttachStats> {
                            let mut file_stats = ParentAttachStats::default();
                            let f = File::open(in_path)?;
                            let r = BufReader::new(f);

                            for line in r.lines() {
                                let line = line?;
                                if line.is_empty() {
                                    continue;
                                }
                                let mut v: Value = match serde_json::from_str(&line) {
                                    Ok(x) => x,
                                    Err(_) => continue,
                                };
                                let is_comment =
                                    v.get("body").is_some() && v.get("parent_id").is_some();

                                if is_comment {
                                    // Own-month is derived from the consuming record's
                                    // created_utc; used to pick the most-likely parent
                                    // shard before falling back to other months.
                                    let own_ym = v
                                        .get("created_utc")
                                        .and_then(|x| x.as_i64())
                                        .map(ym_from_epoch);

                                    if let Some(parent_id) =
                                        v.get("parent_id").and_then(|x| x.as_str())
                                    {
                                        let mut parent_obj = serde_json::Map::new();
                                        let mut payload_fields = 0usize;

                                        if let Some(rest) = parent_id.strip_prefix("t1_") {
                                            if let Some(text) = load_shard_value(
                                                parents_c_eager,
                                                comment_shards,
                                                &mut c_cache,
                                                rest,
                                                own_ym,
                                            )? {
                                                parent_obj.insert(
                                                    "kind".into(),
                                                    Value::String("comment".into()),
                                                );
                                                parent_obj.insert(
                                                    "id".into(),
                                                    Value::String(rest.to_string()),
                                                );
                                                parent_obj
                                                    .insert("body".into(), Value::String(text));
                                                payload_fields += 1;
                                            }
                                        } else if let Some(rest) = parent_id.strip_prefix("t3_") {
                                            if let Some((title, selftext)) = load_shard_value(
                                                parents_s_eager,
                                                submission_shards,
                                                &mut s_cache,
                                                rest,
                                                own_ym,
                                            )? {
                                                parent_obj.insert(
                                                    "kind".into(),
                                                    Value::String("submission".into()),
                                                );
                                                parent_obj.insert(
                                                    "id".into(),
                                                    Value::String(rest.to_string()),
                                                );
                                                parent_obj
                                                    .insert("title".into(), Value::String(title));
                                                parent_obj.insert(
                                                    "selftext".into(),
                                                    Value::String(selftext),
                                                );
                                                payload_fields += 2;
                                            }
                                        }

                                        if payload_fields > 0 {
                                            if let Some(map) = v.as_object_mut() {
                                                map.insert(
                                                    "parent".into(),
                                                    Value::Object(parent_obj),
                                                );
                                            }
                                            file_stats.resolved += 1;
                                        } else {
                                            file_stats.unresolved += 1;
                                        }
                                    }
                                }

                                serde_json::to_writer(&mut *w, &v)?;
                                w.write_all(b"\n")?;
                            }

                            Ok(file_stats)
                        },
                    )?;

                    if let Some(pb) = &pb {
                        pb.inc(1);
                    }
                    attached.lock().unwrap()[*idx] = Some((out_path, file_stats));
                    Ok(())
                },
            )?;

            if let Some(pb) = pb {
                let final_msg = format!("{label} done");
                pb.finish_with_message(final_msg);
            }

            let mut stats = ParentAttachStats::default();
            let out_paths = attached
                .into_inner()
                .unwrap()
                .into_iter()
                .map(|entry| {
                    let (path, file_stats) = entry.expect("attach result missing after success");
                    stats.add(file_stats);
                    path
                })
                .collect();
            Ok((out_paths, stats))
        })
    }
}
