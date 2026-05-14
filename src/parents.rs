use crate::atomic_write::{
    ensure_staging_dir, sweep_stale_inprogress, write_jsonl_atomic, INPROGRESS_EXT,
};
use crate::date::YearMonth;
use crate::filters::ym_from_epoch;
use crate::json_utils::is_comment_record_for_parent_attach;
use crate::mem::{available_memory_fraction, is_low_memory};
use crate::ndjson::{read_line_capped, DEFAULT_MAX_LINE_BYTES};
use crate::parents_ids::{IdShards, SharedIdsetCache, WorkerShardCache};
use crate::paths::{discover_all, plan_files_checked, FileJob, FileKind};
use crate::pipeline::RedditETL;
use crate::progress::{make_count_progress, make_progress_bar_labeled, total_compressed_size};
use crate::util::{
    create_dir_all_with_backoff, create_with_backoff, open_with_backoff, read_dir_with_backoff,
    remove_with_backoff, replace_file_atomic_backoff, with_thread_pool,
};
use crate::zstd_jsonl::{
    for_each_line_with_progress_cfg_no_throttle_status, malformed_json_error, parse_minimal,
};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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
const ATTACH_FINGERPRINT_VERSION: u32 = 1;
const ATTACH_FORMAT_VERSION: u32 = 1;
const ATTACH_SIDECAR_SUFFIX: &str = ".parents-attach.json";
const RESOLVER_FINGERPRINT_VERSION: u32 = 1;
const RESOLVER_FORMAT_VERSION: u32 = 1;
const RESOLVER_SIDECAR_SUFFIX: &str = ".parents-resolve.json";
const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ParentAttachDiagnostics {
    files_scanned: u64,
    resume_skipped_files: u64,
    parsed_records: u64,
    rc_records: u64,
    records_with_body: u64,
    records_with_parent_id: u64,
    records_with_link_id: u64,
    comment_shaped_records: u64,
}

impl ParentAttachDiagnostics {
    fn add(&mut self, other: Self) {
        self.files_scanned += other.files_scanned;
        self.resume_skipped_files += other.resume_skipped_files;
        self.parsed_records += other.parsed_records;
        self.rc_records += other.rc_records;
        self.records_with_body += other.records_with_body;
        self.records_with_parent_id += other.records_with_parent_id;
        self.records_with_link_id += other.records_with_link_id;
        self.comment_shaped_records += other.comment_shaped_records;
    }
}

const ATTACH_INITIAL_DIAGNOSTIC_SAMPLE_RECORDS: u64 = 100;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ParentAttachInitialShape {
    parsed_records: u64,
    records_with_body: u64,
    records_with_parent_id: u64,
    records_with_link_id: u64,
    records_with_body_and_parent_id: u64,
    records_with_prefixed_parent_id: u64,
}

impl ParentAttachInitialShape {
    fn observe(&mut self, v: &Value) {
        let has_body = v.get("body").is_some();
        let parent_id = v.get("parent_id").and_then(|x| x.as_str());
        let has_parent_id = v.get("parent_id").is_some();
        let has_link_id = v.get("link_id").is_some();

        self.parsed_records += 1;
        if has_body {
            self.records_with_body += 1;
        }
        if has_parent_id {
            self.records_with_parent_id += 1;
        }
        if has_link_id {
            self.records_with_link_id += 1;
        }
        if has_body && has_parent_id {
            self.records_with_body_and_parent_id += 1;
        }
        if parent_id
            .map(|id| id.starts_with("t1_") || id.starts_with("t3_"))
            .unwrap_or(false)
        {
            self.records_with_prefixed_parent_id += 1;
        }
    }

    fn no_legacy_comment_shape(self) -> bool {
        self.parsed_records > 0 && self.records_with_body_and_parent_id == 0
    }

    fn observed_shape(self) -> &'static str {
        match (self.records_with_parent_id > 0, self.records_with_body > 0) {
            (true, false) => "sample records had `parent_id` but no `body`",
            (false, true) => "sample records had `body` but no `parent_id`",
            (false, false) => "sample records had neither `body` nor `parent_id`",
            (true, true) => {
                "sample records mixed `body` and `parent_id`, but no single record had both"
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct AttachFingerprint {
    version: u32,
    attach_format_version: u32,
    input: AttachFileIdentity,
    resolution_range: AttachResolutionRange,
    parent_cache: AttachParentCacheFingerprint,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct AttachResolutionRange {
    start: Option<String>,
    end: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct AttachParentCacheFingerprint {
    comment_shards: AttachShardSetFingerprint,
    submission_shards: AttachShardSetFingerprint,
    eager_comments: AttachMapDigest,
    eager_submissions: AttachMapDigest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct AttachShardSetFingerprint {
    index_present: bool,
    shards: u64,
    digest: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct AttachMapDigest {
    entries: u64,
    digest: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct AttachFileIdentity {
    path: String,
    exists: bool,
    len: Option<u64>,
    modified_unix_secs: Option<i64>,
    modified_nanos: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ResolverFingerprint {
    version: u32,
    resolver_format_version: u32,
    source: ResolverSourceFingerprint,
    resolution_range: AttachResolutionRange,
    parent_ids: ParentIdsFingerprint,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ResolverSourceFingerprint {
    kind: String,
    month: String,
    file: AttachFileIdentity,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ParentIdsFingerprint {
    t1: ParentIdSetFingerprint,
    t3: ParentIdSetFingerprint,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ParentIdSetFingerprint {
    kind: String,
    storage: String,
    ids: u64,
    digest: String,
    shard_count: u64,
    backing_shards: Vec<ParentIdShardFingerprint>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ParentIdShardFingerprint {
    index: u64,
    ids: u64,
    digest: String,
    len: u64,
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
    /// Return true when neither the comment (`t1_`) nor submission (`t3_`)
    /// parent-id backing contains any IDs.
    pub fn is_empty(&self) -> bool {
        fn backing_empty(mem: Option<&AHashSet<String>>, sharded: Option<&IdShards>) -> bool {
            mem.map_or(true, |ids| ids.is_empty())
                && sharded.map_or(true, |shards| shards.total_ids == 0)
        }

        backing_empty(self.t1_ids_mem.as_ref(), self.t1_ids_sharded.as_ref())
            && backing_empty(self.t3_ids_mem.as_ref(), self.t3_ids_sharded.as_ref())
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
    parent_ids_fp: &ParentIdsFingerprint,
    resolution_range: &AttachResolutionRange,
    comments_out: &Path,
    submissions_out: &Path,
    resume: bool,
    read_buf: usize,
    write_buf: usize,
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

    // Limit file concurrency to reduce RAM spikes while resolving.
    crate::concurrency::for_each_file_limited(files, file_concurrency, |job| -> Result<()> {
        let (out_dir, prefix) = match job.kind {
            FileKind::Comment => (comments_out, "RC"),
            FileKind::Submission => (submissions_out, "RS"),
        };
        let out = out_dir.join(format!("{}_{}.json", prefix, job.ym));
        let sidecar_path = resolver_fingerprint_path(&out);
        let fingerprint = build_resolver_fingerprint(job, parent_ids_fp, resolution_range);

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
            // Validate both the shard JSON and its resolver fingerprint. The
            // sidecar binds this cache shard to the current ParentIds, source
            // corpus file, source kind/month, resolver format, and requested
            // resolver window; stale-but-parseable shards are rebuilt.
            let valid_json = match open_with_backoff(&out, 16, 50) {
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
            if valid_json && resolver_fingerprint_matches(&sidecar_path, &fingerprint) {
                record_shard(out.clone());
                if let Some(pb) = pb {
                    let sz = fs::metadata(&job.path).map(|m| m.len()).unwrap_or(0);
                    pb.inc(sz);
                }
                return Ok(());
            }
            if !valid_json {
                tracing::warn!(path=%out.display(), "resume: existing parent shard is unreadable/corrupt, rebuilding");
            }
        }

        let ensure_contains = |shard_path: &Path, id: &str| -> Result<bool> {
            let set = idset_cache.get_or_load(shard_path)?;
            Ok(set.contains(id))
        };

        let mut out_map_c: HashMap<String, String> = HashMap::new();
        let mut out_map_s: HashMap<String, (String, String)> = HashMap::new();

        let mut line_number = 0u64;
        let completed = for_each_line_with_progress_cfg_no_throttle_status(
            &job.path,
            read_buf,
            |d| {
                if let Some(pb) = pb {
                    pb.inc(d);
                }
            },
            |line| {
                line_number += 1;
                let min = parse_minimal(line)
                    .map_err(|e| malformed_json_error(&job.path, line_number, e))?;
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
        if !completed {
            return Err(anyhow::anyhow!(
                "incomplete zstd decode while resolving parent map from {}",
                job.path.display()
            ));
        }

        // Atomic write: serialize to a sibling .tmp first, fsync the data
        // path via BufWriter::flush, then atomically replace the dest.
        // Prevents readers from observing torn / half-written shard JSON
        // after a crash mid-write.
        let tmp = out.with_extension("json.tmp");
        let write_res = (|| -> Result<()> {
            let f = create_with_backoff(&tmp, 16, 50)
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

        if let Err(e) = write_res {
            let _ = remove_with_backoff(&tmp, 8, 50);
            return Err(e).with_context(|| format!("write parent shard {}", out.display()));
        }

        write_resolver_fingerprint_atomic(&sidecar_path, &fingerprint, write_buf)?;
        record_shard(out.clone());

        Ok(())
    })?;

    Ok((comment_shards.into_inner(), submission_shards.into_inner()))
}

fn attach_inprogress_exists(staging_dir: &Path, final_dest: &Path) -> Result<bool> {
    let file_name = final_dest
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "final_dest has no UTF-8 file name: {}",
                final_dest.display()
            )
        })?;
    if !staging_dir.exists() {
        return Ok(false);
    }
    for entry in fs::read_dir(staging_dir)
        .with_context(|| format!("read staging dir {}", staging_dir.display()))?
    {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.starts_with(file_name) && name.ends_with(INPROGRESS_EXT) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn attach_fingerprint_path(final_dest: &Path) -> PathBuf {
    let mut sidecar = final_dest.as_os_str().to_os_string();
    sidecar.push(ATTACH_SIDECAR_SUFFIX);
    PathBuf::from(sidecar)
}

fn system_time_parts(t: SystemTime) -> (i64, u32) {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => (
            i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
            d.subsec_nanos(),
        ),
        Err(e) => {
            let d = e.duration();
            (
                -i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
                d.subsec_nanos(),
            )
        }
    }
}

fn attach_file_identity(path: &Path) -> AttachFileIdentity {
    let stable_path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let metadata = fs::metadata(path).ok();
    let modified = metadata
        .as_ref()
        .and_then(|m| m.modified().ok())
        .map(system_time_parts);
    let (modified_unix_secs, modified_nanos) = match modified {
        Some((secs, nanos)) => (Some(secs), Some(nanos)),
        None => (None, None),
    };

    AttachFileIdentity {
        path: stable_path.to_string_lossy().into_owned(),
        exists: metadata.is_some(),
        len: metadata.as_ref().map(|m| m.len()),
        modified_unix_secs,
        modified_nanos,
    }
}

fn update_digest_bytes(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(FNV_PRIME);
    }
}

fn update_digest_bool(hash: &mut u64, value: bool) {
    update_digest_bytes(hash, &[u8::from(value)]);
}

fn update_digest_u32(hash: &mut u64, value: u32) {
    update_digest_bytes(hash, &value.to_le_bytes());
}

fn update_digest_u64(hash: &mut u64, value: u64) {
    update_digest_bytes(hash, &value.to_le_bytes());
}

fn update_digest_i64(hash: &mut u64, value: i64) {
    update_digest_bytes(hash, &value.to_le_bytes());
}

fn update_digest_str(hash: &mut u64, value: &str) {
    update_digest_u64(hash, value.len() as u64);
    update_digest_bytes(hash, value.as_bytes());
}

fn update_digest_option_u64(hash: &mut u64, value: Option<u64>) {
    update_digest_bool(hash, value.is_some());
    if let Some(value) = value {
        update_digest_u64(hash, value);
    }
}

fn update_digest_option_i64(hash: &mut u64, value: Option<i64>) {
    update_digest_bool(hash, value.is_some());
    if let Some(value) = value {
        update_digest_i64(hash, value);
    }
}

fn update_digest_option_u32(hash: &mut u64, value: Option<u32>) {
    update_digest_bool(hash, value.is_some());
    if let Some(value) = value {
        update_digest_u32(hash, value);
    }
}

fn update_file_identity_digest(hash: &mut u64, identity: &AttachFileIdentity) {
    update_digest_str(hash, &identity.path);
    update_digest_bool(hash, identity.exists);
    update_digest_option_u64(hash, identity.len);
    update_digest_option_i64(hash, identity.modified_unix_secs);
    update_digest_option_u32(hash, identity.modified_nanos);
}

fn attach_shard_set_fingerprint(
    shards: Option<&HashMap<YearMonth, PathBuf>>,
) -> AttachShardSetFingerprint {
    let index_present = shards.is_some();
    let mut entries = Vec::new();
    if let Some(shards) = shards {
        entries.extend(
            shards
                .iter()
                .map(|(ym, path)| (*ym, attach_file_identity(path))),
        );
    }
    entries.sort_by(|(a_ym, a_id), (b_ym, b_id)| {
        a_ym.cmp(b_ym).then_with(|| a_id.path.cmp(&b_id.path))
    });

    let mut hash = FNV_OFFSET_BASIS;
    update_digest_bool(&mut hash, index_present);
    update_digest_u64(&mut hash, entries.len() as u64);
    for (ym, identity) in &entries {
        update_digest_str(&mut hash, &ym.to_string());
        update_file_identity_digest(&mut hash, identity);
    }

    AttachShardSetFingerprint {
        index_present,
        shards: entries.len() as u64,
        digest: format!("{hash:016x}"),
    }
}

fn finish_unordered_digest_string(entries: u64, sum: u64, xor: u64) -> String {
    let mut hash = FNV_OFFSET_BASIS;
    update_digest_u64(&mut hash, entries);
    update_digest_u64(&mut hash, sum);
    update_digest_u64(&mut hash, xor);
    format!("{hash:016x}")
}

fn finish_unordered_digest(entries: usize, sum: u64, xor: u64) -> AttachMapDigest {
    AttachMapDigest {
        entries: entries as u64,
        digest: finish_unordered_digest_string(entries as u64, sum, xor),
    }
}

fn attach_comment_map_digest(map: &HashMap<String, String>) -> AttachMapDigest {
    let mut sum = 0u64;
    let mut xor = 0u64;
    for (id, body) in map {
        let mut entry_hash = FNV_OFFSET_BASIS;
        update_digest_str(&mut entry_hash, id);
        update_digest_str(&mut entry_hash, body);
        sum = sum.wrapping_add(entry_hash);
        xor ^= entry_hash.rotate_left((entry_hash & 63) as u32);
    }
    finish_unordered_digest(map.len(), sum, xor)
}

fn attach_submission_map_digest(map: &HashMap<String, (String, String)>) -> AttachMapDigest {
    let mut sum = 0u64;
    let mut xor = 0u64;
    for (id, (title, selftext)) in map {
        let mut entry_hash = FNV_OFFSET_BASIS;
        update_digest_str(&mut entry_hash, id);
        update_digest_str(&mut entry_hash, title);
        update_digest_str(&mut entry_hash, selftext);
        sum = sum.wrapping_add(entry_hash);
        xor ^= entry_hash.rotate_left((entry_hash & 63) as u32);
    }
    finish_unordered_digest(map.len(), sum, xor)
}

fn update_unordered_id_digest(count: &mut u64, sum: &mut u64, xor: &mut u64, id: &str) {
    let mut entry_hash = FNV_OFFSET_BASIS;
    update_digest_str(&mut entry_hash, id);
    *count += 1;
    *sum = sum.wrapping_add(entry_hash);
    *xor ^= entry_hash.rotate_left((entry_hash & 63) as u32);
}

fn fingerprint_mem_id_set(kind: &str, ids: &AHashSet<String>) -> ParentIdSetFingerprint {
    let mut count = 0u64;
    let mut sum = 0u64;
    let mut xor = 0u64;
    for id in ids {
        update_unordered_id_digest(&mut count, &mut sum, &mut xor, id);
    }
    ParentIdSetFingerprint {
        kind: kind.to_string(),
        storage: "memory".to_string(),
        ids: count,
        digest: finish_unordered_digest_string(count, sum, xor),
        shard_count: 0,
        backing_shards: Vec::new(),
    }
}

fn fingerprint_empty_id_set(kind: &str) -> ParentIdSetFingerprint {
    ParentIdSetFingerprint {
        kind: kind.to_string(),
        storage: "empty".to_string(),
        ids: 0,
        digest: finish_unordered_digest_string(0, 0, 0),
        shard_count: 0,
        backing_shards: Vec::new(),
    }
}

fn fingerprint_id_shard_file(path: &Path) -> Result<(u64, u64, u64)> {
    let f = open_with_backoff(path, 16, 50)
        .with_context(|| format!("open parent-id shard {}", path.display()))?;
    let mut r = BufReader::new(f);
    let mut count = 0u64;
    let mut sum = 0u64;
    let mut xor = 0u64;
    let mut id = String::new();
    loop {
        let n = read_line_capped(&mut r, &mut id, DEFAULT_MAX_LINE_BYTES, path)
            .with_context(|| format!("read parent-id shard {}", path.display()))?;
        if n == 0 {
            break;
        }
        if !id.is_empty() {
            update_unordered_id_digest(&mut count, &mut sum, &mut xor, &id);
        }
    }
    Ok((count, sum, xor))
}

fn fingerprint_sharded_id_set(kind: &str, shards: &IdShards) -> Result<ParentIdSetFingerprint> {
    let mut total_count = 0u64;
    let mut total_sum = 0u64;
    let mut total_xor = 0u64;
    let mut backing_shards = Vec::with_capacity(shards.count);

    for idx in 0..shards.count {
        let path = shards.path_for(idx);
        let metadata = fs::metadata(&path)
            .with_context(|| format!("stat parent-id shard {}", path.display()))?;
        let (count, sum, xor) = fingerprint_id_shard_file(&path)?;
        total_count += count;
        total_sum = total_sum.wrapping_add(sum);
        total_xor ^= xor;
        backing_shards.push(ParentIdShardFingerprint {
            index: idx as u64,
            ids: count,
            digest: finish_unordered_digest_string(count, sum, xor),
            len: metadata.len(),
        });
    }

    Ok(ParentIdSetFingerprint {
        kind: kind.to_string(),
        storage: "sharded".to_string(),
        ids: total_count,
        digest: finish_unordered_digest_string(total_count, total_sum, total_xor),
        shard_count: shards.count as u64,
        backing_shards,
    })
}

fn parent_id_set_fingerprint(
    kind: &str,
    mem: Option<&AHashSet<String>>,
    sharded: Option<&IdShards>,
) -> Result<ParentIdSetFingerprint> {
    if let Some(shards) = sharded {
        fingerprint_sharded_id_set(kind, shards)
    } else if let Some(ids) = mem {
        Ok(fingerprint_mem_id_set(kind, ids))
    } else {
        Ok(fingerprint_empty_id_set(kind))
    }
}

fn parent_ids_fingerprint(ids: &ParentIds) -> Result<ParentIdsFingerprint> {
    Ok(ParentIdsFingerprint {
        t1: parent_id_set_fingerprint("t1", ids.t1_ids_mem.as_ref(), ids.t1_ids_sharded.as_ref())?,
        t3: parent_id_set_fingerprint("t3", ids.t3_ids_mem.as_ref(), ids.t3_ids_sharded.as_ref())?,
    })
}

fn attach_parent_cache_fingerprint(parents: &ParentMaps) -> AttachParentCacheFingerprint {
    AttachParentCacheFingerprint {
        comment_shards: attach_shard_set_fingerprint(parents.comment_shards.as_ref()),
        submission_shards: attach_shard_set_fingerprint(parents.submission_shards.as_ref()),
        eager_comments: attach_comment_map_digest(&parents.comments),
        eager_submissions: attach_submission_map_digest(&parents.submissions),
    }
}

fn attach_resolution_range(
    start: Option<YearMonth>,
    end: Option<YearMonth>,
) -> AttachResolutionRange {
    AttachResolutionRange {
        start: start.map(|ym| ym.to_string()),
        end: end.map(|ym| ym.to_string()),
    }
}

fn build_attach_fingerprint(
    input: &Path,
    parent_cache: &AttachParentCacheFingerprint,
    resolution_range: &AttachResolutionRange,
) -> AttachFingerprint {
    AttachFingerprint {
        version: ATTACH_FINGERPRINT_VERSION,
        attach_format_version: ATTACH_FORMAT_VERSION,
        input: attach_file_identity(input),
        resolution_range: resolution_range.clone(),
        parent_cache: parent_cache.clone(),
    }
}

fn resolver_fingerprint_path(final_dest: &Path) -> PathBuf {
    let mut sidecar = final_dest.as_os_str().to_os_string();
    sidecar.push(RESOLVER_SIDECAR_SUFFIX);
    PathBuf::from(sidecar)
}

fn resolver_kind_label(kind: FileKind) -> &'static str {
    match kind {
        FileKind::Comment => "comments",
        FileKind::Submission => "submissions",
    }
}

fn build_resolver_fingerprint(
    job: &FileJob,
    parent_ids: &ParentIdsFingerprint,
    resolution_range: &AttachResolutionRange,
) -> ResolverFingerprint {
    ResolverFingerprint {
        version: RESOLVER_FINGERPRINT_VERSION,
        resolver_format_version: RESOLVER_FORMAT_VERSION,
        source: ResolverSourceFingerprint {
            kind: resolver_kind_label(job.kind).to_string(),
            month: job.ym.to_string(),
            file: attach_file_identity(&job.path),
        },
        resolution_range: resolution_range.clone(),
        parent_ids: parent_ids.clone(),
    }
}

fn resolver_fingerprint_matches(sidecar_path: &Path, expected: &ResolverFingerprint) -> bool {
    match open_with_backoff(sidecar_path, 16, 50) {
        Ok(f) => match serde_json::from_reader::<_, ResolverFingerprint>(BufReader::new(f)) {
            Ok(actual) if &actual == expected => true,
            Ok(_) => {
                tracing::debug!(path=%sidecar_path.display(), "resume: parent resolver fingerprint changed, rebuilding");
                false
            }
            Err(e) => {
                tracing::warn!(path=%sidecar_path.display(), error=%e, "resume: parent resolver fingerprint sidecar is unreadable, rebuilding");
                false
            }
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::debug!(path=%sidecar_path.display(), "resume: parent resolver fingerprint sidecar missing, rebuilding");
            false
        }
        Err(e) => {
            tracing::warn!(path=%sidecar_path.display(), error=%e, "resume: parent resolver fingerprint sidecar cannot be opened, rebuilding");
            false
        }
    }
}

fn write_resolver_fingerprint_atomic(
    sidecar_path: &Path,
    fingerprint: &ResolverFingerprint,
    write_buf_bytes: usize,
) -> Result<()> {
    let mut staged = sidecar_path.as_os_str().to_os_string();
    staged.push(INPROGRESS_EXT);
    let staged = PathBuf::from(staged);

    if let Some(parent) = sidecar_path.parent() {
        create_dir_all_with_backoff(parent, 16, 50)
            .with_context(|| format!("create resolver sidecar parent {}", parent.display()))?;
    }

    let file = create_with_backoff(&staged, 16, 50)
        .with_context(|| format!("create staged resolver fingerprint {}", staged.display()))?;
    let mut writer = BufWriter::with_capacity(write_buf_bytes, file);
    let write_result = (|| -> Result<()> {
        serde_json::to_writer_pretty(&mut writer, fingerprint)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        Ok(())
    })();

    if let Err(e) = write_result {
        drop(writer);
        let _ = remove_with_backoff(&staged, 8, 50);
        return Err(e)
            .with_context(|| format!("write staged resolver fingerprint {}", staged.display()));
    }
    drop(writer);

    if let Err(e) = replace_file_atomic_backoff(&staged, sidecar_path) {
        let _ = remove_with_backoff(&staged, 8, 50);
        return Err(e).with_context(|| {
            format!(
                "publish resolver fingerprint sidecar {}",
                sidecar_path.display()
            )
        });
    }
    Ok(())
}

fn attach_fingerprint_matches(sidecar_path: &Path, expected: &AttachFingerprint) -> bool {
    match open_with_backoff(sidecar_path, 16, 50) {
        Ok(f) => match serde_json::from_reader::<_, AttachFingerprint>(BufReader::new(f)) {
            Ok(actual) if &actual == expected => true,
            Ok(_) => {
                tracing::debug!(path=%sidecar_path.display(), "resume: attach fingerprint changed, rebuilding");
                false
            }
            Err(e) => {
                tracing::warn!(path=%sidecar_path.display(), error=%e, "resume: attach fingerprint sidecar is unreadable, rebuilding");
                false
            }
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::debug!(path=%sidecar_path.display(), "resume: attach fingerprint sidecar missing, rebuilding");
            false
        }
        Err(e) => {
            tracing::warn!(path=%sidecar_path.display(), error=%e, "resume: attach fingerprint sidecar cannot be opened, rebuilding");
            false
        }
    }
}

fn write_attach_fingerprint_atomic(
    staging_dir: &Path,
    sidecar_path: &Path,
    fingerprint: &AttachFingerprint,
    write_buf_bytes: usize,
) -> Result<()> {
    let file_name = sidecar_path.file_name().ok_or_else(|| {
        anyhow::anyhow!(
            "attach fingerprint sidecar has no file name: {}",
            sidecar_path.display()
        )
    })?;
    let mut staged = staging_dir.join(file_name);
    staged.as_mut_os_string().push(INPROGRESS_EXT);

    create_dir_all_with_backoff(staging_dir, 16, 50)
        .with_context(|| format!("create staging dir {}", staging_dir.display()))?;
    if let Some(parent) = sidecar_path.parent() {
        create_dir_all_with_backoff(parent, 16, 50)
            .with_context(|| format!("create sidecar parent {}", parent.display()))?;
    }

    let file = create_with_backoff(&staged, 16, 50)
        .with_context(|| format!("create staged attach fingerprint {}", staged.display()))?;
    let mut writer = BufWriter::with_capacity(write_buf_bytes, file);
    let write_result = (|| -> Result<()> {
        serde_json::to_writer_pretty(&mut writer, fingerprint)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        Ok(())
    })();

    if let Err(e) = write_result {
        drop(writer);
        let _ = remove_with_backoff(&staged, 8, 50);
        return Err(e)
            .with_context(|| format!("write staged attach fingerprint {}", staged.display()));
    }
    drop(writer);

    if let Err(e) = replace_file_atomic_backoff(&staged, sidecar_path) {
        let _ = remove_with_backoff(&staged, 8, 50);
        return Err(e).with_context(|| {
            format!(
                "publish attach fingerprint sidecar {}",
                sidecar_path.display()
            )
        });
    }
    Ok(())
}

fn validate_jsonl_file(path: &Path) -> bool {
    let Ok(f) = open_with_backoff(path, 16, 50) else {
        return false;
    };
    let mut r = BufReader::new(f);
    let mut buf = String::new();
    loop {
        match read_line_capped(&mut r, &mut buf, DEFAULT_MAX_LINE_BYTES, path) {
            Ok(0) => return true,
            Ok(_) => {
                if buf.trim().is_empty() {
                    continue;
                }
                if serde_json::from_str::<Value>(&buf).is_err() {
                    return false;
                }
            }
            Err(_) => return false,
        }
    }
}

fn looks_like_rc_spool_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.starts_with("part_RC_") || name.starts_with("RC_"))
        .unwrap_or(false)
}

fn diagnose_initial_attach_shape(inputs: &[(usize, PathBuf)], read_buf: usize) -> Result<()> {
    let Some((_, first_input)) = inputs.first() else {
        return Ok(());
    };

    let f = open_with_backoff(first_input, 16, 50)
        .with_context(|| format!("open initial parent attach input {}", first_input.display()))?;
    let mut r = BufReader::with_capacity(read_buf, f);
    let mut buf = String::with_capacity(64 * 1024);
    let mut line_number = 0u64;
    let mut shape = ParentAttachInitialShape::default();

    while shape.parsed_records < ATTACH_INITIAL_DIAGNOSTIC_SAMPLE_RECORDS {
        let n = read_line_capped(&mut r, &mut buf, DEFAULT_MAX_LINE_BYTES, first_input)
            .with_context(|| {
                format!(
                    "read initial parent attach input {} near line {}",
                    first_input.display(),
                    line_number + 1
                )
            })?;
        if n == 0 {
            break;
        }
        line_number += 1;
        if buf.trim().is_empty() {
            continue;
        }
        let v: Value = serde_json::from_str(&buf).with_context(|| {
            format!(
                "malformed JSON in initial parent attach input {} at line {}",
                first_input.display(),
                line_number
            )
        })?;
        shape.observe(&v);
    }

    if shape.no_legacy_comment_shape()
        && (shape.records_with_parent_id > 0 || looks_like_rc_spool_path(first_input))
    {
        let observed_shape = shape.observed_shape();
        tracing::error!(
            path = %first_input.display(),
            sampled_records = shape.parsed_records,
            records_with_body = shape.records_with_body,
            records_with_parent_id = shape.records_with_parent_id,
            records_with_link_id = shape.records_with_link_id,
            records_with_prefixed_parent_id = shape.records_with_prefixed_parent_id,
            "parents attach initial sample found no records with both `body` and `parent_id` ({observed_shape}); using relaxed parent_id-based matching for records whose `parent_id` starts with `t1_`/`t3_`; if this spool was produced with --whitelist/.whitelist_fields, include body,parent_id,link_id to retain child comment text and parent-resolution context"
        );
    }

    Ok(())
}

fn warn_if_no_comment_shaped_records(diagnostics: ParentAttachDiagnostics) {
    if diagnostics.files_scanned == 0
        || diagnostics.parsed_records == 0
        || diagnostics.comment_shaped_records > 0
    {
        return;
    }

    tracing::warn!(
        scanned_files = diagnostics.files_scanned,
        resume_skipped_files = diagnostics.resume_skipped_files,
        records = diagnostics.parsed_records,
        rc_records = diagnostics.rc_records,
        records_with_body = diagnostics.records_with_body,
        records_with_parent_id = diagnostics.records_with_parent_id,
        records_with_link_id = diagnostics.records_with_link_id,
        "parents attach found zero records with a usable `parent_id`; comment records must include a `parent_id` starting with `t1_` or `t3_`, and parent resolution also benefits from `link_id`; if the spool was produced with --whitelist/.whitelist_fields, include parent_id,link_id (and body if you need the child comment text) or the output will be a no-op copy"
    );
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
            create_dir_all_with_backoff(&comments_out, 16, 50).with_context(|| {
                format!(
                    "create comments parent cache dir {}",
                    comments_out.display()
                )
            })?;
            create_dir_all_with_backoff(&submissions_out, 16, 50).with_context(|| {
                format!(
                    "create submissions parent cache dir {}",
                    submissions_out.display()
                )
            })?;

            let discovered = discover_all(&self.opts.comments_dir, &self.opts.submissions_dir);
            let files = plan_files_checked(
                &discovered,
                &self.opts.comments_dir,
                &self.opts.submissions_dir,
                crate::config::Sources::Both,
                self.opts.start,
                self.opts.end,
            )
            .with_context(|| {
                format!(
                    "resolve_parent_maps planned zero corpus files for resolver range {:?}..={:?}",
                    self.opts.start, self.opts.end
                )
            })?;
            let parent_ids_fp = parent_ids_fingerprint(ids)?;
            let resolution_range = attach_resolution_range(self.opts.start, self.opts.end);

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
                &parent_ids_fp,
                &resolution_range,
                &comments_out,
                &submissions_out,
                resume,
                self.opts.read_buffer_bytes,
                self.opts.write_buffer_bytes,
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
                    let mut v: Vec<PathBuf> = read_dir_with_backoff(dir, 16, 50)
                        .map_err(|e| {
                            tracing::warn!(dir=%dir.display(), error=%e, "failed to eager-list parent cache shards");
                            e
                        })
                        .ok()
                        .into_iter()
                        .flat_map(|entries| entries.into_iter().map(|e| e.path()))
                        .filter(|p| {
                            p.file_name()
                                .and_then(|name| name.to_str())
                                .map(|name| {
                                    name.ends_with(".json")
                                        && !name.ends_with(RESOLVER_SIDECAR_SUFFIX)
                                        && !name.ends_with(INPROGRESS_EXT)
                                })
                                .unwrap_or(false)
                        })
                        .collect();
                    v.sort();
                    v
                };

                for p in load(&comments_out) {
                    let f = open_with_backoff(&p, 16, 50)?;
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
                    let f = open_with_backoff(&p, 16, 50)?;
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
            create_dir_all_with_backoff(out_dir, 16, 50).with_context(|| {
                format!("create parent attach output dir {}", out_dir.display())
            })?;
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
            let parent_cache_fingerprint = attach_parent_cache_fingerprint(parents);
            let resolution_range = attach_resolution_range(self.opts.start, self.opts.end);

            let indexed_inputs: Vec<(usize, PathBuf)> = inputs.into_iter().enumerate().collect();
            diagnose_initial_attach_shape(&indexed_inputs, self.opts.read_buffer_bytes)?;
            let attached = std::sync::Mutex::new(vec![None; indexed_inputs.len()]);

            crate::concurrency::for_each_file_limited(
                &indexed_inputs,
                self.opts.file_concurrency,
                |(idx, in_path)| -> Result<()> {
                    let name = in_path.file_name().unwrap().to_string_lossy().to_string();
                    let out_path = out_dir.join(name);
                    let inprogress_exists = attach_inprogress_exists(&staging_dir, &out_path)?;
                    let sidecar_path = attach_fingerprint_path(&out_path);
                    let fingerprint = build_attach_fingerprint(
                        in_path,
                        &parent_cache_fingerprint,
                        &resolution_range,
                    );

                    if resume && out_path.exists() && !inprogress_exists {
                        if attach_fingerprint_matches(&sidecar_path, &fingerprint) {
                            if validate_jsonl_file(&out_path) {
                                if let Some(pb) = &pb {
                                    pb.inc(1);
                                }
                                let diagnostics = ParentAttachDiagnostics {
                                    resume_skipped_files: 1,
                                    ..Default::default()
                                };
                                attached.lock().unwrap()[*idx] =
                                    Some((out_path, ParentAttachStats::default(), diagnostics));
                                return Ok(());
                            }
                            tracing::warn!(path=%out_path.display(), "resume: existing attached JSONL is unreadable/corrupt, rebuilding");
                        } else {
                            tracing::debug!(path=%out_path.display(), sidecar=%sidecar_path.display(), "resume: attached JSONL fingerprint missing or stale, rebuilding");
                        }
                    }

                    // Per-worker FIFO caches of parsed shard JSON. See
                    // `WorkerShardCache` for why eviction is plain FIFO with no
                    // bump-on-hit.
                    let mut c_cache = WorkerShardCache::<String>::new(COMMENT_SHARD_CACHE_CAP);
                    let mut s_cache =
                        WorkerShardCache::<(String, String)>::new(SUBMISSION_SHARD_CACHE_CAP);

                    let is_rc_spool_part = looks_like_rc_spool_path(in_path);
                    let (file_stats, diagnostics) = write_jsonl_atomic(
                        &staging_dir,
                        &out_path,
                        self.opts.write_buffer_bytes,
                        |w| -> Result<(ParentAttachStats, ParentAttachDiagnostics)> {
                            let mut file_stats = ParentAttachStats::default();
                            let mut diagnostics = ParentAttachDiagnostics {
                                files_scanned: 1,
                                ..Default::default()
                            };
                            let f = open_with_backoff(in_path, 16, 50)?;
                            let mut r = BufReader::new(f);
                            let mut line_buf = String::new();
                            let mut line_no: u64 = 0;

                            loop {
                                let n = read_line_capped(
                                    &mut r,
                                    &mut line_buf,
                                    DEFAULT_MAX_LINE_BYTES,
                                    in_path,
                                )
                                .with_context(|| {
                                    format!(
                                        "read parent attach input {} at line {}",
                                        in_path.display(),
                                        line_no + 1
                                    )
                                })?;
                                if n == 0 {
                                    break;
                                }
                                line_no += 1;
                                if line_buf.is_empty() {
                                    continue;
                                }
                                let mut v: Value =
                                    serde_json::from_str(&line_buf).with_context(|| {
                                        format!(
                                            "malformed JSON in parent attach input {} at line {}",
                                            in_path.display(),
                                            line_no
                                        )
                                    })?;

                                let has_body = v.get("body").is_some();
                                let has_parent_id = v.get("parent_id").is_some();
                                let has_link_id = v.get("link_id").is_some();
                                diagnostics.parsed_records += 1;
                                if is_rc_spool_part {
                                    diagnostics.rc_records += 1;
                                }
                                if has_body {
                                    diagnostics.records_with_body += 1;
                                }
                                if has_parent_id {
                                    diagnostics.records_with_parent_id += 1;
                                }
                                if has_link_id {
                                    diagnostics.records_with_link_id += 1;
                                }

                                let is_comment = is_comment_record_for_parent_attach(&v);
                                if is_comment {
                                    diagnostics.comment_shaped_records += 1;
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

                            Ok((file_stats, diagnostics))
                        },
                    )?;

                    write_attach_fingerprint_atomic(
                        &staging_dir,
                        &sidecar_path,
                        &fingerprint,
                        self.opts.write_buffer_bytes,
                    )?;

                    if let Some(pb) = &pb {
                        pb.inc(1);
                    }
                    attached.lock().unwrap()[*idx] = Some((out_path, file_stats, diagnostics));
                    Ok(())
                },
            )?;

            if let Some(pb) = pb {
                let final_msg = format!("{label} done");
                pb.finish_with_message(final_msg);
            }

            let mut stats = ParentAttachStats::default();
            let mut diagnostics = ParentAttachDiagnostics::default();
            let out_paths = attached
                .into_inner()
                .unwrap()
                .into_iter()
                .map(|entry| {
                    let (path, file_stats, file_diagnostics) =
                        entry.expect("attach result missing after success");
                    stats.add(file_stats);
                    diagnostics.add(file_diagnostics);
                    path
                })
                .collect();
            warn_if_no_comment_shaped_records(diagnostics);
            Ok((out_paths, stats))
        })
    }
}
