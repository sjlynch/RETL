use crate::atomic_write::{
    ensure_staging_dir, sweep_stale_inprogress, write_json_pretty_atomic, write_jsonl_atomic,
    INPROGRESS_EXT,
};
use crate::date::YearMonth;
use crate::filters::ym_from_epoch;
use crate::json_utils::is_comment_record_for_parent_attach;
use crate::mem::{available_memory_fraction, is_low_memory};
use crate::ndjson::{read_line_capped, DEFAULT_MAX_LINE_BYTES};
use crate::parents_ids::{IdShards, SharedIdsetCache, WorkerShardCache};
use crate::paths::{discover_all_checked, plan_files_checked, FileJob, FileKind};
use crate::pipeline::RedditETL;
use crate::progress::{make_count_progress, make_progress_bar_labeled, total_compressed_size};
use crate::run_manifest::{
    discover_upstream_manifests_from_inputs, file_identities, maybe_write_run_manifest,
    path_to_stable_string, ManifestDestination, RunManifestInput, RunManifestStart,
};
use crate::util::{
    fnv1a_offset_basis, fnv1a_update,
    output_parent, system_time_parts, with_thread_pool,
};
use crate::zstd_jsonl::{
    for_each_line_with_progress_cfg_no_throttle_status, malformed_json_error, parse_minimal,
};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::io::BufReader;
use std::path::{Path, PathBuf};
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
const ATTACH_FINGERPRINT_VERSION: u32 = 1;
const ATTACH_FORMAT_VERSION: u32 = 1;
const ATTACH_SIDECAR_SUFFIX: &str = ".parents-attach.json";
const RESOLVER_FINGERPRINT_VERSION: u32 = 1;
const RESOLVER_FORMAT_VERSION: u32 = 1;
const LEGACY_PARENT_PAYLOAD_FORMAT_VERSION: u32 = 1;
const STRUCTURED_PARENT_PAYLOAD_FORMAT_VERSION: u32 = 2;
const RESOLVER_SIDECAR_SUFFIX: &str = ".parents-resolve.json";

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
