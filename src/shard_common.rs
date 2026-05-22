//! Shared sharded-writer hashing utility used by `shard.rs`, `kv_shard.rs`,
//! and the private id-shard writer in `parents.rs`.
//!
//! The three call sites independently maintained the same `ahash::RandomState`
//! + `idx()` pair with hard-coded magic seeds. Each writer's seed is
//! load-bearing — the on-disk shard layout (which `shard_NNNN.tmp` a key
//! lands in) is derived from it, and resume runs would re-shard if the
//! mapping changed. Per-label seeds are kept verbatim here so behavior is
//! preserved; new labels may pick fresh seeds, but **do not edit existing
//! label seeds**.

use crate::ndjson::{read_line_capped, DEFAULT_MAX_LINE_BYTES};
use ahash::{AHashSet, RandomState};
use anyhow::{Context, Result};
use parking_lot::Mutex;
use std::fs::File;
use std::hash::{BuildHasher, Hash, Hasher};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

/// Construct a deterministic [`RandomState`] for a given module label.
///
/// Labels correspond to existing call sites:
/// - `"usernames"` — `shard::ShardedWriter`
/// - `"kv"` — `kv_shard::ShardedKVWriter`
/// - `"parent_ids"` — `parents::IdShardWriter` / `parents::IdShards`
pub(crate) fn seeded_state(label: &str) -> RandomState {
    let (k0, k1, k2, k3) = match label {
        "usernames" => (
            0x1234_5678_9abc_def0,
            0x0fed_cba9_8765_4321,
            0xdead_beef_cafe_babe,
            0x0bad_f00d_face_feed,
        ),
        "kv" => (
            0x0123_4567_89ab_cdef,
            0xfedc_ba98_7654_3210,
            0xcafe_babe_dead_beef,
            0xface_feed_0bad_f00d,
        ),
        "parent_ids" => (
            0x2200_1100_3300_4400,
            0x5500_6600_7700_8800,
            0x9900_aa00_bb00_cc00,
            0xdd00_ee00_ff00_0123,
        ),
        other => panic!("seeded_state: unknown label {other:?}"),
    };
    RandomState::with_seeds(k0, k1, k2, k3)
}

/// Compute the shard index for `key` under `state`, modulo `count`.
#[inline]
pub(crate) fn shard_index(state: &RandomState, key: &str, count: usize) -> usize {
    let mut hasher = state.build_hasher();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % count
}

pub(crate) type LineShardWriters = Vec<Mutex<BufWriter<File>>>;

/// Create deterministic line-oriented shard scratch files under `dir`.
///
/// Callers supply the filename pattern so on-disk layouts remain owned by the
/// username/KV/parent-ID formats while file creation/backoff and mutex wrapping
/// stay in one auditable helper.
pub(crate) fn create_line_shard_writers(
    dir: &Path,
    count: usize,
    mut file_name: impl FnMut(usize) -> String,
    context_label: &str,
) -> Result<LineShardWriters> {
    let mut writers = Vec::with_capacity(count);
    for i in 0..count {
        let path: PathBuf = dir.join(file_name(i));
        let file = crate::util::create_with_default_backoff(&path)
            .with_context(|| format!("create {context_label} {}", path.display()))?;
        writers.push(Mutex::new(BufWriter::new(file)));
    }
    Ok(writers)
}

pub(crate) fn flush_line_shard_writers(writers: &LineShardWriters) -> Result<()> {
    for writer in writers {
        writer.lock().flush()?;
    }
    Ok(())
}

/// Read a line-oriented shard file, collapse duplicate lines into a hash set,
/// and write the unique lines back to `output`. Returns the unique line count.
///
/// `sort` controls the on-disk ordering of the output:
/// - `true` — sort the unique lines (`sort_unstable`) for a deterministic
///   layout. Used by [`crate::shard::ShardedWriter`].
/// - `false` — write them in unspecified `AHashSet` iteration order. Used by
///   the parent-id `dedupe` stage, which never needs a sorted shard.
///
/// `ShardedWriter` and the parent-id dedupe stage previously kept two
/// copy-paste-derived dedup loops that had silently drifted (one sorted, one
/// did not); both now route through this single helper so they cannot diverge
/// again.
pub(crate) fn dedup_line_shard(input: &Path, output: &Path, sort: bool) -> Result<usize> {
    let in_file = crate::util::open_with_default_backoff(input)
        .with_context(|| format!("open shard for dedup: {}", input.display()))?;
    let mut reader = BufReader::new(in_file);

    let mut seen: AHashSet<String> = AHashSet::with_capacity(64_000);
    let mut buf = String::with_capacity(16 * 1024);
    loop {
        let n = read_line_capped(&mut reader, &mut buf, DEFAULT_MAX_LINE_BYTES, input)
            .with_context(|| format!("read shard for dedup: {}", input.display()))?;
        if n == 0 {
            break;
        }
        if !buf.is_empty() {
            seen.insert(buf.clone());
        }
    }

    let unique_count = seen.len();
    let out_file = crate::util::create_with_default_backoff(output)
        .with_context(|| format!("create dedup output: {}", output.display()))?;
    let mut writer = BufWriter::new(out_file);
    if sort {
        let mut keys: Vec<String> = seen.into_iter().collect();
        keys.sort_unstable();
        for key in keys {
            writer.write_all(key.as_bytes())?;
            writer.write_all(b"\n")?;
        }
    } else {
        for key in seen {
            writer.write_all(key.as_bytes())?;
            writer.write_all(b"\n")?;
        }
    }
    writer.flush()?;
    Ok(unique_count)
}
