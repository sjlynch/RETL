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

use ahash::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};

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
