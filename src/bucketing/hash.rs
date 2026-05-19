use ahash::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};

/// Seeds for Stage 1's shard router. Distinct from [`STAGE2_SEEDS`] and
/// [`MICRO_SEEDS`] so a key landing in shard `i` here will redistribute
/// when re-bucketed in Stage 2 (and again in micro-bucketing). Do not unify.
const STAGE1_SEEDS: [u64; 4] = [
    0x1111_2222_3333_4444,
    0x5555_6666_7777_8888,
    0x9999_aaaa_bbbb_cccc,
    0xdddd_eeee_ffff_1234,
];

/// Seeds for Stage 2's re-bucketing. Intentionally differ from
/// [`STAGE1_SEEDS`] so re-bucketing actually redistributes keys across the
/// bucket files. Do not unify with the Stage 1 or micro-bucket seeds.
const STAGE2_SEEDS: [u64; 4] = [
    0xabcdef01_abcdef02,
    0xabcdef03_abcdef04,
    0xabcdef05_abcdef06,
    0xabcdef07_abcdef08,
];

/// Seeds for in-memory micro-bucket routing inside
/// [`super::process_bucket_streaming`]. Distinct from the Stage 1/2 seeds so
/// the final fan-out is independent of the on-disk shard/bucket assignment.
/// Do not unify with the Stage 1 or Stage 2 seeds.
const MICRO_SEEDS: [u64; 4] = [
    0x0a0b_0c0d_0e0f_a1a2,
    0xb1b2_b3b4_b5b6_c1c2,
    0xd1d2_d3d4_d5d6_e1e2,
    0xf1f2_f3f4_f5f6_0102,
];

#[inline]
pub(super) fn stable_index(state: &RandomState, key: &str, parts: usize) -> usize {
    let mut h = state.build_hasher();
    key.hash(&mut h);
    (h.finish() as usize) % parts.max(1)
}

#[inline]
fn state_from_seeds(seeds: [u64; 4]) -> RandomState {
    RandomState::with_seeds(seeds[0], seeds[1], seeds[2], seeds[3])
}

#[inline]
pub(super) fn stage1_state() -> RandomState {
    state_from_seeds(STAGE1_SEEDS)
}

#[inline]
pub(super) fn stage2_state() -> RandomState {
    state_from_seeds(STAGE2_SEEDS)
}

#[inline]
pub(super) fn micro_state() -> RandomState {
    state_from_seeds(MICRO_SEEDS)
}
