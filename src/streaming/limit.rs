use crate::filters::{
    matches_full, matches_minimal, matches_subreddit_basic, within_bounds, DateBounds,
};
use crate::json_whitelist::WhitelistTokenizer;
use crate::paths::FileJob;
use crate::query::QuerySpec;
use crate::shard::ShardedWriter;
use crate::zstd_jsonl::{
    for_each_line_with_opts_status, malformed_json_error, parse_minimal, LineStreamOpts,
    PartialReadPolicy,
};
use anyhow::{anyhow, Result};
use indicatif::ProgressBar;
use serde_json::{Map, Value};
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

/// The three JSON keys the byte-level rewriter targets, with their `":` suffix
/// pre-baked so the search can flat-scan the line for an anchored byte sequence.
const TIMESTAMP_KEY_PATTERNS: &[&[u8]] =
    &[b"\"created_utc\":", b"\"retrieved_on\":", b"\"edited\":"];

/// Number of accepted records sampled before warning/erroring that a whitelist
/// did not match any top-level keys. Large enough to avoid overreacting to a
/// few schema variants, small enough to catch typos near the start of a run.
pub(crate) const WHITELIST_ZERO_MATCH_SAMPLE: u64 = 100;

const WHITELIST_ZERO_MATCH_HINT: &str = "check field names. Comments use `body`/`parent_id`/`link_id`; submissions use `title`/`selftext`/`domain`.";

#[derive(Debug)]
struct RecordLimitReached;

impl std::fmt::Display for RecordLimitReached {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("record limit reached")
    }
}

impl std::error::Error for RecordLimitReached {}

pub(crate) fn record_limit_reached_error() -> anyhow::Error {
    anyhow!(RecordLimitReached)
}

pub(crate) fn is_record_limit_reached(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|cause| cause.downcast_ref::<RecordLimitReached>().is_some())
}

#[derive(Debug)]
pub(crate) struct RecordLimit {
    max: u64,
    claimed: AtomicU64,
}

impl RecordLimit {
    pub(crate) fn new(max: u64) -> Self {
        Self::new_with_claimed(max, 0)
    }

    pub(crate) fn new_with_claimed(max: u64, claimed: u64) -> Self {
        Self {
            max,
            claimed: AtomicU64::new(claimed.min(max)),
        }
    }

    pub(crate) fn is_zero(&self) -> bool {
        self.max == 0
    }

    pub(crate) fn is_exhausted(&self) -> bool {
        self.claimed.load(Ordering::Relaxed) >= self.max
    }

    pub(crate) fn try_claim(&self) -> bool {
        let mut cur = self.claimed.load(Ordering::Relaxed);
        loop {
            if cur >= self.max {
                return false;
            }
            match self.claimed.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }
}

#[inline]
pub(crate) fn claim_record_or_stop(limit: Option<&RecordLimit>) -> Result<()> {
    if let Some(limit) = limit {
        if !limit.try_claim() {
            return Err(record_limit_reached_error());
        }
    }
    Ok(())
}

/// One projection emission reported to the [`WhitelistMatchTracker`]. The
/// tracker keeps the fast and slow paths' field-presence counters separate so
/// the verdict reflects production semantics (the fast `WhitelistTokenizer`
/// path) rather than being skewed by tokenizer-fallback lines.
#[derive(Debug, Clone, Copy)]
pub(crate) struct WhitelistEmission<'a> {
    /// Indices of requested whitelist fields that were present in this record.
    pub matched_fields: &'a [usize],
    /// True when the slow `serde_json::Value` path produced this emission
    /// (the `WhitelistTokenizer` rejected the line structurally).
    pub used_slow_path: bool,
}
