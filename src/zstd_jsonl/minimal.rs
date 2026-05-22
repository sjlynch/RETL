use anyhow::{anyhow, Result};
use serde::Deserialize;
use std::fs;
use std::io::{self, BufReader, Read};
use std::path::Path;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use zstd::stream::read::Decoder;

use crate::mem::maybe_throttle_low_memory;
use crate::ndjson::{read_line_capped, InvalidLineError, DEFAULT_MAX_LINE_BYTES};

/// Prevents "Frame requires too much memory" on large Reddit dumps.
///
/// Late-year Reddit dumps were written with the spec's max window (~2 GiB);
/// the zstd default rejects them with that error. Every decoder in this
/// module passes this through `decoder.window_log_max(...)`.
const ZSTD_WINDOW_LOG_MAX: u32 = 31;

/// Fire the cooperative-throttle callback every 65 536 lines.
///
/// Used as a bit-mask on a wrapping `u32` tick counter
/// (`tick & THROTTLE_SAMPLE_MASK == 0`). Bucketing/dedupe carry their own
/// bounded-channel backpressure, so this throttle is a coarse safety net —
/// sample less often to keep mutex contention out of the hot read loop.
const THROTTLE_SAMPLE_MASK: u32 = 0xFFFF;

/// Default `BufReader` capacity when callers do not specify one.
const DEFAULT_READ_BUF_BYTES: usize = 16 * 1024;

fn de_opt_string_lossy<'de, D>(deserializer: D) -> std::result::Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    Ok(value.and_then(|v| match v {
        serde_json::Value::String(s) => Some(s),
        _ => None,
    }))
}

fn de_opt_i64_lossy<'de, D>(deserializer: D) -> std::result::Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    Ok(value.and_then(|v| coerce_json_i64(&v)))
}

/// Coerce a JSON value to `i64` for the numeric fast-path fields
/// (`created_utc`, `score`).
///
/// serde_json's `Value::as_i64()` returns `None` for whole-number floats
/// (`100.0`) and for numeric strings (`"100"`). A bare `as_i64()` would
/// therefore make such records yield `score = None` / `created_utc = None`
/// and get silently dropped by `matches_minimal` / `within_bounds`, even
/// though the `--json` slow path keeps them (it coerces via `value_to_f64`).
/// Reddit dumps do occasionally store these fields as strings or floats, so
/// the fast path mirrors that coercion: integers, integral floats, and
/// integral numeric strings all resolve; fractional, non-numeric, or
/// out-of-`i64`-range values stay `None`.
fn coerce_json_i64(value: &serde_json::Value) -> Option<i64> {
    match value {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(i)
            } else if let Some(u) = n.as_u64() {
                i64::try_from(u).ok()
            } else {
                integral_f64_to_i64(n.as_f64()?)
            }
        }
        serde_json::Value::String(s) => {
            if let Ok(i) = s.parse::<i64>() {
                Some(i)
            } else if let Ok(u) = s.parse::<u64>() {
                i64::try_from(u).ok()
            } else {
                integral_f64_to_i64(s.parse::<f64>().ok()?)
            }
        }
        _ => None,
    }
}

/// Convert a finite, whole-number `f64` to `i64`, rejecting fractional or
/// out-of-`i64`-range values. The round-trip check (`i as f64 == n`) catches
/// magnitudes that the saturating `as` cast would otherwise clamp.
fn integral_f64_to_i64(n: f64) -> Option<i64> {
    if !n.is_finite() || n.fract() != 0.0 {
        return None;
    }
    let i = n as i64;
    (i as f64 == n).then_some(i)
}

fn de_opt_bool_lossy<'de, D>(deserializer: D) -> std::result::Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    Ok(value.and_then(|v| v.as_bool()))
}

/// Minimal line-level schema for fast filtering.
/// Extra fields are ignored by serde.
/// NOTE: includes `score` to enable fast numeric filters.
/// Includes `selftext`, `body`, `title`, `url`, `is_self`, and `parent_id` so
/// keyword/URL filtering works without a full parse. Includes `domain`
/// (submissions) and `id` (both kinds). Optional fields are lossy: unexpected
/// JSON types become `None` instead of making the entire hot-path parse fail,
/// so schema drift in unused fields does not silently drop otherwise valid
/// records. The numeric fields `created_utc` and `score` additionally coerce
/// string- and float-encoded numbers (`"100"`, `100.0`) to `i64` so the fast
/// path filters them the same way the `--json` slow path does.
#[derive(Debug, Deserialize)]
pub struct MinimalRecord {
    #[serde(default, deserialize_with = "de_opt_string_lossy")]
    pub subreddit: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string_lossy")]
    pub author: Option<String>,
    #[serde(default, deserialize_with = "de_opt_i64_lossy")]
    pub created_utc: Option<i64>,
    #[serde(default, deserialize_with = "de_opt_i64_lossy")]
    pub score: Option<i64>,

    // ID of the record (present on both RC and RS)
    #[serde(default, deserialize_with = "de_opt_string_lossy")]
    pub id: Option<String>,

    // Optional textual / metadata fields (only present when applicable):
    #[serde(default, deserialize_with = "de_opt_string_lossy")]
    pub selftext: Option<String>, // submissions
    #[serde(default, deserialize_with = "de_opt_string_lossy")]
    pub body: Option<String>, // comments
    #[serde(default, deserialize_with = "de_opt_string_lossy")]
    pub title: Option<String>, // submissions
    #[serde(default, deserialize_with = "de_opt_string_lossy")]
    pub url: Option<String>, // submissions (outbound URL on link posts; permalink on some self posts)
    #[serde(default, deserialize_with = "de_opt_bool_lossy")]
    pub is_self: Option<bool>, // submissions (true for self/text posts)

    #[allow(dead_code)]
    #[serde(default, deserialize_with = "de_opt_string_lossy")]
    pub parent_id: Option<String>, // comments

    #[serde(default, deserialize_with = "de_opt_string_lossy")]
    pub domain: Option<String>, // submissions (used by domains_in)
}

// ----------------------------- Helpers for full-error logging ------------------------------------
