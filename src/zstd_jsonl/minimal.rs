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
use crate::ndjson::{read_line_capped, DEFAULT_MAX_LINE_BYTES};

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
    Ok(value.and_then(|v| v.as_i64()))
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
/// records.
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
