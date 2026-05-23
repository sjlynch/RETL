//! Generalized GROUP BY + agg-expression DSL.
//!
//! [`ExprAggregator`] is an [`Aggregator`] configured by a list of
//! [`GroupKey`]s and a list of [`AggOp`]s. It supersedes the hardcoded
//! per-author/per-month rollups in `bin_helpers/aggregate/group.rs`: any
//! combination of group keys (`author`, `subreddit`, `year`, `month`,
//! `day`/`hour`, or arbitrary JSON pointers) can be paired with any combination
//! of `count(*)`, `distinct(<field>)`, `sum/min/max/first/last(<field>)`
//! aggregate ops, and the result is emitted as one JSON row per group.
//!
//! # Distinct counting
//!
//! `CountDistinct` starts each per-group estimator as an exact [`HashSet`] of
//! 64-bit value hashes. Once a group's cardinality exceeds
//! [`DISTINCT_HLL_THRESHOLD`] (default `4096`) the estimator is promoted to a
//! HyperLogLog sketch with `p = 12` (`m = 4096`) — fixed-size 4 KiB per group,
//! relative-error bound `1.04 / sqrt(m) ≈ 1.6 %`. Promotions are logged at
//! `INFO` so a caller can correlate an output value with the chosen estimator
//! (and never get a silent precision change).
//!
//! Merging tolerates mixed Exact/Approx peer states: exact-on-exact unions
//! the sets (and promotes to HLL if the union crosses the threshold), and
//! mixed pairs promote the exact set into the HLL register-by-register. The
//! resulting fold is associative, so the parallel shard-merge in
//! [`crate::RedditETL::aggregate_jsonls_parallel`] is safe.
//!
//! # Field-reference syntax
//!
//! Aggregate-op arguments accept either a bare field name (`score`, `id`,
//! `body`) — which is read via a direct top-level lookup, matching the
//! [`crate::MinimalRecord`] fast-path field set — or a JSON pointer beginning
//! with `/` (`/data/score`), which falls back to
//! [`serde_json::Value::pointer`]. The same convention applies to
//! `--group-by json:/pointer` keys.

use crate::aggregate::Aggregator;
use ahash::AHasher;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
use std::collections::{BTreeMap, HashSet};
use std::hash::Hasher;
use time::OffsetDateTime;

/// Per-group cardinality at which `CountDistinct` upgrades from an exact
/// [`HashSet`] to a [`HllState`] HyperLogLog sketch. Picked so small
/// fixtures stay exact and large rollups stay bounded at 4 KiB per group.
pub const DISTINCT_HLL_THRESHOLD: usize = 4096;

/// `log2(m)` for the HyperLogLog sketch — 12 ⇒ m = 4096 registers,
/// 1.04/sqrt(m) ≈ 1.6 % relative error.
const HLL_PRECISION: u32 = 12;
const HLL_REGISTERS: usize = 1 << HLL_PRECISION;

// -----------------------------------------------------------------------------
// Group key
// -----------------------------------------------------------------------------

/// Granularity for the `Date` group key. `Day` produces `YYYY-MM-DD`;
/// `Hour` produces `YYYY-MM-DDTHH`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DateGranularity {
    Day,
    Hour,
}

/// A single group-by column.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupKey {
    Subreddit,
    Author,
    Year,
    Month,
    Date(DateGranularity),
    /// JSON pointer (`""` or `"/..."`). Read via [`Value::pointer`].
    JsonPointer(String),
}

impl GroupKey {
    /// Parse one CLI-supplied token (`subreddit`, `author`, `year`, `month`,
    /// `day`, `date`, `hour`, or `json:/<pointer>`).
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "subreddit" => Ok(Self::Subreddit),
            "author" => Ok(Self::Author),
            "year" => Ok(Self::Year),
            "month" => Ok(Self::Month),
            "day" | "date" => Ok(Self::Date(DateGranularity::Day)),
            "hour" => Ok(Self::Date(DateGranularity::Hour)),
            s if s.starts_with("json:") => {
                let pointer = &s["json:".len()..];
                validate_json_pointer(pointer)?;
                Ok(Self::JsonPointer(pointer.to_string()))
            }
            _ => Err(format!(
                "unsupported --group-by key {raw:?}; expected subreddit, author, year, month, day, hour, or json:/pointer"
            )),
        }
    }

    /// Column header used in JSONL output rows.
    pub fn label(&self) -> String {
        match self {
            Self::Subreddit => "subreddit".to_string(),
            Self::Author => "author".to_string(),
            Self::Year => "year".to_string(),
            Self::Month => "month".to_string(),
            Self::Date(DateGranularity::Day) => "date".to_string(),
            Self::Date(DateGranularity::Hour) => "hour".to_string(),
            Self::JsonPointer(p) => sanitize_field_label(p),
        }
    }

    fn key_for(&self, record: &Value) -> Option<String> {
        match self {
            Self::Subreddit => record.get("subreddit").and_then(value_to_key),
            Self::Author => record.get("author").and_then(value_to_key),
            Self::Year | Self::Month | Self::Date(_) => {
                let secs = record.get("created_utc").and_then(value_to_seconds)?;
                let dt = OffsetDateTime::from_unix_timestamp(secs).ok()?;
                Some(match self {
                    Self::Year => format!("{:04}", dt.year()),
                    Self::Month => format!("{:04}-{:02}", dt.year(), u8::from(dt.month())),
                    Self::Date(DateGranularity::Day) => {
                        format!(
                            "{:04}-{:02}-{:02}",
                            dt.year(),
                            u8::from(dt.month()),
                            dt.day()
                        )
                    }
                    Self::Date(DateGranularity::Hour) => format!(
                        "{:04}-{:02}-{:02}T{:02}",
                        dt.year(),
                        u8::from(dt.month()),
                        dt.day(),
                        dt.hour()
                    ),
                    _ => unreachable!(),
                })
            }
            Self::JsonPointer(p) => record.pointer(p).and_then(value_to_key),
        }
    }
}

/// Parse a comma-separated `--group-by` list.
pub fn parse_group_keys(raw: &str) -> Result<Vec<GroupKey>, String> {
    let parts: Vec<&str> = raw
        .split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .collect();
    if parts.is_empty() {
        return Err("--group-by requires at least one key".to_string());
    }
    parts.into_iter().map(GroupKey::parse).collect()
}

// -----------------------------------------------------------------------------
// Agg op
// -----------------------------------------------------------------------------

/// A single aggregate expression evaluated per group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggOp {
    /// `count(*)` — number of records in the group.
    Count,
    /// `distinct(<field>)` — number of distinct non-null values of `field`.
    /// Exact below [`DISTINCT_HLL_THRESHOLD`], HyperLogLog above.
    CountDistinct(String),
    /// `sum(<field>)` — numeric sum (integer-preserving until first float).
    Sum(String),
    /// `min(<field>)` — numeric minimum across non-null records.
    Min(String),
    /// `max(<field>)` — numeric maximum across non-null records.
    Max(String),
    /// `first(<field>)` — value of `field` at the row with the minimum
    /// `created_utc`. Tie-broken by lexicographic JSON serialization.
    First(String),
    /// `last(<field>)` — value of `field` at the row with the maximum
    /// `created_utc`. Tie-broken by lexicographic JSON serialization.
    Last(String),
}

impl AggOp {
    /// Column header used in JSONL output rows. `count(*)` → `count`,
    /// `distinct(id)` → `distinct_id`, `max(score)` → `max_score`, etc.
    pub fn label(&self) -> String {
        match self {
            Self::Count => "count".to_string(),
            Self::CountDistinct(f) => format!("distinct_{}", sanitize_field_label(f)),
            Self::Sum(f) => format!("sum_{}", sanitize_field_label(f)),
            Self::Min(f) => format!("min_{}", sanitize_field_label(f)),
            Self::Max(f) => format!("max_{}", sanitize_field_label(f)),
            Self::First(f) => format!("first_{}", sanitize_field_label(f)),
            Self::Last(f) => format!("last_{}", sanitize_field_label(f)),
        }
    }
}

/// Parse a comma-separated `--agg` list (`count(*),distinct(id),max(score)`).
pub fn parse_agg_ops(raw: &str) -> Result<Vec<AggOp>, String> {
    let mut ops = Vec::new();
    for piece in raw.split(',') {
        let piece = piece.trim();
        if piece.is_empty() {
            continue;
        }
        ops.push(parse_one_agg_op(piece)?);
    }
    if ops.is_empty() {
        return Err("--agg requires at least one op".to_string());
    }
    Ok(ops)
}

fn parse_one_agg_op(raw: &str) -> Result<AggOp, String> {
    let open = raw.find('(').ok_or_else(|| {
        format!("expected `op(arg)`, got {raw:?}; e.g. `count(*)`, `distinct(id)`, `max(score)`")
    })?;
    if !raw.ends_with(')') {
        return Err(format!("missing closing `)` in {raw:?}"));
    }
    let name = raw[..open].trim();
    let arg = raw[open + 1..raw.len() - 1].trim();
    match name {
        "count" => {
            if arg != "*" {
                return Err(format!(
                    "count() expects `*`; use distinct({arg}) for unique counts",
                ));
            }
            Ok(AggOp::Count)
        }
        "distinct" | "count_distinct" => {
            require_field(arg, name)?;
            validate_field_or_pointer(arg)?;
            Ok(AggOp::CountDistinct(arg.to_string()))
        }
        "sum" => {
            require_field(arg, name)?;
            validate_field_or_pointer(arg)?;
            Ok(AggOp::Sum(arg.to_string()))
        }
        "min" => {
            require_field(arg, name)?;
            validate_field_or_pointer(arg)?;
            Ok(AggOp::Min(arg.to_string()))
        }
        "max" => {
            require_field(arg, name)?;
            validate_field_or_pointer(arg)?;
            Ok(AggOp::Max(arg.to_string()))
        }
        "first" => {
            require_field(arg, name)?;
            validate_field_or_pointer(arg)?;
            Ok(AggOp::First(arg.to_string()))
        }
        "last" => {
            require_field(arg, name)?;
            validate_field_or_pointer(arg)?;
            Ok(AggOp::Last(arg.to_string()))
        }
        other => Err(format!(
            "unknown aggregate op {other:?}; expected count, distinct, sum, min, max, first, or last"
        )),
    }
}

fn require_field(arg: &str, name: &str) -> Result<(), String> {
    if arg.is_empty() {
        Err(format!("{name}() requires a field name or /pointer argument"))
    } else if arg == "*" {
        Err(format!(
            "{name}() does not accept `*`; pass a field name like `{name}(score)` or `/pointer`"
        ))
    } else {
        Ok(())
    }
}

fn validate_field_or_pointer(arg: &str) -> Result<(), String> {
    if arg.starts_with('/') {
        validate_json_pointer(arg)
    } else {
        Ok(())
    }
}

fn validate_json_pointer(pointer: &str) -> Result<(), String> {
    if pointer.is_empty() || pointer.starts_with('/') {
        Ok(())
    } else {
        Err(format!(
            "JSON pointers must be empty or start with '/': {pointer:?}"
        ))
    }
}

fn sanitize_field_label(field: &str) -> String {
    let trimmed = field.trim_start_matches('/');
    if trimmed.is_empty() {
        // Top-level pointer (`""`): give it *some* label.
        return "root".to_string();
    }
    trimmed.replace('/', "_")
}

// -----------------------------------------------------------------------------
// Numeric helpers
// -----------------------------------------------------------------------------

/// Integer-preserving numeric value. Switches to `Float` on overflow or when
/// it first sees a non-integer JSON number.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
enum NumVal {
    Int(i128),
    Float(f64),
}

impl NumVal {
    fn from_value(v: &Value) -> Option<Self> {
        match v {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Some(Self::Int(i as i128))
                } else if let Some(u) = n.as_u64() {
                    Some(Self::Int(u as i128))
                } else if let Some(f) = n.as_f64() {
                    Some(Self::Float(f))
                } else {
                    None
                }
            }
            Value::String(s) => {
                if let Ok(i) = s.parse::<i128>() {
                    Some(Self::Int(i))
                } else {
                    s.parse::<f64>().ok().map(Self::Float)
                }
            }
            _ => None,
        }
    }

    fn cmp(self, other: Self) -> std::cmp::Ordering {
        match (self, other) {
            (Self::Int(a), Self::Int(b)) => a.cmp(&b),
            (Self::Float(a), Self::Float(b)) => a.total_cmp(&b),
            (Self::Int(a), Self::Float(b)) => (a as f64).total_cmp(&b),
            (Self::Float(a), Self::Int(b)) => a.total_cmp(&(b as f64)),
        }
    }

    fn add(self, other: Self) -> Self {
        match (self, other) {
            (Self::Int(a), Self::Int(b)) => match a.checked_add(b) {
                Some(s) => Self::Int(s),
                None => Self::Float(a as f64 + b as f64),
            },
            (Self::Int(a), Self::Float(b)) => Self::Float(a as f64 + b),
            (Self::Float(a), Self::Int(b)) => Self::Float(a + b as f64),
            (Self::Float(a), Self::Float(b)) => Self::Float(a + b),
        }
    }

    fn into_json(self) -> Value {
        match self {
            Self::Int(i) => {
                if let Ok(i64v) = i64::try_from(i) {
                    Value::Number(Number::from(i64v))
                } else {
                    // i128 outside i64 — fall back to float (still bounded
                    // and JSON-representable). Surfacing as a string would
                    // confuse downstream readers more than the precision
                    // loss does.
                    Number::from_f64(i as f64).map_or(Value::Null, Value::Number)
                }
            }
            Self::Float(f) => Number::from_f64(f).map_or(Value::Null, Value::Number),
        }
    }
}

// -----------------------------------------------------------------------------
// HyperLogLog
// -----------------------------------------------------------------------------

/// Compact HyperLogLog sketch with `m = 2^HLL_PRECISION` 1-byte registers
/// (4 KiB at `p = 12`). Estimation uses the standard HyperLogLog formula
/// with small-range linear-counting correction.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HllState {
    registers: Vec<u8>,
}

impl HllState {
    fn new() -> Self {
        Self {
            registers: vec![0u8; HLL_REGISTERS],
        }
    }

    fn add_hash(&mut self, h: u64) {
        let idx = (h as usize) & (HLL_REGISTERS - 1);
        // Right-shift the bucket-index bits out so the LZ count is over the
        // remaining (64 - p)-bit substream. Because the top `p` bits of the
        // shifted value are zero by construction, the raw `leading_zeros`
        // overcounts by exactly `p` — subtract it back. When `w == 0` (a
        // legitimate outcome at p = 12: probability 2^-52) `leading_zeros`
        // is 64, giving the maximum M value of `64 - p + 1`.
        let w = h >> HLL_PRECISION;
        let lz_in_substream = w.leading_zeros().saturating_sub(HLL_PRECISION);
        let m_val = (lz_in_substream + 1) as u8;
        if m_val > self.registers[idx] {
            self.registers[idx] = m_val;
        }
    }

    fn merge(&mut self, other: &Self) {
        for (a, b) in self.registers.iter_mut().zip(other.registers.iter()) {
            if *b > *a {
                *a = *b;
            }
        }
    }

    /// HyperLogLog cardinality estimate with linear-counting correction
    /// for small ranges. Returned as `u64`; relative error ≈ 1.6 % at the
    /// configured precision.
    pub fn estimate(&self) -> u64 {
        let m = HLL_REGISTERS as f64;
        let alpha = 0.7213_f64 / (1.0 + 1.079 / m);
        let mut sum = 0.0_f64;
        let mut zeros = 0usize;
        for &r in &self.registers {
            sum += 2f64.powi(-(r as i32));
            if r == 0 {
                zeros += 1;
            }
        }
        let raw = alpha * m * m / sum;
        if raw <= 2.5 * m && zeros > 0 {
            // Linear counting is more accurate for small cardinalities.
            (m * (m / zeros as f64).ln()).round() as u64
        } else {
            raw.round() as u64
        }
    }
}

// -----------------------------------------------------------------------------
// Distinct state — exact below threshold, HyperLogLog above.
// -----------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
enum DistinctState {
    Exact(HashSet<u64>),
    Approx(HllState),
}

impl DistinctState {
    fn new() -> Self {
        Self::Exact(HashSet::new())
    }

    fn add(&mut self, hash: u64, op_label: &str) {
        match self {
            Self::Exact(set) => {
                set.insert(hash);
                if set.len() > DISTINCT_HLL_THRESHOLD {
                    let mut hll = HllState::new();
                    for h in set.iter() {
                        hll.add_hash(*h);
                    }
                    tracing::info!(
                        op = %op_label,
                        threshold = DISTINCT_HLL_THRESHOLD,
                        hll_precision = HLL_PRECISION,
                        relative_error = 1.04 / (HLL_REGISTERS as f64).sqrt(),
                        "{op_label}: exact distinct set exceeded {DISTINCT_HLL_THRESHOLD}; switching to HyperLogLog (p={HLL_PRECISION}, m={HLL_REGISTERS})",
                    );
                    *self = Self::Approx(hll);
                }
            }
            Self::Approx(hll) => hll.add_hash(hash),
        }
    }

    fn count(&self) -> u64 {
        match self {
            Self::Exact(set) => set.len() as u64,
            Self::Approx(hll) => hll.estimate(),
        }
    }

    fn merge(&mut self, other: Self, op_label: &str) {
        let left = std::mem::replace(self, Self::Exact(HashSet::new()));
        *self = match (left, other) {
            (Self::Exact(mut a), Self::Exact(b)) => {
                a.extend(b);
                if a.len() > DISTINCT_HLL_THRESHOLD {
                    let mut hll = HllState::new();
                    for h in a.iter() {
                        hll.add_hash(*h);
                    }
                    tracing::info!(
                        op = %op_label,
                        threshold = DISTINCT_HLL_THRESHOLD,
                        "{op_label}: exact-exact merge crossed distinct threshold; promoting to HyperLogLog",
                    );
                    Self::Approx(hll)
                } else {
                    Self::Exact(a)
                }
            }
            (Self::Approx(mut a), Self::Approx(b)) => {
                a.merge(&b);
                Self::Approx(a)
            }
            (Self::Exact(set), Self::Approx(mut hll))
            | (Self::Approx(mut hll), Self::Exact(set)) => {
                for h in set {
                    hll.add_hash(h);
                }
                Self::Approx(hll)
            }
        };
    }
}

// -----------------------------------------------------------------------------
// Per-op accumulator
// -----------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
enum OpState {
    Count(u64),
    Distinct(DistinctState),
    Sum { acc: NumVal, seen: bool },
    Extreme(Option<NumVal>), // for both Min and Max
    /// Value-at-extreme-timestamp, for both `first` and `last`. Tie-broken
    /// lexicographically on the JSON serialization of the value so the merge
    /// stays associative.
    Extremum {
        ts: Option<i64>,
        value: Option<Value>,
    },
}

impl OpState {
    fn new(op: &AggOp) -> Self {
        match op {
            AggOp::Count => Self::Count(0),
            AggOp::CountDistinct(_) => Self::Distinct(DistinctState::new()),
            AggOp::Sum(_) => Self::Sum {
                acc: NumVal::Int(0),
                seen: false,
            },
            AggOp::Min(_) | AggOp::Max(_) => Self::Extreme(None),
            AggOp::First(_) | AggOp::Last(_) => Self::Extremum {
                ts: None,
                value: None,
            },
        }
    }

    fn into_json(self) -> Value {
        match self {
            Self::Count(n) => Value::Number(Number::from(n)),
            Self::Distinct(d) => Value::Number(Number::from(d.count())),
            Self::Sum { acc, seen } => {
                if seen {
                    acc.into_json()
                } else {
                    Value::Null
                }
            }
            Self::Extreme(opt) => opt.map_or(Value::Null, NumVal::into_json),
            Self::Extremum { value, .. } => value.unwrap_or(Value::Null),
        }
    }
}

// -----------------------------------------------------------------------------
// ExprAggregator
// -----------------------------------------------------------------------------

/// Generalized GROUP BY / agg-op aggregator. Implement `Aggregator`, so it
/// plugs into [`crate::RedditETL::aggregate_jsonls_parallel_collect_with`].
///
/// Construct with [`ExprAggregator::new`] (per-shard factory passed to
/// `aggregate_jsonls_parallel_collect_with`). [`Default`] produces the
/// merge identity, which adopts the configured schema from its peer on
/// first merge — required by the trait's `Default + Send` bound.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExprAggregator {
    group_keys: Vec<GroupKey>,
    ops: Vec<AggOp>,
    /// Groups keyed by the concatenated `Option<String>` group-key values
    /// (one slot per `group_keys` entry; `None` is a missing/null value).
    /// `BTreeMap` for deterministic output ordering. Serialized as a
    /// `Vec<(key, value)>` because JSON object keys must be strings and the
    /// per-group keys here are arbitrary `Vec<Option<String>>` tuples.
    #[serde(
        serialize_with = "serialize_groups",
        deserialize_with = "deserialize_groups"
    )]
    groups: BTreeMap<Vec<Option<String>>, Vec<OpState>>,
    records_ingested: u64,
    records_skipped_no_group_key: u64,
}

fn serialize_groups<S>(
    groups: &BTreeMap<Vec<Option<String>>, Vec<OpState>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeSeq;
    let mut seq = serializer.serialize_seq(Some(groups.len()))?;
    for entry in groups.iter() {
        seq.serialize_element(&entry)?;
    }
    seq.end()
}

fn deserialize_groups<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<Vec<Option<String>>, Vec<OpState>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let entries: Vec<(Vec<Option<String>>, Vec<OpState>)> =
        Deserialize::deserialize(deserializer)?;
    Ok(entries.into_iter().collect())
}

impl ExprAggregator {
    /// Configure a new aggregator with the supplied group keys and ops.
    /// Either list may be empty: empty `group_keys` produces a single global
    /// row; empty `ops` is treated as `count(*)` so the rollup is not
    /// silently degenerate.
    pub fn new(group_keys: Vec<GroupKey>, ops: Vec<AggOp>) -> Self {
        let ops = if ops.is_empty() {
            vec![AggOp::Count]
        } else {
            ops
        };
        Self {
            group_keys,
            ops,
            groups: BTreeMap::new(),
            records_ingested: 0,
            records_skipped_no_group_key: 0,
        }
    }

    pub fn group_keys(&self) -> &[GroupKey] {
        &self.group_keys
    }

    pub fn ops(&self) -> &[AggOp] {
        &self.ops
    }

    pub fn records_ingested(&self) -> u64 {
        self.records_ingested
    }

    pub fn records_skipped_no_group_key(&self) -> u64 {
        self.records_skipped_no_group_key
    }

    /// Returns the number of output rows (one per group).
    pub fn row_count(&self) -> usize {
        self.groups.len()
    }

    /// Consume the aggregator and emit one JSON object per group, ordered by
    /// the group-key tuple (`BTreeMap` iteration order).
    ///
    /// Column order: each [`GroupKey::label`] in input order, then each
    /// [`AggOp::label`] in input order. Missing group-key components are
    /// emitted as JSON `null`.
    pub fn into_rows(self) -> Vec<Map<String, Value>> {
        let group_keys = self.group_keys;
        let ops = self.ops;
        let mut rows = Vec::with_capacity(self.groups.len());
        for (key, states) in self.groups {
            let mut row = Map::with_capacity(group_keys.len() + ops.len());
            for (label, value) in group_keys.iter().map(GroupKey::label).zip(key.into_iter()) {
                row.insert(
                    label,
                    value.map_or(Value::Null, Value::String),
                );
            }
            for (op, state) in ops.iter().zip(states.into_iter()) {
                row.insert(op.label(), state.into_json());
            }
            rows.push(row);
        }
        rows
    }

    fn adopt_schema_from(&mut self, other: &Self) {
        self.group_keys = other.group_keys.clone();
        self.ops = other.ops.clone();
    }
}

fn record_value_for_field(record: &Value, field: &str) -> Option<Value> {
    if field.starts_with('/') {
        record.pointer(field).cloned()
    } else {
        record.get(field).cloned()
    }
}

fn record_value_for_field_ref<'a>(record: &'a Value, field: &str) -> Option<&'a Value> {
    if field.starts_with('/') {
        record.pointer(field)
    } else {
        record.get(field)
    }
}

fn hash_value(v: &Value) -> u64 {
    // Stable hash of the JSON value's serialized form. Distinct keys must
    // round-trip across shard merges, so we hash the canonical JSON string
    // rather than the Rust enum discriminant (which would differ between an
    // `i64` and a `String` of the same digits).
    let mut hasher = AHasher::default();
    match v {
        Value::String(s) => {
            // Cheap fast path: hash the string bytes directly.
            hasher.write_u8(b's');
            hasher.write(s.as_bytes());
        }
        _ => {
            hasher.write_u8(b'j');
            let s = serde_json::to_string(v).unwrap_or_default();
            hasher.write(s.as_bytes());
        }
    }
    hasher.finish()
}

impl Aggregator for ExprAggregator {
    fn ingest(&mut self, record: &Value) {
        // Default-constructed (merge-identity) instance: pretend the record
        // didn't arrive — the configured peer will absorb it on merge.
        // This matches `GroupMetricAgg`'s identity-shard convention.
        if self.group_keys.is_empty() && self.ops.is_empty() {
            return;
        }
        self.records_ingested += 1;

        let mut key = Vec::with_capacity(self.group_keys.len());
        for gk in &self.group_keys {
            let value = gk.key_for(record);
            if value.is_none() {
                // Match `GroupMetricAgg`: a missing group-key component is
                // recorded as a skip so a mistyped --group-by token doesn't
                // silently produce an all-`null` group.
                self.records_skipped_no_group_key += 1;
                return;
            }
            key.push(value);
        }

        let ops = &self.ops;
        let states = self
            .groups
            .entry(key)
            .or_insert_with(|| ops.iter().map(OpState::new).collect());
        let created_utc = record.get("created_utc").and_then(value_to_seconds);

        for (op, state) in ops.iter().zip(states.iter_mut()) {
            match (op, state) {
                (AggOp::Count, OpState::Count(c)) => *c += 1,
                (AggOp::CountDistinct(field), OpState::Distinct(d)) => {
                    if let Some(v) = record_value_for_field_ref(record, field) {
                        if !v.is_null() {
                            d.add(hash_value(v), &op.label());
                        }
                    }
                }
                (AggOp::Sum(field), OpState::Sum { acc, seen }) => {
                    if let Some(v) = record_value_for_field_ref(record, field) {
                        if let Some(n) = NumVal::from_value(v) {
                            *acc = acc.add(n);
                            *seen = true;
                        }
                    }
                }
                (AggOp::Min(field), OpState::Extreme(slot)) => {
                    if let Some(v) = record_value_for_field_ref(record, field) {
                        if let Some(n) = NumVal::from_value(v) {
                            *slot = Some(match slot.take() {
                                None => n,
                                Some(old) => {
                                    if n.cmp(old).is_lt() {
                                        n
                                    } else {
                                        old
                                    }
                                }
                            });
                        }
                    }
                }
                (AggOp::Max(field), OpState::Extreme(slot)) => {
                    if let Some(v) = record_value_for_field_ref(record, field) {
                        if let Some(n) = NumVal::from_value(v) {
                            *slot = Some(match slot.take() {
                                None => n,
                                Some(old) => {
                                    if n.cmp(old).is_gt() {
                                        n
                                    } else {
                                        old
                                    }
                                }
                            });
                        }
                    }
                }
                (AggOp::First(field), OpState::Extremum { ts, value }) => {
                    if let (Some(this_ts), Some(this_val)) =
                        (created_utc, record_value_for_field(record, field))
                    {
                        update_extremum(ts, value, this_ts, this_val, /*pick_smaller_ts*/ true);
                    }
                }
                (AggOp::Last(field), OpState::Extremum { ts, value }) => {
                    if let (Some(this_ts), Some(this_val)) =
                        (created_utc, record_value_for_field(record, field))
                    {
                        update_extremum(ts, value, this_ts, this_val, /*pick_smaller_ts*/ false);
                    }
                }
                _ => {
                    // OpState was built from `ops`, so this is unreachable by
                    // construction. Treat it as a bug.
                    debug_assert!(false, "op/state mismatch in ExprAggregator");
                }
            }
        }
    }

    fn merge(&mut self, other: Self) {
        // Default identity-shard handling: adopt the configured peer's schema.
        if self.ops.is_empty() && self.group_keys.is_empty() {
            self.adopt_schema_from(&other);
        } else if other.ops.is_empty() && other.group_keys.is_empty() {
            // Peer is the identity: nothing to merge in.
            self.records_ingested += other.records_ingested;
            self.records_skipped_no_group_key += other.records_skipped_no_group_key;
            return;
        } else if self.group_keys != other.group_keys || self.ops != other.ops {
            panic!(
                "refusing to merge incompatible ExprAggregator shards: left group_keys={:?} ops={:?}, right group_keys={:?} ops={:?}",
                self.group_keys, self.ops, other.group_keys, other.ops
            );
        }

        self.records_ingested += other.records_ingested;
        self.records_skipped_no_group_key += other.records_skipped_no_group_key;

        for (key, other_states) in other.groups {
            let ops = &self.ops;
            let states = self
                .groups
                .entry(key)
                .or_insert_with(|| ops.iter().map(OpState::new).collect());
            for ((op, dst), src) in ops.iter().zip(states.iter_mut()).zip(other_states.into_iter())
            {
                merge_op_state(op, dst, src);
            }
        }
    }
}

fn merge_op_state(op: &AggOp, dst: &mut OpState, src: OpState) {
    match (op, dst, src) {
        (AggOp::Count, OpState::Count(a), OpState::Count(b)) => *a += b,
        (AggOp::CountDistinct(_), OpState::Distinct(a), OpState::Distinct(b)) => {
            a.merge(b, &op.label())
        }
        (AggOp::Sum(_), OpState::Sum { acc, seen }, OpState::Sum { acc: b_acc, seen: b_seen }) => {
            if b_seen {
                *acc = acc.add(b_acc);
                *seen = true;
            }
        }
        (AggOp::Min(_), OpState::Extreme(slot), OpState::Extreme(other)) => {
            if let Some(n) = other {
                *slot = Some(match slot.take() {
                    None => n,
                    Some(old) => {
                        if n.cmp(old).is_lt() {
                            n
                        } else {
                            old
                        }
                    }
                });
            }
        }
        (AggOp::Max(_), OpState::Extreme(slot), OpState::Extreme(other)) => {
            if let Some(n) = other {
                *slot = Some(match slot.take() {
                    None => n,
                    Some(old) => {
                        if n.cmp(old).is_gt() {
                            n
                        } else {
                            old
                        }
                    }
                });
            }
        }
        (
            AggOp::First(_),
            OpState::Extremum { ts, value },
            OpState::Extremum { ts: b_ts, value: b_val },
        ) => {
            if let (Some(this_ts), Some(this_val)) = (b_ts, b_val) {
                update_extremum(ts, value, this_ts, this_val, /*pick_smaller_ts*/ true);
            }
        }
        (
            AggOp::Last(_),
            OpState::Extremum { ts, value },
            OpState::Extremum { ts: b_ts, value: b_val },
        ) => {
            if let (Some(this_ts), Some(this_val)) = (b_ts, b_val) {
                update_extremum(ts, value, this_ts, this_val, /*pick_smaller_ts*/ false);
            }
        }
        _ => debug_assert!(false, "op/state mismatch in ExprAggregator merge"),
    }
}

/// Update a `first`/`last` slot with a candidate `(ts, value)` pair.
/// `pick_smaller_ts == true` ⇒ `first`; `false` ⇒ `last`. Ties are
/// broken by lexicographic JSON serialization of `value` (smaller wins
/// in both cases) so the fold is associative.
fn update_extremum(
    slot_ts: &mut Option<i64>,
    slot_value: &mut Option<Value>,
    candidate_ts: i64,
    candidate_value: Value,
    pick_smaller_ts: bool,
) {
    let replace = match (*slot_ts, slot_value.as_ref()) {
        (None, _) | (_, None) => true,
        (Some(cur_ts), Some(cur_val)) => {
            if pick_smaller_ts {
                if candidate_ts < cur_ts {
                    true
                } else if candidate_ts == cur_ts {
                    // Lex tie-break for associativity.
                    let cur = serde_json::to_string(cur_val).unwrap_or_default();
                    let cand = serde_json::to_string(&candidate_value).unwrap_or_default();
                    cand < cur
                } else {
                    false
                }
            } else if candidate_ts > cur_ts {
                true
            } else if candidate_ts == cur_ts {
                let cur = serde_json::to_string(cur_val).unwrap_or_default();
                let cand = serde_json::to_string(&candidate_value).unwrap_or_default();
                cand < cur
            } else {
                false
            }
        }
    };
    if replace {
        *slot_ts = Some(candidate_ts);
        *slot_value = Some(candidate_value);
    }
}

// -----------------------------------------------------------------------------
// Value → key helpers
// -----------------------------------------------------------------------------

fn value_to_key(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(v).ok(),
    }
}

fn value_to_seconds(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        Value::String(s) => s.parse::<i64>().ok().or_else(|| {
            // Tolerate fractional unix seconds, but reject ISO timestamps —
            // the GroupKey::Month parse path already covered ISO via the
            // GroupMetricAgg helpers; here we treat string `created_utc`
            // as an epoch integer like the corpus does.
            s.parse::<f64>().ok().map(|f| f as i64)
        }),
        _ => None,
    }
}

// -----------------------------------------------------------------------------
// Output rendering
// -----------------------------------------------------------------------------

/// Write the aggregated rows as JSONL (one JSON object per line) to `w`.
/// Column order is fixed by the aggregator's schema; see
/// [`ExprAggregator::into_rows`].
pub fn write_rows_jsonl<W: std::io::Write>(rows: &[Map<String, Value>], w: &mut W) -> std::io::Result<()> {
    for row in rows {
        serde_json::to_writer(&mut *w, row)?;
        w.write_all(b"\n")?;
    }
    Ok(())
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn run<A: Aggregator>(records: Vec<Value>, mut agg: A) -> A {
        for r in records {
            agg.ingest(&r);
        }
        agg
    }

    #[test]
    fn parse_group_keys_accepts_known_keys() {
        let v = parse_group_keys("subreddit,month,author").unwrap();
        assert_eq!(
            v,
            vec![GroupKey::Subreddit, GroupKey::Month, GroupKey::Author]
        );
    }

    #[test]
    fn parse_group_keys_accepts_json_pointer() {
        let v = parse_group_keys("json:/foo/bar").unwrap();
        assert_eq!(v, vec![GroupKey::JsonPointer("/foo/bar".to_string())]);
    }

    #[test]
    fn parse_group_keys_rejects_unknown() {
        let err = parse_group_keys("nope").unwrap_err();
        assert!(err.contains("unsupported"), "{err}");
    }

    #[test]
    fn parse_agg_ops_parses_mixed_list() {
        let v = parse_agg_ops("count(*), distinct(id), max(score)").unwrap();
        assert_eq!(
            v,
            vec![
                AggOp::Count,
                AggOp::CountDistinct("id".to_string()),
                AggOp::Max("score".to_string()),
            ]
        );
    }

    #[test]
    fn parse_agg_ops_rejects_count_with_field() {
        let err = parse_agg_ops("count(id)").unwrap_err();
        assert!(err.contains("count() expects"), "{err}");
    }

    #[test]
    fn parse_agg_ops_rejects_unknown_op() {
        let err = parse_agg_ops("median(score)").unwrap_err();
        assert!(err.contains("unknown aggregate op"), "{err}");
    }

    #[test]
    fn aggregator_counts_per_subreddit() {
        let records = vec![
            json!({"subreddit": "rust", "id": "a"}),
            json!({"subreddit": "rust", "id": "b"}),
            json!({"subreddit": "go", "id": "c"}),
        ];
        let agg = run(
            records,
            ExprAggregator::new(vec![GroupKey::Subreddit], vec![AggOp::Count]),
        );
        let rows = agg.into_rows();
        assert_eq!(rows.len(), 2);
        let go = rows.iter().find(|r| r.get("subreddit").and_then(Value::as_str) == Some("go")).unwrap();
        assert_eq!(go.get("count"), Some(&json!(1)));
        let rust = rows.iter().find(|r| r.get("subreddit").and_then(Value::as_str) == Some("rust")).unwrap();
        assert_eq!(rust.get("count"), Some(&json!(2)));
    }

    #[test]
    fn aggregator_distinct_exact_below_threshold() {
        let mut agg = ExprAggregator::new(
            vec![GroupKey::Subreddit],
            vec![AggOp::CountDistinct("author".to_string())],
        );
        for i in 0..10 {
            agg.ingest(&json!({"subreddit": "rust", "author": format!("u{}", i % 4)}));
        }
        let row = agg
            .into_rows()
            .into_iter()
            .find(|r| r.get("subreddit").and_then(Value::as_str) == Some("rust"))
            .unwrap();
        assert_eq!(row.get("distinct_author"), Some(&json!(4)));
    }

    #[test]
    fn aggregator_distinct_promotes_to_hll_above_threshold() {
        // Cross the threshold so we exercise the promotion path; the estimate
        // should be within HLL's ~1.6 % relative error.
        let n = DISTINCT_HLL_THRESHOLD + 200;
        let mut agg = ExprAggregator::new(
            vec![GroupKey::Subreddit],
            vec![AggOp::CountDistinct("id".to_string())],
        );
        for i in 0..n {
            agg.ingest(&json!({"subreddit": "rust", "id": format!("id-{i}")}));
        }
        let row = agg
            .into_rows()
            .into_iter()
            .find(|r| r.get("subreddit").and_then(Value::as_str) == Some("rust"))
            .unwrap();
        let estimate = row.get("distinct_id").and_then(Value::as_u64).unwrap();
        let exact = n as f64;
        let err = (estimate as f64 - exact).abs() / exact;
        assert!(
            err < 0.05,
            "HLL estimate {estimate} too far from exact {exact}: rel err {err}",
        );
    }

    #[test]
    fn aggregator_sum_min_max_first_last() {
        let mut agg = ExprAggregator::new(
            vec![GroupKey::Subreddit],
            vec![
                AggOp::Sum("score".to_string()),
                AggOp::Min("score".to_string()),
                AggOp::Max("score".to_string()),
                AggOp::First("id".to_string()),
                AggOp::Last("id".to_string()),
            ],
        );
        agg.ingest(&json!({"subreddit": "rust", "score": 3, "id": "b", "created_utc": 200}));
        agg.ingest(&json!({"subreddit": "rust", "score": 5, "id": "a", "created_utc": 100}));
        agg.ingest(&json!({"subreddit": "rust", "score": 7, "id": "c", "created_utc": 300}));
        let row = agg.into_rows().into_iter().next().unwrap();
        assert_eq!(row.get("sum_score"), Some(&json!(15)));
        assert_eq!(row.get("min_score"), Some(&json!(3)));
        assert_eq!(row.get("max_score"), Some(&json!(7)));
        assert_eq!(row.get("first_id"), Some(&json!("a")));
        assert_eq!(row.get("last_id"), Some(&json!("c")));
    }

    #[test]
    fn aggregator_merge_associative() {
        let make = || ExprAggregator::new(
            vec![GroupKey::Subreddit],
            vec![AggOp::Count, AggOp::CountDistinct("id".to_string())],
        );
        let mut a = make();
        let mut b = make();
        let mut c = make();
        for i in 0..5 {
            a.ingest(&json!({"subreddit": "rust", "id": format!("x{i}")}));
        }
        for i in 0..5 {
            b.ingest(&json!({"subreddit": "rust", "id": format!("y{i}")}));
        }
        for i in 0..5 {
            c.ingest(&json!({"subreddit": "rust", "id": format!("x{i}")}));
        }

        let mut left = a.clone();
        let mut bc = b.clone();
        bc.merge(c.clone());
        left.merge(bc);

        let mut right = a;
        right.merge(b);
        right.merge(c);

        assert_eq!(left.row_count(), right.row_count());
        let lr = left.into_rows();
        let rr = right.into_rows();
        assert_eq!(lr, rr);
    }

    #[test]
    fn aggregator_identity_shard_adopts_schema() {
        // Default-constructed ExprAggregator has no schema and behaves as the
        // merge identity: ingesting into it is a no-op, and merging a
        // configured peer into it adopts the schema.
        let mut identity = ExprAggregator::default();
        identity.ingest(&json!({"subreddit": "rust"}));
        assert_eq!(identity.records_ingested(), 0);

        let mut real = ExprAggregator::new(vec![GroupKey::Subreddit], vec![AggOp::Count]);
        real.ingest(&json!({"subreddit": "rust"}));
        identity.merge(real);
        let rows = identity.into_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("count"), Some(&json!(1)));
    }

    #[test]
    fn aggregator_records_skipped_no_group_key() {
        let mut agg = ExprAggregator::new(
            vec![GroupKey::Subreddit, GroupKey::Author],
            vec![AggOp::Count],
        );
        agg.ingest(&json!({"subreddit": "rust", "author": "alice"}));
        agg.ingest(&json!({"subreddit": "rust"})); // missing author
        agg.ingest(&json!({"author": "bob"})); // missing subreddit
        assert_eq!(agg.records_ingested(), 3);
        assert_eq!(agg.records_skipped_no_group_key(), 2);
        assert_eq!(agg.row_count(), 1);
    }

    #[test]
    fn aggregator_serializes_and_round_trips_via_json() {
        let mut agg = ExprAggregator::new(
            vec![GroupKey::Subreddit],
            vec![
                AggOp::Count,
                AggOp::CountDistinct("id".to_string()),
                AggOp::Sum("score".to_string()),
                AggOp::Min("score".to_string()),
                AggOp::First("id".to_string()),
            ],
        );
        agg.ingest(&json!({"subreddit": "rust", "id": "a", "score": 5, "created_utc": 100}));
        let s = serde_json::to_string(&agg).expect("serialize ExprAggregator");
        let _back: ExprAggregator = serde_json::from_str(&s).expect("deserialize ExprAggregator");
    }

    #[test]
    fn hll_estimate_within_error_bound() {
        let mut hll = HllState::new();
        let n = 50_000u64;
        for i in 0..n {
            let mut hasher = AHasher::default();
            hasher.write_u64(i);
            hll.add_hash(hasher.finish());
        }
        let est = hll.estimate();
        let rel_err = (est as f64 - n as f64).abs() / n as f64;
        // Configured precision gives ~1.6 % standard error; allow 5 % to keep
        // the test stable across hash distributions.
        assert!(rel_err < 0.05, "HLL est {est}, exact {n}, rel err {rel_err}");
    }
}
