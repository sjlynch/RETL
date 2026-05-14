//! Shared helpers for the `retl` binary's subcommand handlers:
//! the `RecCount` aggregator, ETL builder glue, the `plan!` macro, and a
//! couple of CLI-only path/I/O helpers.

use anyhow::{Context, Result};
use retl::{
    Aggregator, ConfigBuildError, JsonPointerPredicate, NumericComparison, RedditETL, Sources,
    YearMonth,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use time::OffsetDateTime;

use crate::bin_args::CommonOpts;

// -----------------------------------------------------------------------------
// Aggregator used by the `aggregate` subcommand.
// -----------------------------------------------------------------------------

/// Built-in aggregator: counts records across the supplied JSONL inputs.
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct RecCount {
    pub(crate) count: u64,
}

impl Aggregator for RecCount {
    fn ingest(&mut self, _record: &Value) {
        self.count += 1;
    }
    fn merge(&mut self, other: Self) {
        self.count += other.count;
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum GroupBySpec {
    Subreddit,
    Author,
    Month,
    JsonPointer(String),
}

impl GroupBySpec {
    pub(crate) fn parse(raw: &str) -> Result<Self> {
        match raw {
            "subreddit" => Ok(Self::Subreddit),
            "author" => Ok(Self::Author),
            "month" => Ok(Self::Month),
            s if s.starts_with("json:") => {
                let pointer = s.trim_start_matches("json:");
                validate_json_pointer(pointer)?;
                Ok(Self::JsonPointer(pointer.to_string()))
            }
            _ => anyhow::bail!(
                "unsupported --by {raw:?}; expected subreddit, author, month, or json:/pointer"
            ),
        }
    }

    fn key_for(&self, record: &Value) -> Option<String> {
        match self {
            Self::Subreddit => record.get("subreddit").and_then(value_to_key),
            Self::Author => record.get("author").and_then(value_to_key),
            Self::Month => record.get("created_utc").and_then(value_to_month),
            Self::JsonPointer(pointer) => record.pointer(pointer).and_then(value_to_key),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum MetricKind {
    #[default]
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MetricSpec {
    kind: MetricKind,
    pointer: Option<String>,
}

impl MetricSpec {
    pub(crate) fn parse(raw: Option<&str>) -> Result<Self> {
        let Some(raw) = raw else {
            return Ok(Self::default());
        };
        if raw == "count" {
            return Ok(Self::default());
        }
        let (kind_raw, pointer) = raw.split_once(':').ok_or_else(|| {
            anyhow::anyhow!(
                "unsupported --metric {raw:?}; expected count, sum:/pointer, avg:/pointer, min:/pointer, or max:/pointer"
            )
        })?;
        validate_json_pointer(pointer)?;
        let kind = match kind_raw {
            "sum" => MetricKind::Sum,
            "avg" => MetricKind::Avg,
            "min" => MetricKind::Min,
            "max" => MetricKind::Max,
            _ => anyhow::bail!(
                "unsupported --metric {raw:?}; expected count, sum:/pointer, avg:/pointer, min:/pointer, or max:/pointer"
            ),
        };
        Ok(Self {
            kind,
            pointer: Some(pointer.to_string()),
        })
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct MetricState {
    count: u64,
    sum: f64,
    min: Option<f64>,
    max: Option<f64>,
}

impl MetricState {
    fn ingest_number(&mut self, n: f64) {
        self.count += 1;
        self.sum += n;
        self.min = Some(self.min.map_or(n, |old| old.min(n)));
        self.max = Some(self.max.map_or(n, |old| old.max(n)));
    }

    fn merge(&mut self, other: Self) {
        self.count += other.count;
        self.sum += other.sum;
        if let Some(n) = other.min {
            self.min = Some(self.min.map_or(n, |old| old.min(n)));
        }
        if let Some(n) = other.max {
            self.max = Some(self.max.map_or(n, |old| old.max(n)));
        }
    }
}

/// Built-in grouped aggregator used by `retl aggregate --by ...`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct GroupMetricAgg {
    group_by: Option<GroupBySpec>,
    metric: MetricSpec,
    groups: BTreeMap<String, MetricState>,
}

impl GroupMetricAgg {
    pub(crate) fn new(group_by: GroupBySpec, metric: MetricSpec) -> Self {
        Self {
            group_by: Some(group_by),
            metric,
            groups: BTreeMap::new(),
        }
    }

    pub(crate) fn rows(&self, top: Option<usize>) -> Vec<(String, String)> {
        let mut rows: Vec<(String, String, f64)> = self
            .groups
            .iter()
            .filter_map(|(key, state)| {
                let (display, sort_value) = match self.metric.kind {
                    MetricKind::Count => (state.count.to_string(), state.count as f64),
                    MetricKind::Sum => (format_number(state.sum), state.sum),
                    MetricKind::Avg => {
                        if state.count == 0 {
                            return None;
                        }
                        let avg = state.sum / state.count as f64;
                        (format_number(avg), avg)
                    }
                    MetricKind::Min => {
                        let n = state.min?;
                        (format_number(n), n)
                    }
                    MetricKind::Max => {
                        let n = state.max?;
                        (format_number(n), n)
                    }
                };
                Some((key.clone(), display, sort_value))
            })
            .collect();

        if let Some(limit) = top {
            rows.sort_by(|a, b| b.2.total_cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
            rows.truncate(limit);
        }

        rows.into_iter()
            .map(|(key, value, _)| (key, value))
            .collect()
    }
}

impl Aggregator for GroupMetricAgg {
    fn ingest(&mut self, record: &Value) {
        let Some(group_by) = &self.group_by else {
            return;
        };
        let Some(key) = group_by.key_for(record) else {
            return;
        };

        if self.metric.kind == MetricKind::Count {
            self.groups.entry(key).or_default().count += 1;
            return;
        }

        let Some(pointer) = &self.metric.pointer else {
            return;
        };
        let Some(n) = record.pointer(pointer).and_then(value_to_f64) else {
            return;
        };
        self.groups.entry(key).or_default().ingest_number(n);
    }

    fn merge(&mut self, other: Self) {
        if let (Some(left_by), Some(right_by)) = (&self.group_by, &other.group_by) {
            if left_by != right_by || self.metric != other.metric {
                panic!(
                    "refusing to merge incompatible grouped aggregate shards: left by={:?} metric={:?}, right by={:?} metric={:?}",
                    left_by, self.metric, right_by, other.metric
                );
            }
        } else if self.group_by.is_none() {
            self.group_by = other.group_by.clone();
            self.metric = other.metric.clone();
        } else if !other.groups.is_empty() {
            panic!("refusing to merge grouped aggregate shard with missing group_by metadata");
        }
        for (key, state) in other.groups {
            self.groups.entry(key).or_default().merge(state);
        }
    }
}

fn validate_json_pointer(pointer: &str) -> Result<()> {
    if pointer.is_empty() || pointer.starts_with('/') {
        Ok(())
    } else {
        anyhow::bail!("JSON pointers must be empty or start with '/': {pointer:?}")
    }
}

fn value_to_key(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(v).ok(),
    }
}

fn value_to_f64(v: &Value) -> Option<f64> {
    let n = match v {
        Value::Number(n) => n.as_f64()?,
        Value::String(s) => s.parse::<f64>().ok()?,
        _ => return None,
    };
    n.is_finite().then_some(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "refusing to merge incompatible grouped aggregate shards")]
    fn group_metric_merge_rejects_incompatible_metadata() {
        let mut left = GroupMetricAgg::new(GroupBySpec::Subreddit, MetricSpec::default());
        let right = GroupMetricAgg::new(GroupBySpec::Author, MetricSpec::default());
        left.merge(right);
    }
}

fn value_to_month(v: &Value) -> Option<String> {
    match v {
        Value::String(s) if looks_like_year_month_prefix(s) => Some(s[..7].to_string()),
        Value::String(s) => s.parse::<i64>().ok().and_then(unix_seconds_to_month),
        Value::Number(n) => n.as_i64().and_then(unix_seconds_to_month),
        _ => None,
    }
}

fn looks_like_year_month_prefix(s: &str) -> bool {
    s.len() >= 7
        && s.as_bytes()[4] == b'-'
        && s.as_bytes()[0..4].iter().all(u8::is_ascii_digit)
        && s.as_bytes()[5..7].iter().all(u8::is_ascii_digit)
}

fn unix_seconds_to_month(secs: i64) -> Option<String> {
    let dt = OffsetDateTime::from_unix_timestamp(secs).ok()?;
    Some(format!("{:04}-{:02}", dt.year(), u8::from(dt.month())))
}

fn format_number(n: f64) -> String {
    n.to_string()
}

// -----------------------------------------------------------------------------
// Builder helpers.
// -----------------------------------------------------------------------------

pub(crate) fn ensure_dirs(common: &CommonOpts) -> Result<PathBuf> {
    fs::create_dir_all(&common.work_dir)
        .with_context(|| format!("creating work_dir {}", common.work_dir.display()))?;
    let lib_tmp = common.work_dir.join("lib_tmp");
    fs::create_dir_all(&lib_tmp)
        .with_context(|| format!("creating work_dir {}", lib_tmp.display()))?;
    Ok(lib_tmp)
}

pub(crate) fn parse_json_predicate(raw: &str) -> Result<JsonPointerPredicate> {
    let raw = raw.trim();
    if raw.is_empty() {
        anyhow::bail!("bad --json predicate: predicate is empty");
    }

    if let Some(pointer) = raw.strip_prefix("exists:") {
        validate_cli_json_pointer(pointer, raw)?;
        return Ok(JsonPointerPredicate::exists(pointer.to_string()));
    }

    let (pointer, op, value) = split_json_predicate(raw)?;
    validate_cli_json_pointer(pointer, raw)?;

    match op {
        "=" => Ok(JsonPointerPredicate::equals(
            pointer.to_string(),
            parse_json_scalar(value, raw, op)?,
        )),
        "!=" => Ok(JsonPointerPredicate::not_equals(
            pointer.to_string(),
            parse_json_scalar(value, raw, op)?,
        )),
        ">" | ">=" | "<" | "<=" => {
            let number = value.parse::<f64>().map_err(|_| {
                anyhow::anyhow!(
                    "bad --json predicate {raw:?}: value {value:?} is not a number for operator {op:?}"
                )
            })?;
            if !number.is_finite() {
                anyhow::bail!(
                    "bad --json predicate {raw:?}: value {value:?} is not finite for operator {op:?}"
                );
            }
            let cmp = match op {
                ">" => NumericComparison::GreaterThan,
                ">=" => NumericComparison::GreaterThanOrEqual,
                "<" => NumericComparison::LessThan,
                "<=" => NumericComparison::LessThanOrEqual,
                _ => unreachable!(),
            };
            Ok(JsonPointerPredicate::number(
                pointer.to_string(),
                cmp,
                number,
            ))
        }
        "~=" => {
            if value.is_empty() {
                anyhow::bail!(
                    "bad --json predicate {raw:?}: value is empty for regex operator {op:?}"
                );
            }
            Ok(JsonPointerPredicate::regex(
                pointer.to_string(),
                value.to_string(),
            ))
        }
        _ => unreachable!("split_json_predicate only returns supported operators"),
    }
}

fn split_json_predicate(raw: &str) -> Result<(&str, &'static str, &str)> {
    let mut best: Option<(usize, &'static str)> = None;
    for op in ["~=", ">=", "<=", "!=", "=", ">", "<"] {
        if let Some(idx) = raw.find(op) {
            let replace = best
                .map(|(best_idx, best_op)| {
                    idx < best_idx || (idx == best_idx && op.len() > best_op.len())
                })
                .unwrap_or(true);
            if replace {
                best = Some((idx, op));
            }
        }
    }

    if let Some((idx, op)) = best {
        let pointer = &raw[..idx];
        let value = &raw[idx + op.len()..];
        if pointer.is_empty() {
            anyhow::bail!("bad --json predicate {raw:?}: pointer is empty before operator {op:?}");
        }
        if value.is_empty() {
            anyhow::bail!("bad --json predicate {raw:?}: value is empty for operator {op:?}");
        }
        return Ok((pointer, op, value));
    }

    anyhow::bail!(
        "bad --json predicate {raw:?}: missing or unsupported operator; expected exists:/path, /path=value, /path!=value, /path>value, /path>=value, /path<value, /path<=value, or /path~=REGEX"
    )
}

fn validate_cli_json_pointer(pointer: &str, raw: &str) -> Result<()> {
    if !pointer.starts_with('/') {
        anyhow::bail!("bad --json predicate {raw:?}: pointer {pointer:?} must start with '/'");
    }
    let mut chars = pointer.chars();
    while let Some(ch) = chars.next() {
        if ch == '~' {
            match chars.next() {
                Some('0') | Some('1') => {}
                other => anyhow::bail!(
                    "bad --json predicate {raw:?}: pointer {pointer:?} has invalid escape near '~{:?}'; use '~0' for '~' and '~1' for '/'",
                    other
                ),
            }
        }
    }
    Ok(())
}

fn parse_json_scalar(raw_value: &str, raw_predicate: &str, op: &str) -> Result<Value> {
    match serde_json::from_str::<Value>(raw_value) {
        Ok(value) if matches!(
            value,
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)
        ) => Ok(value),
        Ok(value) => anyhow::bail!(
            "bad --json predicate {raw_predicate:?}: value {raw_value:?} for operator {op:?} must be a JSON scalar, not {}",
            value_type_name(&value)
        ),
        Err(_) => Ok(Value::String(raw_value.to_string())),
    }
}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

pub(crate) fn build_etl(common: &CommonOpts) -> Result<RedditETL> {
    if let (Some(start), Some(end)) = (common.start, common.end) {
        if start > end {
            return Err(ConfigBuildError::InvalidDateRange { start, end }.into());
        }
    }
    let lib_tmp = ensure_dirs(common)?;
    let mut etl = RedditETL::new()
        .base_dir(&common.data_dir)
        .work_dir(&lib_tmp)
        .progress(!common.no_progress)
        .sources(Sources::from(common.source))
        .date_range(common.start, common.end);

    if let Some(p) = common.parallelism {
        etl = etl.parallelism(p);
    }
    if let Some(fc) = common.file_concurrency {
        etl = etl.file_concurrency(fc);
    }
    Ok(etl)
}

/// Build a `ScanPlan` from `etl` with common CLI scan selections applied.
/// Kept as a macro so the binary call sites stay concise while sharing the
/// same public builder surface external users get from `retl::ScanPlan`.
macro_rules! plan {
    ($etl:expr, $common:expr, $query:expr) => {{
        let common = &$common;
        let query = &$query;
        let mut scan = $etl.scan();
        if !common.subreddits.is_empty() {
            scan = scan.subreddits(common.subreddits.iter().map(String::as_str));
        }
        if !query.authors.is_empty() {
            scan = scan.authors_in(query.authors.iter().map(String::as_str));
        }
        if !query.exclude_authors.is_empty() {
            scan = scan.authors_out(query.exclude_authors.iter().map(String::as_str));
        }
        if query.exclude_common_bots {
            scan = scan.exclude_common_bots();
        }
        if let Some(author_regex) = &query.author_regex {
            scan = scan.author_regex(author_regex.as_str());
        }
        if !query.keywords.is_empty() {
            scan = scan.keywords_any(query.keywords.iter().map(String::as_str));
        }
        if let Some(min_score) = query.min_score {
            scan = scan.min_score(min_score);
        }
        if let Some(max_score) = query.max_score {
            scan = scan.max_score(max_score);
        }
        if query.contains_url {
            scan = scan.contains_url(true);
        }
        if !query.domains.is_empty() {
            scan = scan.domains_in(query.domains.iter().map(String::as_str));
        }
        for json_predicate in &query.json_predicates {
            scan = scan.json_predicate($crate::bin_helpers::parse_json_predicate(json_predicate)?);
        }
        if common.include_deleted {
            scan = scan.include_pseudo_users();
        }
        scan
    }};
}
pub(crate) use plan;

// -----------------------------------------------------------------------------
// CLI-only path / I/O helpers.
// -----------------------------------------------------------------------------

/// Run an operation that writes to a file path, then stream the resulting
/// file to stdout and remove it. Used to honor `--out -` for APIs that only
/// know how to write to a `Path`.
pub(crate) fn stream_path_output_to_stdout(
    work_dir: &Path,
    temp_prefix: &str,
    file_stem: &str,
    write_output: impl FnOnce(&Path) -> Result<()>,
) -> Result<()> {
    let lib_tmp = work_dir.join("lib_tmp");
    fs::create_dir_all(&lib_tmp)
        .with_context(|| format!("creating work_dir {}", lib_tmp.display()))?;
    let tmp_path = lib_tmp.join(format!(
        "retl_{}_{}_{}",
        temp_prefix,
        std::process::id(),
        file_stem
    ));
    let _ = fs::remove_file(&tmp_path);

    let result = write_output(&tmp_path);

    if let Err(e) = result {
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }

    let copy_result = (|| -> Result<()> {
        let mut f = fs::File::open(&tmp_path)
            .with_context(|| format!("opening {temp_prefix} tempfile {}", tmp_path.display()))?;
        let stdout = io::stdout();
        let mut w = stdout.lock();
        io::copy(&mut f, &mut w)
            .with_context(|| format!("streaming {temp_prefix} output to stdout"))?;
        w.flush()?;
        Ok(())
    })();

    let _ = fs::remove_file(&tmp_path);
    copy_result
}

/// Run an extraction that writes to a file path, then stream the resulting
/// file to stdout and remove it.
pub(crate) fn stream_extract_to_stdout(
    work_dir: &Path,
    file_stem: &str,
    extract: impl FnOnce(&Path) -> Result<()>,
) -> Result<()> {
    stream_path_output_to_stdout(work_dir, "export", file_stem, extract)
}

/// Discover spool parts in `dir`, parsing `part_RC_YYYY-MM.jsonl` and
/// `part_RS_YYYY-MM.jsonl` filenames. Returns `(sorted_paths, min, max)`.
pub(crate) fn discover_spool_parts(dir: &Path) -> Result<(Vec<PathBuf>, YearMonth, YearMonth)> {
    let entries =
        fs::read_dir(dir).with_context(|| format!("reading spool dir {}", dir.display()))?;
    let mut parts: Vec<(YearMonth, PathBuf)> = Vec::new();
    for e in entries {
        let e = e?;
        let name = e.file_name().to_string_lossy().into_owned();
        let stem = name
            .strip_prefix("part_RC_")
            .or_else(|| name.strip_prefix("part_RS_"))
            .and_then(|s| s.strip_suffix(".jsonl"));
        if let Some(stem) = stem {
            if let Ok(ym) = stem.parse::<YearMonth>() {
                parts.push((ym, e.path()));
            }
        }
    }
    if parts.is_empty() {
        anyhow::bail!(
            "no part_RC_YYYY-MM.jsonl or part_RS_YYYY-MM.jsonl files found in {}",
            dir.display()
        );
    }
    parts.sort_by(|a, b| a.1.cmp(&b.1));
    let min_ym = parts.iter().map(|(ym, _)| *ym).min().unwrap();
    let max_ym = parts.iter().map(|(ym, _)| *ym).max().unwrap();
    Ok((parts.into_iter().map(|(_, p)| p).collect(), min_ym, max_ym))
}
