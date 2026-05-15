//! Shared helpers for the `retl` binary's subcommand handlers:
//! the `RecCount` aggregator, ETL builder glue, the `plan!` macro, and a
//! couple of CLI-only path/I/O helpers.

use anyhow::{Context, Result};
use retl::{
    create_dir_all_with_backoff, open_with_backoff, read_dir_with_backoff, remove_with_backoff,
    Aggregator, ConfigBuildError, JsonPointerPredicate, NumericComparison, PartialReadReporter,
    RedditETL, Sources, YearMonth,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::BTreeMap;
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

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
enum MetricNumber {
    Int(i128),
    Float(f64),
}

impl MetricNumber {
    fn format(self, format: NumberFormat) -> String {
        match self {
            Self::Int(n) => n.to_string(),
            Self::Float(n) => format_number(n, format),
        }
    }

    fn sort_value(self) -> MetricSortValue {
        match self {
            Self::Int(n) => MetricSortValue::Int(n),
            Self::Float(n) => MetricSortValue::Float(n),
        }
    }

    fn cmp_numeric(&self, other: &Self) -> Ordering {
        match (*self, *other) {
            (Self::Int(a), Self::Int(b)) => a.cmp(&b),
            (Self::Float(a), Self::Float(b)) => a.total_cmp(&b),
            (Self::Int(a), Self::Float(b)) => cmp_i128_f64(a, b),
            (Self::Float(a), Self::Int(b)) => cmp_i128_f64(b, a).reverse(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
enum NumericSum {
    Int(i128),
    Float(f64),
}

impl Default for NumericSum {
    fn default() -> Self {
        Self::Int(0)
    }
}

impl NumericSum {
    fn add_number(&mut self, n: MetricNumber) {
        match (*self, n) {
            (Self::Int(sum), MetricNumber::Int(n)) => {
                *self = sum
                    .checked_add(n)
                    .map(Self::Int)
                    .unwrap_or_else(|| Self::Float(sum as f64 + n as f64));
            }
            (Self::Int(sum), MetricNumber::Float(n)) => {
                *self = Self::Float(sum as f64 + n);
            }
            (Self::Float(sum), MetricNumber::Int(n)) => {
                *self = Self::Float(sum + n as f64);
            }
            (Self::Float(sum), MetricNumber::Float(n)) => {
                *self = Self::Float(sum + n);
            }
        }
    }

    fn merge(&mut self, other: Self) {
        match other {
            Self::Int(n) => self.add_number(MetricNumber::Int(n)),
            Self::Float(n) => self.add_number(MetricNumber::Float(n)),
        }
    }

    fn format_sum(self, format: NumberFormat) -> String {
        match self {
            Self::Int(n) => n.to_string(),
            Self::Float(n) => format_number(n, format),
        }
    }

    fn format_avg(self, count: u64, format: NumberFormat) -> String {
        debug_assert!(count > 0);
        match self {
            Self::Int(sum) => format_integer_average(sum, count),
            Self::Float(sum) => format_number(sum / count as f64, format),
        }
    }

    fn sort_sum(self) -> MetricSortValue {
        match self {
            Self::Int(n) => MetricSortValue::Int(n),
            Self::Float(n) => MetricSortValue::Float(n),
        }
    }

    fn sort_avg(self, count: u64) -> MetricSortValue {
        debug_assert!(count > 0);
        match self {
            Self::Int(sum) => MetricSortValue::Ratio {
                numerator: sum,
                denominator: count,
            },
            Self::Float(sum) => MetricSortValue::Float(sum / count as f64),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum MetricSortValue {
    Int(i128),
    Float(f64),
    Ratio { numerator: i128, denominator: u64 },
}

impl MetricSortValue {
    fn cmp_numeric(&self, other: &Self) -> Ordering {
        match (*self, *other) {
            (Self::Int(a), Self::Int(b)) => a.cmp(&b),
            (Self::Float(a), Self::Float(b)) => a.total_cmp(&b),
            (
                Self::Ratio {
                    numerator: a_num,
                    denominator: a_den,
                },
                Self::Ratio {
                    numerator: b_num,
                    denominator: b_den,
                },
            ) => cmp_i128_ratios(a_num, a_den, b_num, b_den),
            (
                Self::Ratio {
                    numerator,
                    denominator,
                },
                Self::Int(n),
            ) => cmp_i128_ratios(numerator, denominator, n, 1),
            (
                Self::Int(n),
                Self::Ratio {
                    numerator,
                    denominator,
                },
            ) => cmp_i128_ratios(n, 1, numerator, denominator),
            (Self::Int(a), Self::Float(b)) => cmp_i128_f64(a, b),
            (Self::Float(a), Self::Int(b)) => cmp_i128_f64(b, a).reverse(),
            (Self::Float(a), other) => a.total_cmp(&other.as_f64_lossy()),
            (other, Self::Float(b)) => other.as_f64_lossy().total_cmp(&b),
        }
    }

    fn as_f64_lossy(self) -> f64 {
        match self {
            Self::Int(n) => n as f64,
            Self::Float(n) => n,
            Self::Ratio {
                numerator,
                denominator,
            } => numerator as f64 / denominator as f64,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct MetricState {
    count: u64,
    sum: NumericSum,
    min: Option<MetricNumber>,
    max: Option<MetricNumber>,
}

impl MetricState {
    fn ingest_number(&mut self, n: MetricNumber) {
        self.count += 1;
        self.sum.add_number(n);
        self.min = Some(
            self.min
                .map_or(n, |old| if n.cmp_numeric(&old).is_lt() { n } else { old }),
        );
        self.max = Some(
            self.max
                .map_or(n, |old| if n.cmp_numeric(&old).is_gt() { n } else { old }),
        );
    }

    fn merge(&mut self, other: Self) {
        self.count += other.count;
        self.sum.merge(other.sum);
        if let Some(n) = other.min {
            self.min = Some(
                self.min
                    .map_or(n, |old| if n.cmp_numeric(&old).is_lt() { n } else { old }),
            );
        }
        if let Some(n) = other.max {
            self.max = Some(
                self.max
                    .map_or(n, |old| if n.cmp_numeric(&old).is_gt() { n } else { old }),
            );
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
        self.rows_with_scientific(top, false)
    }

    pub(crate) fn rows_with_scientific(
        &self,
        top: Option<usize>,
        scientific: bool,
    ) -> Vec<(String, String)> {
        let number_format = NumberFormat::from_scientific(scientific);
        let mut rows: Vec<(String, String, MetricSortValue)> = self
            .groups
            .iter()
            .filter_map(|(key, state)| {
                let (display, sort_value) = match self.metric.kind {
                    MetricKind::Count => (
                        state.count.to_string(),
                        MetricSortValue::Int(state.count as i128),
                    ),
                    MetricKind::Sum => (state.sum.format_sum(number_format), state.sum.sort_sum()),
                    MetricKind::Avg => {
                        if state.count == 0 {
                            return None;
                        }
                        (
                            state.sum.format_avg(state.count, number_format),
                            state.sum.sort_avg(state.count),
                        )
                    }
                    MetricKind::Min => {
                        let n = state.min?;
                        (n.format(number_format), n.sort_value())
                    }
                    MetricKind::Max => {
                        let n = state.max?;
                        (n.format(number_format), n.sort_value())
                    }
                };
                Some((key.clone(), display, sort_value))
            })
            .collect();

        if let Some(limit) = top {
            rows.sort_by(|a, b| b.2.cmp_numeric(&a.2).then_with(|| a.0.cmp(&b.0)));
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
        let Some(n) = record.pointer(pointer).and_then(value_to_metric_number) else {
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

fn value_to_metric_number(v: &Value) -> Option<MetricNumber> {
    match v {
        Value::Number(n) => metric_number_from_str(&n.to_string()),
        Value::String(s) => metric_number_from_str(s),
        _ => None,
    }
}

fn metric_number_from_str(raw: &str) -> Option<MetricNumber> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }
    if is_plain_integer_literal(s) {
        if let Ok(n) = s.parse::<i128>() {
            return Some(MetricNumber::Int(n));
        }
    }
    let n = s.parse::<f64>().ok()?;
    n.is_finite().then_some(MetricNumber::Float(n))
}

fn is_plain_integer_literal(s: &str) -> bool {
    let digits = s
        .strip_prefix('-')
        .or_else(|| s.strip_prefix('+'))
        .unwrap_or(s);
    !digits.is_empty() && digits.as_bytes().iter().all(u8::is_ascii_digit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    #[should_panic(expected = "refusing to merge incompatible grouped aggregate shards")]
    fn group_metric_merge_rejects_incompatible_metadata() {
        let mut left = GroupMetricAgg::new(GroupBySpec::Subreddit, MetricSpec::default());
        let right = GroupMetricAgg::new(GroupBySpec::Author, MetricSpec::default());
        left.merge(right);
    }

    #[test]
    fn discover_spool_parts_orders_by_year_month_then_path() {
        let dir = tempfile::tempdir().unwrap();
        for name in [
            "part_RC_2006-02.jsonl",
            "part_RS_2006-01.jsonl",
            "part_RS_2006-02.jsonl",
            "part_RC_2006-01.jsonl",
        ] {
            fs::write(dir.path().join(name), "{}\n").unwrap();
        }

        let (parts, min_ym, max_ym) = discover_spool_parts(dir.path()).unwrap();
        let names: Vec<String> = parts
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();

        assert_eq!(min_ym, YearMonth::new(2006, 1));
        assert_eq!(max_ym, YearMonth::new(2006, 2));
        assert_eq!(
            names,
            vec![
                "part_RC_2006-01.jsonl",
                "part_RS_2006-01.jsonl",
                "part_RC_2006-02.jsonl",
                "part_RS_2006-02.jsonl",
            ]
        );
    }

    #[test]
    fn format_number_integer_scores_use_plain_decimal() {
        let rendered = format_number(1_000_000_000_000_000.0, NumberFormat::Decimal);
        assert_eq!(rendered, "1000000000000000");
        assert!(!rendered.contains('e') && !rendered.contains('E'));
    }

    #[test]
    fn format_number_average_uses_stable_decimal() {
        let rendered = format_number(2.5, NumberFormat::Decimal);
        assert_eq!(rendered, "2.5");
        assert!(!rendered.contains('e') && !rendered.contains('E'));
    }

    #[test]
    fn format_integer_average_uses_documented_precision() {
        let rendered = format_integer_average(1, 7);
        assert_eq!(rendered, "0.142857142857142857");
        assert!(!rendered.contains('e') && !rendered.contains('E'));
    }

    #[test]
    fn metric_number_parses_large_integer_strings_exactly() {
        assert_eq!(
            metric_number_from_str("9007199254740993"),
            Some(MetricNumber::Int(9_007_199_254_740_993))
        );
    }

    #[test]
    fn value_to_metric_number_reads_large_json_integer_exactly() {
        let value: Value = serde_json::from_str("90071992547409931234").unwrap();
        assert_eq!(
            value_to_metric_number(&value),
            Some(MetricNumber::Int(90_071_992_547_409_931_234_i128))
        );
    }

    #[test]
    fn value_to_month_rejects_invalid_string_months() {
        assert_eq!(string_to_month("2024-00-01T00:00:00Z"), None);
        assert_eq!(string_to_month("2024-13-01T00:00:00Z"), None);
        assert_eq!(string_to_month("2024-99-01T00:00:00Z"), None);
    }

    #[test]
    fn value_to_month_accepts_valid_string_month_prefixes() {
        for month in [
            "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12",
        ] {
            assert_eq!(
                string_to_month(&format!("2024-{month}-01T00:00:00Z")),
                Some(format!("2024-{month}"))
            );
        }
    }

    fn string_to_month(s: &str) -> Option<String> {
        value_to_month(&Value::String(s.to_string()))
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
    if !(s.len() >= 7
        && s.as_bytes()[4] == b'-'
        && s.as_bytes()[0..4].iter().all(u8::is_ascii_digit)
        && s.as_bytes()[5..7].iter().all(u8::is_ascii_digit))
    {
        return false;
    }

    matches!(s[5..7].parse::<u8>(), Ok(1..=12))
}

fn unix_seconds_to_month(secs: i64) -> Option<String> {
    let dt = OffsetDateTime::from_unix_timestamp(secs).ok()?;
    Some(format!("{:04}-{:02}", dt.year(), u8::from(dt.month())))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NumberFormat {
    Decimal,
    Scientific,
}

impl NumberFormat {
    fn from_scientific(scientific: bool) -> Self {
        if scientific {
            Self::Scientific
        } else {
            Self::Decimal
        }
    }
}

fn format_number(n: f64, format: NumberFormat) -> String {
    if !n.is_finite() {
        return n.to_string();
    }

    let rendered = n.to_string();
    let rendered = if format == NumberFormat::Decimal {
        expand_exponent_notation(&rendered).unwrap_or(rendered)
    } else {
        rendered
    };
    normalize_negative_zero(rendered)
}

const INTEGER_AVERAGE_FRACTIONAL_DIGITS: usize = 18;

fn format_integer_average(sum: i128, count: u64) -> String {
    debug_assert!(count > 0);
    let denom = count as u128;
    let negative = sum.is_negative();
    let abs_sum = sum.unsigned_abs();
    let mut integer = abs_sum / denom;
    let mut rem = abs_sum % denom;
    if rem == 0 {
        let mut out = String::new();
        if negative {
            out.push('-');
        }
        out.push_str(&integer.to_string());
        return normalize_negative_zero(out);
    }

    let mut digits = Vec::with_capacity(INTEGER_AVERAGE_FRACTIONAL_DIGITS);
    for _ in 0..INTEGER_AVERAGE_FRACTIONAL_DIGITS {
        rem *= 10;
        digits.push((rem / denom) as u8);
        rem %= denom;
    }

    if rem * 2 >= denom {
        let mut carry = true;
        for digit in digits.iter_mut().rev() {
            if *digit == 9 {
                *digit = 0;
            } else {
                *digit += 1;
                carry = false;
                break;
            }
        }
        if carry {
            integer += 1;
        }
    }

    while digits.last() == Some(&0) {
        digits.pop();
    }

    let mut out = String::new();
    if negative {
        out.push('-');
    }
    out.push_str(&integer.to_string());
    if !digits.is_empty() {
        out.push('.');
        for digit in digits {
            out.push(char::from(b'0' + digit));
        }
    }
    normalize_negative_zero(out)
}

fn expand_exponent_notation(s: &str) -> Option<String> {
    let e = s.find('e').or_else(|| s.find('E'))?;
    let exponent: i32 = s[e + 1..].parse().ok()?;
    let mut mantissa = &s[..e];
    let sign = if let Some(rest) = mantissa.strip_prefix('-') {
        mantissa = rest;
        "-"
    } else if let Some(rest) = mantissa.strip_prefix('+') {
        mantissa = rest;
        ""
    } else {
        ""
    };

    let integer_digits = mantissa.find('.').unwrap_or(mantissa.len());
    let digits: String = mantissa.chars().filter(|&ch| ch != '.').collect();
    if digits.is_empty() {
        return None;
    }

    let decimal_pos = integer_digits as i32 + exponent;
    let mut out = String::new();
    out.push_str(sign);
    if decimal_pos <= 0 {
        out.push_str("0.");
        for _ in 0..decimal_pos.unsigned_abs() {
            out.push('0');
        }
        out.push_str(&digits);
    } else if decimal_pos as usize >= digits.len() {
        out.push_str(&digits);
        for _ in 0..(decimal_pos as usize - digits.len()) {
            out.push('0');
        }
    } else {
        let split = decimal_pos as usize;
        out.push_str(&digits[..split]);
        out.push('.');
        out.push_str(&digits[split..]);
    }
    Some(normalize_negative_zero(out))
}

fn cmp_i128_ratios(a_num: i128, a_den: u64, b_num: i128, b_den: u64) -> Ordering {
    debug_assert!(a_den > 0 && b_den > 0);
    match (
        a_num.checked_mul(b_den as i128),
        b_num.checked_mul(a_den as i128),
    ) {
        (Some(a), Some(b)) => a.cmp(&b),
        _ => (a_num as f64 / a_den as f64).total_cmp(&(b_num as f64 / b_den as f64)),
    }
}

fn cmp_i128_f64(i: i128, f: f64) -> Ordering {
    if f.is_nan() {
        return Ordering::Equal;
    }
    if f.is_infinite() {
        return if f.is_sign_positive() {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    (i as f64).total_cmp(&f)
}

fn normalize_negative_zero(s: String) -> String {
    if s == "-0" {
        "0".to_string()
    } else {
        s
    }
}

// -----------------------------------------------------------------------------
// Builder helpers.
// -----------------------------------------------------------------------------

pub(crate) fn ensure_dirs(common: &CommonOpts) -> Result<PathBuf> {
    create_dir_all_with_backoff(&common.work_dir, 16, 50)
        .with_context(|| format!("creating work_dir {}", common.work_dir.display()))?;
    let lib_tmp = common.work_dir.join("lib_tmp");
    create_dir_all_with_backoff(&lib_tmp, 16, 50)
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
        .date_range(common.start, common.end)
        .allow_partial(common.allow_partial);

    if let Some(p) = common.parallelism {
        etl = etl.parallelism(p);
    }
    if let Some(fc) = common.file_concurrency {
        etl = etl.file_concurrency(fc);
    }
    Ok(etl)
}

pub(crate) fn emit_partial_read_report(reporter: &PartialReadReporter) -> Result<()> {
    let report = reporter.snapshot();
    if report.skipped_file_count == 0 {
        return Ok(());
    }
    eprintln!("{}", serde_json::to_string(&report)?);
    Ok(())
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
    create_dir_all_with_backoff(&lib_tmp, 16, 50)
        .with_context(|| format!("creating work_dir {}", lib_tmp.display()))?;
    let tmp_path = lib_tmp.join(format!(
        "retl_{}_{}_{}",
        temp_prefix,
        std::process::id(),
        file_stem
    ));
    let _ = remove_with_backoff(&tmp_path, 8, 50);

    let result = write_output(&tmp_path);

    if let Err(e) = result {
        let _ = remove_with_backoff(&tmp_path, 8, 50);
        return Err(e);
    }

    let copy_result = (|| -> Result<()> {
        let mut f = open_with_backoff(&tmp_path, 16, 50)
            .with_context(|| format!("opening {temp_prefix} tempfile {}", tmp_path.display()))?;
        let stdout = io::stdout();
        let mut w = stdout.lock();
        io::copy(&mut f, &mut w)
            .with_context(|| format!("streaming {temp_prefix} output to stdout"))?;
        w.flush()?;
        Ok(())
    })();

    let _ = remove_with_backoff(&tmp_path, 8, 50);
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
    let entries = read_dir_with_backoff(dir, 16, 50)
        .with_context(|| format!("reading spool dir {}", dir.display()))?;
    let mut parts: Vec<(YearMonth, PathBuf)> = Vec::new();
    for e in entries {
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
    parts.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    let min_ym = parts.iter().map(|(ym, _)| *ym).min().unwrap();
    let max_ym = parts.iter().map(|(ym, _)| *ym).max().unwrap();
    Ok((parts.into_iter().map(|(_, p)| p).collect(), min_ym, max_ym))
}
