// -----------------------------------------------------------------------------
// Grouped aggregator: ties `MetricNumber`/`NumericSum` (numeric.rs) and
// `format_*` (format.rs) into the actual `Aggregator` impl exposed by
// `retl aggregate --by ...`. Also home to the per-key value extractors and
// JSON-pointer validator.
// -----------------------------------------------------------------------------

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

#[cfg(test)]
mod tests_group {
    use super::*;

    #[test]
    #[should_panic(expected = "refusing to merge incompatible grouped aggregate shards")]
    fn group_metric_merge_rejects_incompatible_metadata() {
        let mut left = GroupMetricAgg::new(GroupBySpec::Subreddit, MetricSpec::default());
        let right = GroupMetricAgg::new(GroupBySpec::Author, MetricSpec::default());
        left.merge(right);
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
