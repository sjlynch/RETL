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
