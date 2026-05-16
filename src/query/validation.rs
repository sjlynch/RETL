use crate::date::YearMonth;
use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind};
use anyhow::Context;
use regex::Regex;
use serde_json::Value;
use std::borrow::Cow;
use std::fmt;
use std::io::BufReader;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use time::OffsetDateTime;

/// Structured error returned when a scan/query builder contains contradictory
/// or invalid filter settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryBuildError {
    message: String,
}

impl QueryBuildError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for QueryBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for QueryBuildError {}

/// Source kind encoded by Reddit fullname prefixes accepted by record-ID
/// filters. `t1_` selects comments and `t3_` selects submissions; unprefixed
/// IDs remain source-agnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordIdKind {
    Comment,
    Submission,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NormalizedRecordId {
    pub(crate) bare: String,
    pub(crate) kind: Option<RecordIdKind>,
}
