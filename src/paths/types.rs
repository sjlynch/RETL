use crate::config::Sources;
use crate::date::YearMonth;
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Type of monthly file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FileKind {
    Comment,    // RC_YYYY-MM.zst
    Submission, // RS_YYYY-MM.zst
}

impl FileKind {
    pub fn long_label(self) -> &'static str {
        match self {
            FileKind::Comment => "comments",
            FileKind::Submission => "submissions",
        }
    }
}

#[derive(Clone, Debug)]
pub struct FileJob {
    pub kind: FileKind,
    pub ym: YearMonth,
    pub path: PathBuf,
}

/// Returns the discovered earliest and latest for each source. If a user requests dates outside,
/// we clamp to these by simply skipping non-existing months.
#[derive(Clone, Debug)]
pub struct Discovered {
    pub comments: BTreeMap<YearMonth, PathBuf>,
    pub submissions: BTreeMap<YearMonth, PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlanningError {
    DiscoveryFailed {
        kind: FileKind,
        dir: PathBuf,
        error: String,
    },
    NoSourceFiles {
        sources: Sources,
        statuses: Vec<SourceStatus>,
    },
    DateRangeNoFiles {
        sources: Sources,
        requested_start: Option<YearMonth>,
        requested_end: Option<YearMonth>,
        available_start: YearMonth,
        available_end: YearMonth,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceStatus {
    pub kind: FileKind,
    pub dir: PathBuf,
    pub exists: bool,
    pub expected_pattern: &'static str,
    pub matches: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MissingMonthDiagnostic {
    pub kind: FileKind,
    /// The clamped portion of the requested range that was checked for holes.
    /// Months outside the discovered corpus min/max are intentionally excluded
    /// from diagnostics to avoid noisy warnings for pre/post-corpus requests.
    pub range_start: YearMonth,
    pub range_end: YearMonth,
    pub months: Vec<YearMonth>,
}
