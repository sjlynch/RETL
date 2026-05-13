use crate::config::Sources;
use crate::date::{iter_year_months, YearMonth};
use regex::Regex;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

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

fn discover_month_map(dir: &Path, kind: FileKind) -> BTreeMap<YearMonth, PathBuf> {
    let re = match kind {
        FileKind::Comment => Regex::new(r"^RC_(\d{4})-(\d{2})\.zst$").unwrap(),
        FileKind::Submission => Regex::new(r"^RS_(\d{4})-(\d{2})\.zst$").unwrap(),
    };
    let mut map = BTreeMap::new();
    if !dir.exists() {
        return map;
    }
    for entry in WalkDir::new(dir).min_depth(1).max_depth(1) {
        if let Ok(ent) = entry {
            if let Some(name) = ent.file_name().to_str() {
                if let Some(caps) = re.captures(name) {
                    let year: u16 = caps[1].parse().unwrap();
                    let month: u8 = caps[2].parse().unwrap();
                    let ym = YearMonth { year, month };
                    map.insert(ym, ent.path().to_path_buf());
                }
            }
        }
    }
    map
}

/// Returns the discovered earliest and latest for each source. If a user requests dates outside,
/// we clamp to these by simply skipping non-existing months.
pub struct Discovered {
    pub comments: BTreeMap<YearMonth, PathBuf>,
    pub submissions: BTreeMap<YearMonth, PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlanningError {
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

impl fmt::Display for PlanningError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlanningError::NoSourceFiles { sources, statuses } => {
                write!(f, "no input files found for source selection {sources:?}: ")?;
                for (i, s) in statuses.iter().enumerate() {
                    if i > 0 {
                        write!(f, "; ")?;
                    }
                    let label = match s.kind {
                        FileKind::Comment => "comments",
                        FileKind::Submission => "submissions",
                    };
                    if s.exists {
                        write!(
                            f,
                            "{label} directory {} exists but contains no files matching {}",
                            s.dir.display(),
                            s.expected_pattern
                        )?;
                    } else {
                        write!(
                            f,
                            "{label} directory {} does not exist (expected files named {})",
                            s.dir.display(),
                            s.expected_pattern
                        )?;
                    }
                }
                Ok(())
            }
            PlanningError::DateRangeNoFiles {
                sources,
                requested_start,
                requested_end,
                available_start,
                available_end,
            } => {
                let req_start = requested_start
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "<corpus-start>".to_string());
                let req_end = requested_end
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "<corpus-end>".to_string());
                write!(f, "requested date range {req_start}..={req_end} matched no files for source selection {sources:?}; corpus available range is {available_start}..={available_end}")
            }
        }
    }
}

impl Error for PlanningError {}

pub fn discover_all(comments_dir: &Path, submissions_dir: &Path) -> Discovered {
    Discovered {
        comments: discover_month_map(comments_dir, FileKind::Comment),
        submissions: discover_month_map(submissions_dir, FileKind::Submission),
    }
}

pub fn plan_files(
    discovered: &Discovered,
    sources: Sources,
    start: Option<YearMonth>,
    end: Option<YearMonth>,
) -> Vec<FileJob> {
    let mut jobs = Vec::new();

    let mut push_jobs = |kind: FileKind, map: &BTreeMap<YearMonth, PathBuf>| {
        if map.is_empty() {
            return;
        }
        let (lo, hi) = planned_range_for_map(map, start, end);

        for ym in iter_year_months(lo, hi) {
            if let Some(p) = map.get(&ym) {
                jobs.push(FileJob {
                    kind,
                    ym,
                    path: p.clone(),
                });
            }
            // silently skip months that don't exist (auto-handle e.g. 2004 requests)
        }
    };

    match sources {
        Sources::Comments => push_jobs(FileKind::Comment, &discovered.comments),
        Sources::Submissions => push_jobs(FileKind::Submission, &discovered.submissions),
        Sources::Both => {
            push_jobs(FileKind::Comment, &discovered.comments);
            push_jobs(FileKind::Submission, &discovered.submissions);
        }
    }

    jobs
}

/// Return missing monthly files per selected source inside the selected range.
///
/// The range is clamped to each source's discovered min/max so requests outside
/// the corpus do not produce noisy holes. With no explicit start/end, the full
/// discovered range for each source is checked; callers that only want
/// warnings for explicit user ranges should use [`log_missing_month_warnings`].
pub fn missing_month_diagnostics(
    discovered: &Discovered,
    sources: Sources,
    start: Option<YearMonth>,
    end: Option<YearMonth>,
) -> Vec<MissingMonthDiagnostic> {
    let mut diagnostics = Vec::new();
    let mut collect_for = |kind: FileKind, map: &BTreeMap<YearMonth, PathBuf>| {
        if map.is_empty() {
            return;
        }
        let existing_min = *map.keys().next().unwrap();
        let existing_max = *map.keys().next_back().unwrap();
        let (requested_lo, requested_hi) = planned_range_for_map(map, start, end);

        let range_start = requested_lo.max(existing_min);
        let range_end = requested_hi.min(existing_max);
        if range_start > range_end {
            return;
        }

        let months: Vec<YearMonth> = iter_year_months(range_start, range_end)
            .filter(|ym| !map.contains_key(ym))
            .collect();
        if !months.is_empty() {
            diagnostics.push(MissingMonthDiagnostic {
                kind,
                range_start,
                range_end,
                months,
            });
        }
    };

    match sources {
        Sources::Comments => collect_for(FileKind::Comment, &discovered.comments),
        Sources::Submissions => collect_for(FileKind::Submission, &discovered.submissions),
        Sources::Both => {
            collect_for(FileKind::Comment, &discovered.comments);
            collect_for(FileKind::Submission, &discovered.submissions);
        }
    }

    diagnostics
}

pub fn format_year_month_ranges(months: &[YearMonth]) -> String {
    if months.is_empty() {
        return "-".to_string();
    }

    let mut months = months.to_vec();
    months.sort();
    months.dedup();

    let mut out = Vec::new();
    let mut start = months[0];
    let mut end = months[0];
    for ym in months.into_iter().skip(1) {
        if end.next() == Some(ym) {
            end = ym;
        } else {
            push_range(&mut out, start, end);
            start = ym;
            end = ym;
        }
    }
    push_range(&mut out, start, end);
    out.join(",")
}

pub fn log_missing_month_warnings(
    discovered: &Discovered,
    sources: Sources,
    start: Option<YearMonth>,
    end: Option<YearMonth>,
) {
    // Avoid noisy default scans over an as-is corpus; warnings are for explicit
    // user-requested ranges. `retl describe` still surfaces full-corpus holes.
    if start.is_none() && end.is_none() {
        return;
    }

    for diag in missing_month_diagnostics(discovered, sources, start, end) {
        let missing = format_year_month_ranges(&diag.months);
        tracing::warn!(
            source = diag.kind.long_label(),
            range_start = %diag.range_start,
            range_end = %diag.range_end,
            missing_month_count = diag.months.len(),
            missing_months = %missing,
            "requested date range has missing month files; results may be incomplete"
        );
    }
}

fn push_range(out: &mut Vec<String>, start: YearMonth, end: YearMonth) {
    if start == end {
        out.push(start.to_string());
    } else {
        out.push(format!("{start}..={end}"));
    }
}

fn planned_range_for_map(
    map: &BTreeMap<YearMonth, PathBuf>,
    start: Option<YearMonth>,
    end: Option<YearMonth>,
) -> (YearMonth, YearMonth) {
    let existing_min = *map.keys().next().unwrap();
    let existing_max = *map.keys().next_back().unwrap();
    match (start, end) {
        (Some(s), Some(e)) => (s, e),
        (Some(s), None) => (s, existing_max),
        (None, Some(e)) => (existing_min, e),
        (None, None) => (existing_min, existing_max),
    }
}

pub fn plan_files_checked(
    discovered: &Discovered,
    comments_dir: &Path,
    submissions_dir: &Path,
    sources: Sources,
    start: Option<YearMonth>,
    end: Option<YearMonth>,
) -> Result<Vec<FileJob>, PlanningError> {
    let selected = selected_sources(discovered, comments_dir, submissions_dir, sources);
    if !selected.iter().any(|(_, map, _)| !map.is_empty()) {
        let statuses = selected
            .into_iter()
            .map(|(kind, map, dir)| SourceStatus {
                kind,
                dir: dir.to_path_buf(),
                exists: dir.exists(),
                expected_pattern: expected_pattern(kind),
                matches: map.len(),
            })
            .collect();
        return Err(PlanningError::NoSourceFiles { sources, statuses });
    }

    let jobs = plan_files(discovered, sources, start, end);
    if jobs.is_empty() {
        let mut available_start = None::<YearMonth>;
        let mut available_end = None::<YearMonth>;
        for (_, map, _) in selected_sources(discovered, comments_dir, submissions_dir, sources) {
            if let Some(first) = map.keys().next().copied() {
                available_start = Some(available_start.map_or(first, |cur| cur.min(first)));
            }
            if let Some(last) = map.keys().next_back().copied() {
                available_end = Some(available_end.map_or(last, |cur| cur.max(last)));
            }
        }
        if let (Some(available_start), Some(available_end)) = (available_start, available_end) {
            return Err(PlanningError::DateRangeNoFiles {
                sources,
                requested_start: start,
                requested_end: end,
                available_start,
                available_end,
            });
        }
    }

    Ok(jobs)
}

fn selected_sources<'a>(
    discovered: &'a Discovered,
    comments_dir: &'a Path,
    submissions_dir: &'a Path,
    sources: Sources,
) -> Vec<(FileKind, &'a BTreeMap<YearMonth, PathBuf>, &'a Path)> {
    match sources {
        Sources::Comments => vec![(FileKind::Comment, &discovered.comments, comments_dir)],
        Sources::Submissions => vec![(
            FileKind::Submission,
            &discovered.submissions,
            submissions_dir,
        )],
        Sources::Both => vec![
            (FileKind::Comment, &discovered.comments, comments_dir),
            (
                FileKind::Submission,
                &discovered.submissions,
                submissions_dir,
            ),
        ],
    }
}

fn expected_pattern(kind: FileKind) -> &'static str {
    match kind {
        FileKind::Comment => "RC_YYYY-MM.zst",
        FileKind::Submission => "RS_YYYY-MM.zst",
    }
}
