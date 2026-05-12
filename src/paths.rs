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
    NoSourceFiles { sources: Sources, statuses: Vec<SourceStatus> },
    DateRangeNoFiles { sources: Sources, requested_start: Option<YearMonth>, requested_end: Option<YearMonth>, available_start: YearMonth, available_end: YearMonth },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceStatus {
    pub kind: FileKind,
    pub dir: PathBuf,
    pub exists: bool,
    pub expected_pattern: &'static str,
    pub matches: usize,
}

impl fmt::Display for PlanningError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlanningError::NoSourceFiles { sources, statuses } => {
                write!(f, "no input files found for source selection {sources:?}: ")?;
                for (i, s) in statuses.iter().enumerate() {
                    if i > 0 { write!(f, "; ")?; }
                    let label = match s.kind { FileKind::Comment => "comments", FileKind::Submission => "submissions" };
                    if s.exists {
                        write!(f, "{label} directory {} exists but contains no files matching {}", s.dir.display(), s.expected_pattern)?;
                    } else {
                        write!(f, "{label} directory {} does not exist (expected files named {})", s.dir.display(), s.expected_pattern)?;
                    }
                }
                Ok(())
            }
            PlanningError::DateRangeNoFiles { sources, requested_start, requested_end, available_start, available_end } => {
                let req_start = requested_start.map(|d| d.to_string()).unwrap_or_else(|| "<corpus-start>".to_string());
                let req_end = requested_end.map(|d| d.to_string()).unwrap_or_else(|| "<corpus-end>".to_string());
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
        let existing_min = *map.keys().next().unwrap();
        let existing_max = *map.keys().next_back().unwrap();

        let (lo, hi) = match (start, end) {
            (Some(s), Some(e)) => (s, e),
            (Some(s), None) => (s, existing_max),
            (None, Some(e)) => (existing_min, e),
            (None, None) => (existing_min, existing_max),
        };

        for ym in iter_year_months(lo, hi) {
            if let Some(p) = map.get(&ym) {
                jobs.push(FileJob { kind, ym, path: p.clone() });
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
        let statuses = selected.into_iter().map(|(kind, map, dir)| SourceStatus {
            kind,
            dir: dir.to_path_buf(),
            exists: dir.exists(),
            expected_pattern: expected_pattern(kind),
            matches: map.len(),
        }).collect();
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
            return Err(PlanningError::DateRangeNoFiles { sources, requested_start: start, requested_end: end, available_start, available_end });
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
        Sources::Submissions => vec![(FileKind::Submission, &discovered.submissions, submissions_dir)],
        Sources::Both => vec![
            (FileKind::Comment, &discovered.comments, comments_dir),
            (FileKind::Submission, &discovered.submissions, submissions_dir),
        ],
    }
}

fn expected_pattern(kind: FileKind) -> &'static str {
    match kind {
        FileKind::Comment => "RC_YYYY-MM.zst",
        FileKind::Submission => "RS_YYYY-MM.zst",
    }
}
