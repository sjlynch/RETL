use super::{Discovered, FileJob, FileKind, PlanningError, SourceStatus};
use crate::config::Sources;
use crate::date::{iter_year_months, YearMonth};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

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

pub(super) fn planned_range_for_map(
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
