use super::plan::planned_range_for_map;
use super::{Discovered, FileKind, MissingMonthDiagnostic, PlanningError};
use crate::config::Sources;
use crate::date::{iter_year_months, YearMonth};
use std::error::Error;
use std::fmt;
use std::path::PathBuf;

impl fmt::Display for PlanningError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlanningError::DiscoveryFailed { kind, dir, error } => {
                let label = match kind {
                    FileKind::Comment => "comments",
                    FileKind::Submission => "submissions",
                };
                write!(
                    f,
                    "failed to discover {label} corpus directory {}: {error}",
                    dir.display()
                )
            }
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
    let mut collect_for = |kind: FileKind, map: &std::collections::BTreeMap<YearMonth, PathBuf>| {
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
