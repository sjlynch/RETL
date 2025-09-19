use crate::config::Sources;
use crate::date::{iter_year_months, YearMonth};
use regex::Regex;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

/// Type of monthly file.
#[derive(Clone, Copy, Debug)]
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
