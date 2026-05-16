use super::{Discovered, FileKind, PlanningError};
use crate::config::Sources;
use crate::date::YearMonth;
use regex::Regex;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use walkdir::WalkDir;

fn discover_month_map_checked(
    dir: &Path,
    kind: FileKind,
) -> Result<BTreeMap<YearMonth, PathBuf>, PlanningError> {
    let re = match kind {
        FileKind::Comment => Regex::new(r"^RC_(\d{4})-(\d{2})\.zst$").unwrap(),
        FileKind::Submission => Regex::new(r"^RS_(\d{4})-(\d{2})\.zst$").unwrap(),
    };
    let mut map = BTreeMap::new();
    if !dir.exists() {
        return Ok(map);
    }
    if !dir.is_dir() {
        return Err(PlanningError::DiscoveryFailed {
            kind,
            dir: dir.to_path_buf(),
            error: "path exists but is not a directory".to_string(),
        });
    }
    for entry in WalkDir::new(dir).min_depth(1).max_depth(1) {
        let ent = entry.map_err(|e| PlanningError::DiscoveryFailed {
            kind,
            dir: dir.to_path_buf(),
            error: e.to_string(),
        })?;
        if let Some(name) = ent.file_name().to_str() {
            if let Some(caps) = re.captures(name) {
                let ym_raw = format!("{}-{}", &caps[1], &caps[2]);
                match YearMonth::from_str(&ym_raw) {
                    Ok(ym) => {
                        map.insert(ym, ent.path().to_path_buf());
                    }
                    Err(error) => {
                        tracing::warn!(
                            path = %ent.path().display(),
                            source = kind.long_label(),
                            error = %error,
                            "skipping invalid Reddit monthly filename; month must be 01..12"
                        );
                    }
                }
            }
        }
    }
    Ok(map)
}

fn discover_month_map(dir: &Path, kind: FileKind) -> BTreeMap<YearMonth, PathBuf> {
    match discover_month_map_checked(dir, kind) {
        Ok(map) => map,
        Err(e) => {
            tracing::warn!(error = %e, "corpus discovery failed; returning empty map from compatibility wrapper");
            BTreeMap::new()
        }
    }
}

pub fn discover_all(comments_dir: &Path, submissions_dir: &Path) -> Discovered {
    Discovered {
        comments: discover_month_map(comments_dir, FileKind::Comment),
        submissions: discover_month_map(submissions_dir, FileKind::Submission),
    }
}

pub fn discover_all_checked(
    comments_dir: &Path,
    submissions_dir: &Path,
) -> Result<Discovered, PlanningError> {
    discover_sources_checked(comments_dir, submissions_dir, Sources::Both)
}

pub fn discover_sources_checked(
    comments_dir: &Path,
    submissions_dir: &Path,
    sources: Sources,
) -> Result<Discovered, PlanningError> {
    let comments = match sources {
        Sources::Comments | Sources::Both => {
            discover_month_map_checked(comments_dir, FileKind::Comment)?
        }
        Sources::Submissions => BTreeMap::new(),
    };
    let submissions = match sources {
        Sources::Submissions | Sources::Both => {
            discover_month_map_checked(submissions_dir, FileKind::Submission)?
        }
        Sources::Comments => BTreeMap::new(),
    };
    Ok(Discovered {
        comments,
        submissions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn checked_discovery_errors_when_source_path_is_not_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let comments = tmp.path().join("comments");
        let submissions = tmp.path().join("submissions");
        fs::write(&comments, "not a directory").unwrap();
        fs::create_dir(&submissions).unwrap();

        let err = discover_all_checked(&comments, &submissions).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains(&comments.display().to_string()), "{msg}");
        assert!(msg.contains("not a directory"), "{msg}");
    }

    #[test]
    fn invalid_month_filenames_are_warned_and_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        let comments = tmp.path().join("comments");
        let submissions = tmp.path().join("submissions");
        fs::create_dir(&comments).unwrap();
        fs::create_dir(&submissions).unwrap();
        fs::write(comments.join("RC_2024-00.zst"), b"").unwrap();
        fs::write(comments.join("RC_2024-01.zst"), b"").unwrap();
        fs::write(submissions.join("RS_2024-99.zst"), b"").unwrap();

        let discovered = discover_all_checked(&comments, &submissions).unwrap();
        assert!(discovered.comments.contains_key(&YearMonth::new(2024, 1)));
        assert!(!discovered.comments.contains_key(&YearMonth {
            year: 2024,
            month: 0
        }));
        assert!(discovered.submissions.is_empty());
    }
}
