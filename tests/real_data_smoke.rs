//! Lightweight real-data smoke test against the corpus at `F:\reddit`.
//!
//! Skipped automatically when the corpus is not present. Where present, this
//! test deliberately bounds memory by using `IntegrityMode::Quick` with a
//! small sample budget over **exactly one month** of real data — never a
//! full-file decode, never multiple files, never a full pipeline that loads
//! lots of records into memory.
//!
//! Purpose: catch regressions where the production zstd file format,
//! filename layout, or path discovery silently breaks against a real file
//! that synthetic fixtures don't reproduce.

use retl::{IntegrityMode, RedditETL, Sources, YearMonth};
use std::path::Path;

const REAL_BASE: &str = r"F:\reddit";

/// Find a `(year, month)` for which a comment file exists in the corpus,
/// preferring the earliest available month so we sample data the test author
/// expects to be small (relatively) and stable.
fn find_one_real_month() -> Option<YearMonth> {
    let comments_dir = Path::new(REAL_BASE).join("comments");
    if !comments_dir.exists() {
        return None;
    }
    let mut found: Vec<YearMonth> = Vec::new();
    for entry in std::fs::read_dir(&comments_dir).ok()? {
        let Ok(ent) = entry else { continue };
        let name = ent.file_name().to_string_lossy().to_string();
        // RC_YYYY-MM.zst
        if let Some(rest) = name.strip_prefix("RC_") {
            if let Some(stem) = rest.strip_suffix(".zst") {
                let parts: Vec<&str> = stem.split('-').collect();
                if parts.len() == 2 {
                    if let (Ok(y), Ok(m)) = (parts[0].parse::<u16>(), parts[1].parse::<u8>()) {
                        if (1..=12).contains(&m) {
                            found.push(YearMonth::new(y, m));
                        }
                    }
                }
            }
        }
    }
    found.sort();
    found.into_iter().next()
}

#[test]
fn real_corpus_quick_integrity_check_passes_for_one_month() {
    let Some(ym) = find_one_real_month() else {
        eprintln!("real_data_smoke: skipping — no real corpus at {}", REAL_BASE);
        return;
    };

    // Quick mode with a tiny sample budget keeps decompressed-bytes bounded
    // so we don't blow up the modest RAM available on this machine.
    let errors = RedditETL::new()
        .base_dir(REAL_BASE)
        .sources(Sources::Comments)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .check_corpus_integrity(IntegrityMode::Quick { sample_bytes: 64 * 1024 })
        .expect("integrity check returned a hard error");

    assert!(
        errors.is_empty(),
        "quick integrity check on real {} reported {} bad files; first: {:?}",
        ym,
        errors.len(),
        errors.first()
    );
}

#[test]
fn real_corpus_path_planner_discovers_the_one_month() {
    use retl::{discover_all, plan_files};
    let Some(ym) = find_one_real_month() else {
        eprintln!("real_data_smoke: skipping — no real corpus at {}", REAL_BASE);
        return;
    };

    let comments_dir = Path::new(REAL_BASE).join("comments");
    let submissions_dir = Path::new(REAL_BASE).join("submissions");
    let discovered = discover_all(&comments_dir, &submissions_dir);

    let jobs = plan_files(&discovered, Sources::Comments, Some(ym), Some(ym));
    assert_eq!(jobs.len(), 1, "exactly one RC job for {}", ym);
    assert_eq!(jobs[0].ym, ym);
    assert!(
        jobs[0].path.exists(),
        "planned path must exist on disk: {}",
        jobs[0].path.display()
    );
}
