#[path = "common/mod.rs"]
mod common;

use common::*;
use retl::{IntegrityMode, RedditETL, Sources, YearMonth};

/// Demonstrates integrity checks over a deliberately broken monthly file:
/// - We add `RC_2006-02.zst` with invalid (non-zstd) contents.
/// - `check_corpus_integrity(Quick)` and `(Full)` should report one bad file.
/// Outcome: both modes detect the corruption and return it in the error list.
#[test]
fn integrity_check_detects_corrupt_month() {
    let base = make_corpus_basic();
    add_corrupt_month(&base);

    // Limit the scan to the corrupt month to keep the test focused/fast
    let bad_quick = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 2)), Some(YearMonth::new(2006, 2)))
        .progress(false)
        .check_corpus_integrity(IntegrityMode::Quick {
            sample_bytes: 64 * 1024,
        })
        .unwrap();

    assert_eq!(
        bad_quick.len(),
        1,
        "quick integrity should flag the corrupt file"
    );

    let bad_full = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 2)), Some(YearMonth::new(2006, 2)))
        .progress(false)
        .check_corpus_integrity(IntegrityMode::Full)
        .unwrap();

    assert_eq!(
        bad_full.len(),
        1,
        "full integrity should also flag the corrupt file"
    );
}

#[test]
fn integrity_errors_when_no_source_files_match() {
    let tmp = tempfile::tempdir().unwrap();
    let err = RedditETL::new()
        .base_dir(tmp.path().join("missing"))
        .sources(Sources::Comments)
        .progress(false)
        .check_corpus_integrity(IntegrityMode::Full)
        .unwrap_err();

    assert!(
        err.to_string().contains("no input files found"),
        "unexpected error: {err}"
    );
}

#[test]
fn integrity_errors_when_date_range_matches_zero_files() {
    let base = make_corpus_basic();
    let err = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(1990, 1)), Some(YearMonth::new(1990, 2)))
        .progress(false)
        .check_corpus_integrity(IntegrityMode::Full)
        .unwrap_err();

    assert!(
        err.to_string().contains("matched no files"),
        "unexpected error: {err}"
    );
}
