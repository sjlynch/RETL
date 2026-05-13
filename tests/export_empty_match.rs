//! Verify pipeline behavior when a query matches zero records:
//!
//! - `export_partitioned`: pipeline.rs explicitly removes the output file
//!   when `written == 0`, so an unmatched month must NOT leave an empty
//!   `RC_/RS_` artifact at the destination (we'd otherwise pollute the
//!   target corpus with empty files).
//! - `count_by_month`: returns an empty BTreeMap (no panic).
//! - `extract_to_jsonl`: still creates the output file (an empty one) when
//!   files were planned but no records survived filtering.
//! - `extract_to_json`: errors when no input files exist for the date range,
//!   but writes "[]" when files exist and filtering matches no records.

#[path = "common/mod.rs"]
mod common;

use common::*;
use retl::{ConfigBuildError, ExportFormat, PlanningError, RedditETL, Sources, YearMonth};
use std::fs;

#[test]
fn extract_errors_when_selected_source_dirs_are_missing() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();
    let out = base.join("out.jsonl");

    let err = RedditETL::new()
        .base_dir(base)
        .sources(Sources::Both)
        .progress(false)
        .scan()
        .extract_to_jsonl(&out)
        .unwrap_err();

    let planning = err.downcast_ref::<PlanningError>().expect("PlanningError");
    match planning {
        PlanningError::NoSourceFiles { statuses, .. } => {
            assert_eq!(statuses.len(), 2);
            assert!(statuses.iter().all(|s| !s.exists));
            assert!(planning.to_string().contains("does not exist"));
            assert!(planning.to_string().contains("RC_YYYY-MM.zst"));
            assert!(planning.to_string().contains("RS_YYYY-MM.zst"));
        }
        other => panic!("unexpected planning error: {other:?}"),
    }
}

#[test]
fn extract_errors_when_selected_source_dirs_exist_but_have_no_matching_files() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();
    fs::create_dir_all(base.join("comments")).unwrap();
    fs::create_dir_all(base.join("submissions")).unwrap();
    fs::write(base.join("comments").join("not_a_dump.txt"), "x").unwrap();
    let out = base.join("out.jsonl");

    let err = RedditETL::new()
        .base_dir(base)
        .sources(Sources::Both)
        .progress(false)
        .scan()
        .extract_to_jsonl(&out)
        .unwrap_err();

    let planning = err.downcast_ref::<PlanningError>().expect("PlanningError");
    match planning {
        PlanningError::NoSourceFiles { statuses, .. } => {
            assert_eq!(statuses.len(), 2);
            assert!(statuses.iter().all(|s| s.exists));
            assert!(planning.to_string().contains("exists but contains no files matching"));
            assert!(planning.to_string().contains("RC_YYYY-MM.zst"));
            assert!(planning.to_string().contains("RS_YYYY-MM.zst"));
        }
        other => panic!("unexpected planning error: {other:?}"),
    }
}

#[test]
fn extract_errors_when_date_range_excludes_available_corpus() {
    let base = make_corpus_basic();
    let out = base.join("out.jsonl");

    let err = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(1999, 1)), Some(YearMonth::new(1999, 1)))
        .progress(false)
        .scan()
        .extract_to_jsonl(&out)
        .unwrap_err();

    let planning = err.downcast_ref::<PlanningError>().expect("PlanningError");
    match planning {
        PlanningError::DateRangeNoFiles { requested_start, requested_end, available_start, available_end, .. } => {
            assert_eq!(*requested_start, Some(YearMonth::new(1999, 1)));
            assert_eq!(*requested_end, Some(YearMonth::new(1999, 1)));
            assert_eq!(*available_start, YearMonth::new(2006, 1));
            assert_eq!(*available_end, YearMonth::new(2006, 1));
            assert!(planning.to_string().contains("1999-01..=1999-01"));
            assert!(planning.to_string().contains("2006-01..=2006-01"));
        }
        other => panic!("unexpected planning error: {other:?}"),
    }
}

#[test]
fn invalid_date_range_surfaces_structured_build_error() {
    let base = make_corpus_basic();
    let out = base.join("out.jsonl");

    let err = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 2)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .extract_to_jsonl(&out)
        .unwrap_err();

    let build = err.downcast_ref::<ConfigBuildError>().expect("ConfigBuildError");
    assert_eq!(build, &ConfigBuildError::InvalidDateRange { start: YearMonth::new(2006, 2), end: YearMonth::new(2006, 1) });
}

#[test]
fn export_partitioned_jsonl_unmatched_query_removes_output_files() {
    let base = make_corpus_basic();
    let out_dir = base.join("export_empty_jsonl");

    // Filter for an author that does not exist in the corpus.
    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .authors_in(["nonexistent_user_abcxyz"])
        .export_partitioned(&out_dir, ExportFormat::Jsonl)
        .unwrap();

    // The pipeline removes any zero-record output files. Comments and submissions
    // dirs may exist (created up front), but neither RC nor RS file should be present.
    let rc = out_dir.join("comments").join("RC_2006-01.jsonl");
    let rs = out_dir.join("submissions").join("RS_2006-01.jsonl");
    assert!(!rc.exists(), "empty-match RC should have been removed: {}", rc.display());
    assert!(!rs.exists(), "empty-match RS should have been removed: {}", rs.display());
}

#[test]
fn export_partitioned_zst_unmatched_query_removes_output_files() {
    let base = make_corpus_basic();
    let out_dir = base.join("export_empty_zst");

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .authors_in(["nonexistent_user_abcxyz"])
        .export_partitioned(&out_dir, ExportFormat::Zst)
        .unwrap();

    let rc = out_dir.join("comments").join("RC_2006-01.zst");
    let rs = out_dir.join("submissions").join("RS_2006-01.zst");
    assert!(!rc.exists(), "empty-match RC should have been removed: {}", rc.display());
    assert!(!rs.exists(), "empty-match RS should have been removed: {}", rs.display());

    // Staging dir should also be free of leftovers.
    let staging = out_dir.join("_staging");
    if staging.exists() {
        let leftovers: Vec<_> = std::fs::read_dir(&staging)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".inprogress"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "no .inprogress files should remain after empty-match export, found: {:?}",
            leftovers.iter().map(|e| e.path()).collect::<Vec<_>>()
        );
    }
}

#[test]
fn count_by_month_returns_empty_map_for_unmatched_query() {
    let base = make_corpus_basic();
    let counts = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .authors_in(["nonexistent_user_abcxyz"])
        .count_by_month()
        .unwrap();
    assert!(
        counts.is_empty(),
        "no matches should produce empty BTreeMap, got: {:?}",
        counts
    );
}

#[test]
fn extract_to_json_array_with_unmatched_query_writes_empty_array() {
    let base = make_corpus_basic();
    let out = base.join("unmatched.json");

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .authors_in(["nonexistent_user_abcxyz"])
        .extract_to_json(&out, false)
        .unwrap();

    let s = fs::read_to_string(&out).unwrap();
    let arr: Vec<serde_json::Value> = serde_json::from_str(&s).unwrap();
    assert!(arr.is_empty(), "unmatched-query JSON must parse as []");
}

#[test]
fn count_by_month_aggregates_across_multiple_months() {
    // Build a corpus with 3 months. Each month has 2 RC + 2 RS records.
    // With subreddit=programming and pseudo-users excluded by default (they
    // aren't in the multi-month builder anyway), each month sees 4 records.
    let months: Vec<YearMonth> = vec![
        YearMonth::new(2006, 1),
        YearMonth::new(2006, 2),
        YearMonth::new(2006, 3),
    ];
    let base = make_corpus_multi_month(&months);

    let counts = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 3)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .count_by_month()
        .unwrap();

    assert_eq!(counts.len(), 3, "should see one entry per month");
    for ym in &months {
        assert_eq!(
            counts.get(ym).copied(),
            Some(4),
            "month {} should report 4 records (2 RC + 2 RS)",
            ym
        );
    }
}
