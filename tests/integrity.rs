#[path = "common/mod.rs"]
mod common;

use assert_cmd::Command;
use common::*;
use predicates::prelude::*;
use predicates::str::contains;
use retl::{quick_validate_zst, IntegrityMode, QuickOutcome, RedditETL, Sources, YearMonth};

fn retl_cmd() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
}

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
        bad_quick.failure_count(),
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
        bad_full.failure_count(),
        1,
        "full integrity should also flag the corrupt file"
    );
}

/// A file whose *decompressed* size is smaller than the quick-mode sample
/// budget is decoded all the way to EOF — including its trailing checksum —
/// so quick mode on it is equivalent to a full check. `quick_validate_zst`
/// must report that distinctly as `FullyDecoded`, not as a prefix sample.
#[test]
fn quick_validate_reports_fully_decoded_for_sub_sample_size_file() {
    let base = make_corpus_basic();
    // The basic corpus file holds three tiny comment records — well under a
    // mebibyte of decompressed JSON.
    let path = base.join("comments").join("RC_2006-01.zst");

    let outcome = quick_validate_zst(&path, 1024 * 1024).unwrap();

    assert_eq!(
        outcome,
        QuickOutcome::FullyDecoded,
        "a file smaller than the sample budget must be reported as fully decoded"
    );
}

/// The converse: when the sample budget is exhausted before EOF, the same
/// file is reported as `PrefixOnly` so a caller knows trailing corruption
/// past the sampled prefix was not checked.
#[test]
fn quick_validate_reports_prefix_only_when_budget_exhausted() {
    let base = make_corpus_basic();
    let path = base.join("comments").join("RC_2006-01.zst");

    // 8 decompressed bytes is far less than the file's JSON payload.
    let outcome = quick_validate_zst(&path, 8).unwrap();

    assert_eq!(
        outcome,
        QuickOutcome::PrefixOnly,
        "a tiny sample budget must be reported as a prefix-only check"
    );
}

#[test]
fn quick_validate_rejects_zero_sample_bytes() {
    let base = make_corpus_basic();
    let path = base.join("comments").join("RC_2006-01.zst");

    let err = quick_validate_zst(&path, 0).unwrap_err();

    assert!(
        err.to_string().contains("--sample-bytes must be > 0"),
        "unexpected error: {err}"
    );
}

#[test]
fn integrity_mode_quick_rejects_zero_sample_bytes() {
    let base = make_corpus_basic();

    let err = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .progress(false)
        .check_corpus_integrity(IntegrityMode::Quick { sample_bytes: 0 })
        .unwrap_err();

    assert!(
        err.to_string().contains("--sample-bytes must be > 0"),
        "unexpected error: {err}"
    );
}

#[test]
fn cli_integrity_rejects_zero_sample_bytes_before_success() {
    retl_cmd()
        .args(["integrity", "--mode", "quick", "--sample-bytes", "0"])
        .assert()
        .failure()
        .stderr(
            contains("--sample-bytes must be > 0")
                .and(contains("OK: no corruption detected.").not()),
        );
}

#[test]
fn cli_integrity_warns_for_tiny_sample_bytes() {
    let base = make_corpus_basic();

    retl_cmd()
        .args([
            "integrity",
            "--data-dir",
            base.to_str().unwrap(),
            "--source",
            "rc",
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--no-progress",
            "--mode",
            "quick",
            "--sample-bytes",
            "1",
        ])
        .assert()
        .success()
        .stderr(
            contains("quick integrity mode validates only a decompressed prefix sample")
                .and(contains("OK: no corruption detected.")),
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
