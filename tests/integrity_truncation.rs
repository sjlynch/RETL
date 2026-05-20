//! T11: late/trailing zst corruption (truncated tail).
//!
//! These tests exercise the documented split between integrity modes, strict
//! default scans, and explicit `allow_partial` lossy scans:
//!
//! - `IntegrityMode::Full` decodes the whole stream, so it MUST report a tail
//!   truncation. (After #T5 lands `include_checksum` verification this also
//!   covers single-byte payload tampering, but a tail truncation already
//!   reaches EOF before the frame's natural end so Full catches it today.)
//! - `IntegrityMode::Quick { sample_bytes: 4096 }` only reads the first 4096
//!   decompressed bytes; with a healthy prefix, Quick MUST MISS the corruption.
//!   This test locks in that documented limitation so we don't accidentally
//!   "fix" Quick to do a Full scan and silently regress its perf contract.
//! - A normal scan over a corrupt month is strict by default and must fail
//!   instead of returning a plausible partial result. `allow_partial(true)`
//!   preserves the old skip behavior while surfacing skipped paths through a
//!   machine-readable reporter.

#[path = "common/mod.rs"]
mod common;

use common::*;
use retl::{IntegrityMode, RedditETL, Sources, YearMonth};

/// Number of records in the truncated fixture. Large enough that 4096 bytes of
/// decompressed prefix lands well before the truncation, so Quick definitely
/// passes and Full definitely fails.
const FIXTURE_RECORDS: usize = 500;

/// Bytes lopped off the END of the compressed file.
const TRUNCATE_BYTES: u64 = 256;

#[test]
fn full_integrity_detects_tail_truncation() {
    let base = make_corpus_basic();
    let path = base.join("comments").join("RC_2006-03.zst");
    make_truncated_zst(&path, FIXTURE_RECORDS, TRUNCATE_BYTES);

    let bad = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 3)), Some(YearMonth::new(2006, 3)))
        .progress(false)
        .check_corpus_integrity(IntegrityMode::Full)
        .unwrap();

    assert_eq!(
        bad.failure_count(),
        1,
        "Full integrity must report the truncated month: {:?}",
        bad
    );
    assert!(
        bad.failures[0].0.ends_with("RC_2006-03.zst"),
        "reported path should be the truncated file, got {:?}",
        bad.failures[0].0
    );
}

#[test]
fn quick_integrity_misses_tail_truncation() {
    let base = make_corpus_basic();
    let path = base.join("comments").join("RC_2006-03.zst");
    make_truncated_zst(&path, FIXTURE_RECORDS, TRUNCATE_BYTES);

    let bad = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 3)), Some(YearMonth::new(2006, 3)))
        .progress(false)
        .check_corpus_integrity(IntegrityMode::Quick { sample_bytes: 4096 })
        .unwrap();

    // This locks in the *documented limitation*: Quick samples a prefix and
    // therefore cannot detect trailing corruption.
    assert!(
        bad.is_ok(),
        "Quick integrity is documented to miss trailing corruption when the \
         decompressed prefix is healthy; got reports: {:?}",
        bad
    );
}

#[test]
fn normal_scan_errors_on_truncated_month_by_default() {
    let base = make_corpus_basic();
    let path = base.join("comments").join("RC_2006-03.zst");
    make_truncated_zst(&path, FIXTURE_RECORDS, TRUNCATE_BYTES);

    let err = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 3)), Some(YearMonth::new(2006, 3)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .include_pseudo_users()
        .count_by_month()
        .expect_err("strict scans must fail truncated zstd months by default");

    let msg = format!("{err:#}");
    assert!(msg.contains("zstd decode error"), "unexpected error: {msg}");
    assert!(
        msg.contains(&path.display().to_string()),
        "unexpected error: {msg}"
    );
}

#[test]
fn allow_partial_scan_skips_truncated_month_and_reports_path() {
    let base = make_corpus_basic();
    let path = base.join("comments").join("RC_2006-03.zst");
    make_truncated_zst(&path, FIXTURE_RECORDS, TRUNCATE_BYTES);

    let etl = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 3)), Some(YearMonth::new(2006, 3)))
        .progress(false)
        .allow_partial(true);
    let reporter = etl.partial_read_reporter();

    let counts = etl
        .scan()
        .subreddit("programming")
        .include_pseudo_users()
        .count_by_month()
        .expect("allow_partial preserves explicit lossy skip behavior");

    let partial = counts.get(&YearMonth::new(2006, 3)).copied().unwrap_or(0);
    assert!(
        partial < FIXTURE_RECORDS as u64,
        "expected a partial count from the truncated month, got {} (full count would be {})",
        partial,
        FIXTURE_RECORDS
    );

    let report = reporter.snapshot();
    assert_eq!(report.skipped_file_count, 1, "{report:?}");
    assert_eq!(report.skipped_files[0].path, path);
    assert!(
        report.skipped_files[0].error.contains("zstd decode error"),
        "{report:?}"
    );
}
