//! T11: late/trailing zst corruption (truncated tail).
//!
//! These tests exercise the documented split between integrity modes and the
//! "warn-and-skip" behaviour of normal scans:
//!
//! - `IntegrityMode::Full` decodes the whole stream, so it MUST report a tail
//!   truncation. (After #T5 lands `include_checksum` verification this also
//!   covers single-byte payload tampering, but a tail truncation already
//!   reaches EOF before the frame's natural end so Full catches it today.)
//! - `IntegrityMode::Quick { sample_bytes: 4096 }` only reads the first 4096
//!   decompressed bytes; with a healthy prefix, Quick MUST MISS the corruption.
//!   This test locks in that documented limitation so we don't accidentally
//!   "fix" Quick to do a Full scan and silently regress its perf contract.
//! - A normal scan over a corrupt month must NOT crash. The streaming reader
//!   is expected to log a warning and skip the file, returning a (partial)
//!   result rather than aborting the run.

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
        bad.len(),
        1,
        "Full integrity must report the truncated month: {:?}",
        bad
    );
    assert!(
        bad[0].0.ends_with("RC_2006-03.zst"),
        "reported path should be the truncated file, got {:?}",
        bad[0].0
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
        bad.is_empty(),
        "Quick integrity is documented to miss trailing corruption when the \
         decompressed prefix is healthy; got reports: {:?}",
        bad
    );
}

#[test]
fn normal_scan_warns_and_skips_truncated_month_without_crashing() {
    let base = make_corpus_basic();
    let path = base.join("comments").join("RC_2006-03.zst");
    make_truncated_zst(&path, FIXTURE_RECORDS, TRUNCATE_BYTES);

    // A normal scan that touches the corrupt month must NOT crash. The
    // streaming reader is expected to log a warning and skip the file. The
    // count we get back may be partial (records read before the truncation
    // boundary) — what matters is that `count_by_month` returns Ok.
    let counts = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 3)), Some(YearMonth::new(2006, 3)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .allow_pseudo_users()
        .count_by_month()
        .expect("scan over a truncated month must not error out — \
                 the streaming reader is supposed to warn and skip");

    // The map may be empty (file failed before any record was decoded) or
    // contain a partial count for 2006-03; either is acceptable. The
    // load-bearing assertion is that we got here without panicking.
    let partial = counts
        .get(&YearMonth::new(2006, 3))
        .copied()
        .unwrap_or(0);
    assert!(
        partial < FIXTURE_RECORDS as u64,
        "expected a partial count from the truncated month, got {} (full count would be {})",
        partial,
        FIXTURE_RECORDS
    );
}
