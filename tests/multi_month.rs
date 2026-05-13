//! Multi-month / missing-month coverage:
//! - `paths::plan_files` skips missing months while planner diagnostics report requested holes (corpus has 2005-12 and 2006-02 but not 2006-01).
//! - `date::iter_year_months` correctly crosses the year boundary.
//! - `filters::bounds_tuple` + `within_bounds` filter records by record-level `created_utc`
//!   (independent from the plan-level filename gate).

#[path = "common/mod.rs"]
mod common;

use common::*;

use retl::{
    bounds_tuple, discover_all, format_year_month_ranges, iter_year_months,
    missing_month_diagnostics, parse_minimal, plan_files, within_bounds, FileKind, Sources,
    YearMonth,
};

#[test]
fn iter_year_months_crosses_year_boundary() {
    let start = YearMonth::new(2005, 11);
    let end = YearMonth::new(2006, 2);
    let got: Vec<YearMonth> = iter_year_months(start, end).collect();
    assert_eq!(
        got,
        vec![
            YearMonth::new(2005, 11),
            YearMonth::new(2005, 12),
            YearMonth::new(2006, 1),
            YearMonth::new(2006, 2),
        ]
    );
}

#[test]
fn iter_year_months_inclusive_endpoints_and_inverted_range_is_empty() {
    // Single-month range
    let single: Vec<YearMonth> =
        iter_year_months(YearMonth::new(2010, 5), YearMonth::new(2010, 5)).collect();
    assert_eq!(single, vec![YearMonth::new(2010, 5)]);

    // Inverted range (start > end) yields nothing.
    let none: Vec<YearMonth> =
        iter_year_months(YearMonth::new(2010, 6), YearMonth::new(2010, 5)).collect();
    assert!(none.is_empty());
}

#[test]
fn plan_files_skips_missing_month_and_diagnostics_report_hole() {
    // Corpus has 2005-12 and 2006-02 but NOT 2006-01.
    let base = make_corpus_multi_month(&[YearMonth::new(2005, 12), YearMonth::new(2006, 2)]);
    let discovered = discover_all(&base.join("comments"), &base.join("submissions"));

    // Plan a range that *requires* 2005-12, 2006-01, 2006-02. The 2006-01 hole
    // is skipped in the executable plan, and reported separately as diagnostics.
    let jobs = plan_files(
        &discovered,
        Sources::Both,
        Some(YearMonth::new(2005, 12)),
        Some(YearMonth::new(2006, 2)),
    );

    // Expect exactly 4 jobs: 2 months × 2 sources (Comment + Submission).
    assert_eq!(
        jobs.len(),
        4,
        "got jobs: {:?}",
        jobs.iter().map(|j| j.path.clone()).collect::<Vec<_>>()
    );

    // No job should refer to month 2006-01.
    let mut months: Vec<YearMonth> = jobs.iter().map(|j| j.ym).collect();
    months.sort();
    months.dedup();
    assert_eq!(
        months,
        vec![YearMonth::new(2005, 12), YearMonth::new(2006, 2)]
    );

    // Both kinds present.
    let n_rc = jobs
        .iter()
        .filter(|j| matches!(j.kind, FileKind::Comment))
        .count();
    let n_rs = jobs
        .iter()
        .filter(|j| matches!(j.kind, FileKind::Submission))
        .count();
    assert_eq!(n_rc, 2);
    assert_eq!(n_rs, 2);

    let diagnostics = missing_month_diagnostics(
        &discovered,
        Sources::Both,
        Some(YearMonth::new(2005, 12)),
        Some(YearMonth::new(2006, 2)),
    );
    assert_eq!(diagnostics.len(), 2);
    for diag in diagnostics {
        assert_eq!(diag.months, vec![YearMonth::new(2006, 1)]);
        assert_eq!(format_year_month_ranges(&diag.months), "2006-01");
    }
}

#[test]
fn plan_files_clamps_to_existing_when_start_unset() {
    let base = make_corpus_multi_month(&[YearMonth::new(2005, 12), YearMonth::new(2006, 2)]);
    let discovered = discover_all(&base.join("comments"), &base.join("submissions"));

    // Sources::Comments only, end=2006-02, start=None ⇒ clamp to existing min (2005-12).
    let jobs = plan_files(
        &discovered,
        Sources::Comments,
        None,
        Some(YearMonth::new(2006, 2)),
    );
    assert_eq!(jobs.len(), 2, "RC files for 2005-12 and 2006-02");
    for j in &jobs {
        assert!(matches!(j.kind, FileKind::Comment));
    }
}

#[test]
fn within_bounds_uses_record_level_timestamp() {
    let bounds = bounds_tuple(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)));
    assert!(bounds.is_some(), "any endpoint set => Some");

    // Synthesize a MinimalRecord from a JSON line. created_utc is what within_bounds reads.
    fn rec(ts: i64) -> retl::MinimalRecord {
        let line = serde_json::json!({
            "subreddit":"x", "author":"a", "created_utc": ts
        })
        .to_string();
        parse_minimal(&line).unwrap()
    }

    // 2006-01-15 is inside [2006-01, 2006-01].
    let in_jan = rec(1_137_283_200);
    assert!(within_bounds(&in_jan, bounds));

    // 2005-12-31 is outside.
    let dec_2005 = rec(1_136_073_599); // 2005-12-31 23:59:59 UTC
    assert!(!within_bounds(&dec_2005, bounds));

    // 2006-02-01 is outside.
    let feb_2006 = rec(1_138_752_000);
    assert!(!within_bounds(&feb_2006, bounds));

    // Records with no created_utc are rejected when bounds are set.
    let no_ts_line = serde_json::json!({"subreddit":"x", "author":"a"}).to_string();
    let no_ts = parse_minimal(&no_ts_line).unwrap();
    assert!(!within_bounds(&no_ts, bounds));

    // One-sided lower bound: December is out, January/February are in.
    let lower_only = bounds_tuple(Some(YearMonth::new(2006, 1)), None);
    assert!(lower_only.is_some(), "one-sided lower bound is active");
    assert!(!within_bounds(&dec_2005, lower_only));
    assert!(within_bounds(&in_jan, lower_only));
    assert!(within_bounds(&feb_2006, lower_only));
    assert!(!within_bounds(&no_ts, lower_only));

    // One-sided upper bound: February is out, December/January are in.
    let upper_only = bounds_tuple(None, Some(YearMonth::new(2006, 1)));
    assert!(upper_only.is_some(), "one-sided upper bound is active");
    assert!(within_bounds(&dec_2005, upper_only));
    assert!(within_bounds(&in_jan, upper_only));
    assert!(!within_bounds(&feb_2006, upper_only));
    assert!(!within_bounds(&no_ts, upper_only));

    // With no date bounds at all, within_bounds is permissive.
    let no_bounds = bounds_tuple(None, None);
    assert!(no_bounds.is_none());
    assert!(within_bounds(&dec_2005, no_bounds));
    assert!(within_bounds(&no_ts, no_bounds));
}

#[test]
fn bounds_tuple_represents_independent_endpoints() {
    assert_eq!(bounds_tuple(None, None), None);
    assert_eq!(
        bounds_tuple(Some(YearMonth::new(2006, 1)), None)
            .unwrap()
            .start,
        Some(YearMonth::new(2006, 1))
    );
    assert_eq!(
        bounds_tuple(None, Some(YearMonth::new(2006, 1)))
            .unwrap()
            .end,
        Some(YearMonth::new(2006, 1))
    );
    let bounded =
        bounds_tuple(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 3))).unwrap();
    assert_eq!(bounded.start, Some(YearMonth::new(2006, 1)));
    assert_eq!(bounded.end, Some(YearMonth::new(2006, 3)));
}
