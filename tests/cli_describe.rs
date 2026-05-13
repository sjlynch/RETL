#[path = "common/mod.rs"]
mod common;

use assert_cmd::Command;
use common::make_corpus_multi_month;
use predicates::prelude::*;
use predicates::str::contains;
use retl::YearMonth;
use std::fs;

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
}

#[test]
fn describe_reports_available_ranges_counts_and_bytes() {
    let base = make_corpus_multi_month(&[
        YearMonth::new(2006, 1),
        YearMonth::new(2006, 2),
        YearMonth::new(2006, 3),
    ]);
    let rc_feb = base.join("comments").join("RC_2006-02.zst");
    let rs_feb = base.join("submissions").join("RS_2006-02.zst");
    let rc_bytes = fs::metadata(&rc_feb).unwrap().len();
    let rs_bytes = fs::metadata(&rs_feb).unwrap().len();
    let total_bytes = rc_bytes + rs_bytes;

    retl()
        .args([
            "describe",
            "--data-dir",
            base.to_str().unwrap(),
            "--start",
            "2006-02",
            "--end",
            "2006-02",
        ])
        .assert()
        .success()
        .stdout(
            contains("source\tavailable\tfiles_in_range\tcompressed_bytes")
                .and(contains(format!("rc\t2006-01..=2006-03\t1\t{rc_bytes}")))
                .and(contains(format!("rs\t2006-01..=2006-03\t1\t{rs_bytes}")))
                .and(contains(format!("total\t\t2\t{total_bytes}"))),
        );
}

#[test]
fn describe_reports_missing_month_holes() {
    let base = make_corpus_multi_month(&[YearMonth::new(2006, 1), YearMonth::new(2006, 3)]);

    retl()
        .args(["describe", "--data-dir", base.to_str().unwrap()])
        .assert()
        .success()
        .stdout(
            contains("missing_month_count\tmissing_months")
                .and(contains("rc\t2006-01..=2006-03\t2"))
                .and(contains("\t1\t2006-02"))
                .and(contains("rs\t2006-01..=2006-03\t2"))
                .and(contains("total\t\t4").and(contains("\t2\t-"))),
        );
}

#[test]
fn describe_honors_source_selection() {
    let base = make_corpus_multi_month(&[YearMonth::new(2006, 1)]);
    let rc = base.join("comments").join("RC_2006-01.zst");
    let rc_bytes = fs::metadata(&rc).unwrap().len();

    retl()
        .args([
            "describe",
            "--data-dir",
            base.to_str().unwrap(),
            "--source",
            "rc",
        ])
        .assert()
        .success()
        .stdout(
            contains(format!("rc\t2006-01..=2006-01\t1\t{rc_bytes}"))
                .and(contains(format!("total\t\t1\t{rc_bytes}")))
                .and(predicate::str::contains("rs\t").not()),
        );
}
