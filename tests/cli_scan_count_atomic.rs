#[path = "common/mod.rs"]
mod common;

use assert_cmd::Command;
use common::{make_corpus_basic, make_corpus_multi_month, read_lines, write_zst_lines};
use predicates::prelude::*;
use retl::YearMonth;
use serde_json::json;
use std::fs;

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
}

#[test]
fn count_one_sided_start_and_end_apply_record_timestamp_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();
    let rc = base.join("comments").join("RC_2006-01.zst");
    write_zst_lines(
        &rc,
        &[
            json!({
                "id": "dec", "author": "alice", "subreddit": "programming",
                "created_utc": 1_136_073_599_i64, "body": "dec"
            })
            .to_string(),
            json!({
                "id": "jan", "author": "alice", "subreddit": "programming",
                "created_utc": 1_136_073_600_i64, "body": "jan"
            })
            .to_string(),
            json!({
                "id": "feb", "author": "alice", "subreddit": "programming",
                "created_utc": 1_138_752_000_i64, "body": "feb"
            })
            .to_string(),
        ],
    );

    let start_only = retl()
        .arg("count")
        .arg("--data-dir")
        .arg(base)
        .args(["--source", "rc", "--start", "2006-01", "--no-progress"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let start_only = String::from_utf8(start_only).unwrap();
    assert!(!start_only.contains("2005-12"), "{start_only}");
    assert!(start_only.contains("2006-01\t1"), "{start_only}");
    assert!(start_only.contains("2006-02\t1"), "{start_only}");

    let end_only = retl()
        .arg("count")
        .arg("--data-dir")
        .arg(base)
        .args(["--source", "rc", "--end", "2006-01", "--no-progress"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let end_only = String::from_utf8(end_only).unwrap();
    assert!(end_only.contains("2005-12\t1"), "{end_only}");
    assert!(end_only.contains("2006-01\t1"), "{end_only}");
    assert!(!end_only.contains("2006-02"), "{end_only}");
}

#[test]
fn count_warns_when_requested_range_has_missing_month_hole() {
    let base = make_corpus_multi_month(&[YearMonth::new(2006, 1), YearMonth::new(2006, 3)]);

    retl()
        .arg("count")
        .arg("--data-dir")
        .arg(&base)
        .args([
            "--source",
            "rc",
            "--start",
            "2006-01",
            "--end",
            "2006-03",
            "--no-progress",
        ])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("2006-01\t2")
                .and(predicate::str::contains("2006-03\t2"))
                .and(predicate::str::contains("missing month files"))
                .and(predicate::str::contains("2006-02")),
        );
}

#[test]
fn scan_stdout_still_streams_usernames() {
    let base = make_corpus_basic();
    let cwd = tempfile::tempdir().unwrap();
    let work_dir = cwd.path().join("work");

    retl()
        .current_dir(cwd.path())
        .arg("scan")
        .arg("--data-dir")
        .arg(&base)
        .arg("--work-dir")
        .arg(&work_dir)
        .args([
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--subreddit",
            "programming",
            "--author",
            "alice",
            "--no-progress",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("alice\n"));
}

#[test]
fn scan_out_publish_failure_does_not_replace_existing_directory() {
    let base = make_corpus_basic();
    let cwd = tempfile::tempdir().unwrap();
    let work_dir = cwd.path().join("work");
    let out_dir = cwd.path().join("authors.txt");
    fs::create_dir(&out_dir).unwrap();

    retl()
        .arg("scan")
        .arg("--data-dir")
        .arg(&base)
        .arg("--work-dir")
        .arg(&work_dir)
        .args([
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--subreddit",
            "programming",
            "--author",
            "alice",
            "--no-progress",
            "--out",
        ])
        .arg(&out_dir)
        .assert()
        .failure()
        .stderr(predicate::str::contains("publishing staged output"));

    assert!(
        out_dir.is_dir(),
        "failed publish must leave existing destination intact"
    );
    assert!(
        read_lines_in_staging(&cwd.path().join("_staging")).is_empty(),
        "failed publish should clean up staged scan output"
    );
}

#[test]
fn count_month_out_publish_failure_does_not_replace_existing_directory() {
    let base = make_corpus_basic();
    let cwd = tempfile::tempdir().unwrap();
    let work_dir = cwd.path().join("work");
    let out_dir = cwd.path().join("counts.tsv");
    fs::create_dir(&out_dir).unwrap();

    retl()
        .arg("count")
        .arg("--data-dir")
        .arg(&base)
        .arg("--work-dir")
        .arg(&work_dir)
        .args([
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--subreddit",
            "programming",
            "--author",
            "alice",
            "--no-progress",
            "--out",
        ])
        .arg(&out_dir)
        .assert()
        .failure()
        .stderr(predicate::str::contains("publishing staged output"));

    assert!(
        out_dir.is_dir(),
        "failed publish must leave existing destination intact"
    );
    assert!(
        read_lines_in_staging(&cwd.path().join("_staging")).is_empty(),
        "failed publish should clean up staged count output"
    );
}

#[test]
fn count_month_file_output_is_published_on_success() {
    let base = make_corpus_basic();
    let cwd = tempfile::tempdir().unwrap();
    let work_dir = cwd.path().join("work");
    let out = cwd.path().join("counts.tsv");

    retl()
        .arg("count")
        .arg("--data-dir")
        .arg(&base)
        .arg("--work-dir")
        .arg(&work_dir)
        .args([
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--subreddit",
            "programming",
            "--author",
            "alice",
            "--no-progress",
            "--out",
        ])
        .arg(&out)
        .assert()
        .success();

    assert_eq!(read_lines(&out), vec!["2006-01\t1"]);
}

fn read_lines_in_staging(staging: &std::path::Path) -> Vec<String> {
    if !staging.exists() {
        return Vec::new();
    }
    fs::read_dir(staging)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect()
}
