#[path = "common/mod.rs"]
mod common;

use assert_cmd::Command;
use common::{make_corpus_basic, read_lines};
use predicates::prelude::*;
use std::fs;

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
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
