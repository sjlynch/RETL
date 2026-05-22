//! CLI parity tests for the scan-family subcommands:
//!   - `--out -` writes to stdout (and never creates a file literally
//!     named `-`) on `scan` and `first-seen`.
//!   - `--limit` / `--head` caps the records scanned on `count`, `dedupe`,
//!     and `first-seen`, matching `export`/`sample`/`scan`.

#[path = "common/mod.rs"]
mod common;

use assert_cmd::Command;
use common::{make_corpus_basic, read_lines};
use predicates::prelude::*;
use predicates::str::contains;

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
}

// --- `--out -` stdout support ----------------------------------------------

#[test]
fn scan_out_dash_streams_to_stdout_without_creating_dash_file() {
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
            "--out",
            "-",
        ])
        .assert()
        .success()
        .stdout(contains("alice\n"));

    assert!(
        !cwd.path().join("-").exists(),
        "`--out -` must stream to stdout, not stage-and-rename a file named `-`"
    );
}

#[test]
fn first_seen_out_dash_streams_to_stdout_without_creating_dash_file() {
    let base = make_corpus_basic();
    let cwd = tempfile::tempdir().unwrap();
    let work_dir = cwd.path().join("work");

    retl()
        .current_dir(cwd.path())
        .arg("first-seen")
        .arg("--data-dir")
        .arg(&base)
        .arg("--work-dir")
        .arg(&work_dir)
        .args([
            "--source",
            "rc",
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
            "-",
        ])
        .assert()
        .success()
        .stdout(contains("alice\t1136074600"));

    assert!(
        !cwd.path().join("-").exists(),
        "`--out -` must stream to stdout, not stage-and-rename a file named `-`"
    );
}

#[test]
fn aggregate_out_dash_streams_to_stdout_without_creating_dash_file() {
    let cwd = tempfile::tempdir().unwrap();
    let input = cwd.path().join("input.jsonl");
    std::fs::write(&input, "{\"subreddit\":\"rust\"}\n{\"subreddit\":\"rust\"}\n").unwrap();

    retl()
        .current_dir(cwd.path())
        .arg("aggregate")
        .arg(&input)
        .args([
            "--shards-dir",
            cwd.path().join("shards").to_str().unwrap(),
            "--no-progress",
            "--out",
            "-",
        ])
        .assert()
        .success()
        .stdout(contains("\"count\""));

    assert!(
        !cwd.path().join("-").exists(),
        "`aggregate --out -` must stream to stdout, not stage-and-rename a file named `-`"
    );
}

// --- `--limit` parity -------------------------------------------------------

#[test]
fn count_month_limit_caps_records_scanned() {
    let base = make_corpus_basic();
    let work_dir = tempfile::tempdir().unwrap();

    // RC_2006-01 has two non-pseudo matching comments (alice, charlie).
    // Unbounded the month count is 2; `--limit 1` stops after one.
    retl()
        .arg("count")
        .arg("--data-dir")
        .arg(&base)
        .arg("--work-dir")
        .arg(work_dir.path())
        .args(["--source", "rc", "--subreddit", "programming", "--no-progress"])
        .assert()
        .success()
        .stdout(predicate::str::contains("2006-01\t2"));

    retl()
        .arg("count")
        .arg("--data-dir")
        .arg(&base)
        .arg("--work-dir")
        .arg(work_dir.path())
        .args([
            "--source",
            "rc",
            "--subreddit",
            "programming",
            "--no-progress",
            "--limit",
            "1",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("2006-01\t1"));
}

#[test]
fn count_limit_accepts_head_alias() {
    let base = make_corpus_basic();
    let work_dir = tempfile::tempdir().unwrap();

    retl()
        .arg("count")
        .arg("--data-dir")
        .arg(&base)
        .arg("--work-dir")
        .arg(work_dir.path())
        .args([
            "--source",
            "rc",
            "--subreddit",
            "programming",
            "--no-progress",
            "--head",
            "1",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("2006-01\t1"));
}

#[test]
fn dedupe_limit_caps_records_scanned() {
    let base = make_corpus_basic();
    let work_dir = tempfile::tempdir().unwrap();
    let out = work_dir.path().join("authors.txt");

    retl()
        .args([
            "dedupe",
            "--data-dir",
            base.to_str().unwrap(),
            "--work-dir",
            work_dir.path().to_str().unwrap(),
            "--source",
            "rc",
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--subreddit",
            "programming",
            "--key",
            "author",
            "--out",
            out.to_str().unwrap(),
            "--limit",
            "1",
            "--no-progress",
        ])
        .assert()
        .success()
        .stderr(contains("from 1 matching records"));

    // Unbounded the dedupe emits alice + charlie; `--limit 1` keeps only the
    // first matched comment (alice).
    assert_eq!(read_lines(&out), vec!["alice"]);
}

#[test]
fn first_seen_limit_caps_records_scanned() {
    let base = make_corpus_basic();
    let work_dir = tempfile::tempdir().unwrap();
    let out = work_dir.path().join("first_seen.tsv");

    retl()
        .args([
            "first-seen",
            "--data-dir",
            base.to_str().unwrap(),
            "--work-dir",
            work_dir.path().to_str().unwrap(),
            "--source",
            "rc",
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--subreddit",
            "programming",
            "--out",
            out.to_str().unwrap(),
            "--limit",
            "1",
            "--no-progress",
        ])
        .assert()
        .success();

    // Without the limit both alice and charlie appear; `--limit 1` stops
    // after the first matched comment (alice).
    assert_eq!(read_lines(&out), vec!["alice\t1136074600"]);
}
