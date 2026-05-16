use crate::common::cli::{read_text_lines as read_lines, retl, write_jsonl};
use predicates::prelude::*;
use serde_json::json;
use std::fs;

#[test]
fn aggregate_spool_matches_explicit_file_list() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    fs::create_dir(&spool).unwrap();
    let rc = spool.join("part_RC_2006-01.jsonl");
    let rs = spool.join("part_RS_2006-02.jsonl");
    let explicit_out = dir.path().join("explicit.tsv");
    let spool_out = dir.path().join("spool.tsv");
    write_jsonl(
        &rc,
        &[
            json!({"subreddit":"rust", "author":"alice"}),
            json!({"subreddit":"python", "author":"bob"}),
        ],
    );
    write_jsonl(&rs, &[json!({"subreddit":"rust", "author":"carol"})]);

    retl()
        .args([
            "aggregate",
            "--no-progress",
            "--by",
            "subreddit",
            "--out",
            explicit_out.to_str().unwrap(),
            rc.to_str().unwrap(),
            rs.to_str().unwrap(),
        ])
        .assert()
        .success();

    retl()
        .args([
            "aggregate",
            "--no-progress",
            "--spool",
            spool.to_str().unwrap(),
            "--by",
            "subreddit",
            "--out",
            spool_out.to_str().unwrap(),
        ])
        .assert()
        .success();

    assert_eq!(read_lines(&spool_out), read_lines(&explicit_out));
}

#[test]
fn aggregate_rejects_spool_and_explicit_inputs() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    fs::create_dir(&spool).unwrap();
    let input = dir.path().join("input.jsonl");
    let out = dir.path().join("counts.json");
    write_jsonl(&input, &[json!({"id":"a"})]);

    retl()
        .args([
            "aggregate",
            "--no-progress",
            "--spool",
            spool.to_str().unwrap(),
            "--out",
            out.to_str().unwrap(),
            input.to_str().unwrap(),
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "either --spool <DIR> or explicit JSONL input files, not both",
        ));
}

#[test]
fn aggregate_literal_glob_suggests_spool() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    fs::create_dir(&spool).unwrap();
    let out = dir.path().join("counts.json");
    let literal_glob = spool.join("*.jsonl");

    retl()
        .args([
            "aggregate",
            "--no-progress",
            "--out",
            out.to_str().unwrap(),
            literal_glob.to_str().unwrap(),
        ])
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("looks like an unexpanded glob")
                .and(predicate::str::contains("retl aggregate --spool")),
        );

    assert!(!out.exists(), "glob failure should not publish output");
}

#[test]
fn aggregate_empty_spool_fails_clearly() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("empty-spool");
    fs::create_dir(&spool).unwrap();
    let out = dir.path().join("counts.json");

    retl()
        .args([
            "aggregate",
            "--no-progress",
            "--spool",
            spool.to_str().unwrap(),
            "--out",
            out.to_str().unwrap(),
        ])
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("no part_RC_YYYY-MM.jsonl or part_RS_YYYY-MM.jsonl")
                .and(predicate::str::contains("retl export --format spool")),
        );

    assert!(
        !out.exists(),
        "empty spool failure should not publish output"
    );
}
