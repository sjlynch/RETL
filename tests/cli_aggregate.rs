use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::json;
use std::fs;
use std::io::Write;
use std::path::Path;

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
}

fn write_jsonl(path: &Path, rows: &[serde_json::Value]) {
    let mut f = fs::File::create(path).unwrap();
    for row in rows {
        writeln!(f, "{}", row).unwrap();
    }
}

fn read_lines(path: &Path) -> Vec<String> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect()
}

#[test]
fn aggregate_by_subreddit_writes_count_tsv() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.jsonl");
    let out = dir.path().join("counts.tsv");
    write_jsonl(
        &input,
        &[
            json!({"subreddit":"rust", "author":"alice", "created_utc":1136073600, "score":2}),
            json!({"subreddit":"rust", "author":"bob", "created_utc":1136073601, "score":5}),
            json!({"subreddit":"python", "author":"alice", "created_utc":1136073602, "score":7}),
        ],
    );

    retl()
        .args([
            "aggregate",
            "--no-progress",
            "--by",
            "subreddit",
            "--out",
            out.to_str().unwrap(),
            input.to_str().unwrap(),
        ])
        .assert()
        .success();

    assert_eq!(read_lines(&out), vec!["python\t1", "rust\t2"]);
}

#[test]
fn aggregate_by_author_top_sorts_by_count_desc_then_key() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.jsonl");
    let out = dir.path().join("top.tsv");
    write_jsonl(
        &input,
        &[
            json!({"subreddit":"rust", "author":"charlie", "created_utc":1136073600, "score":2}),
            json!({"subreddit":"rust", "author":"alice", "created_utc":1136073601, "score":5}),
            json!({"subreddit":"python", "author":"bob", "created_utc":1136073602, "score":7}),
            json!({"subreddit":"python", "author":"alice", "created_utc":1136073603, "score":11}),
        ],
    );

    retl()
        .args([
            "aggregate",
            "--no-progress",
            "--by",
            "author",
            "--top",
            "2",
            "--out",
            out.to_str().unwrap(),
            input.to_str().unwrap(),
        ])
        .assert()
        .success();

    assert_eq!(read_lines(&out), vec!["alice\t2", "bob\t1"]);
}

#[test]
fn aggregate_json_pointer_sum_and_month_grouping() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.jsonl");
    let sum_out = dir.path().join("score_by_sub.tsv");
    let month_out = dir.path().join("by_month.tsv");
    write_jsonl(
        &input,
        &[
            json!({"subreddit":"rust", "author":"alice", "created_utc":1136073600, "score":2}),
            json!({"subreddit":"rust", "author":"bob", "created_utc":1138752000, "score":5}),
            json!({"subreddit":"python", "author":"alice", "created_utc":1138752001, "score":7}),
        ],
    );

    retl()
        .args([
            "aggregate",
            "--no-progress",
            "--by",
            "json:/subreddit",
            "--metric",
            "sum:/score",
            "--out",
            sum_out.to_str().unwrap(),
            input.to_str().unwrap(),
        ])
        .assert()
        .success();
    assert_eq!(read_lines(&sum_out), vec!["python\t7", "rust\t7"]);

    retl()
        .args([
            "aggregate",
            "--no-progress",
            "--by",
            "month",
            "--out",
            month_out.to_str().unwrap(),
            input.to_str().unwrap(),
        ])
        .assert()
        .success();
    assert_eq!(read_lines(&month_out), vec!["2006-01\t1", "2006-02\t2"]);
}

#[test]
fn aggregate_fails_when_all_inputs_fail() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("counts.json");
    let work = dir.path().join("work");
    let missing_a = dir.path().join("missing-a.jsonl");
    let missing_b = dir.path().join("missing-b.jsonl");

    retl()
        .args([
            "aggregate",
            "--no-progress",
            "--work-dir",
            work.to_str().unwrap(),
            "--out",
            out.to_str().unwrap(),
            missing_a.to_str().unwrap(),
            missing_b.to_str().unwrap(),
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "aggregate failed: 2 of 2 input(s) failed",
        ));

    assert!(
        !out.exists(),
        "total failure should not publish an empty aggregate"
    );
}

#[test]
fn aggregate_plain_count_does_not_warn_about_resume() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.jsonl");
    let out = dir.path().join("counts.json");
    let work = dir.path().join("work");
    write_jsonl(&input, &[json!({"id":"a"})]);

    retl()
        .args([
            "aggregate",
            "--no-progress",
            "--work-dir",
            work.to_str().unwrap(),
            "--out",
            out.to_str().unwrap(),
            input.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains("does not support resume").not());
}

#[test]
fn aggregate_shards_dir_preserves_unrelated_files() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.jsonl");
    let out = dir.path().join("counts.json");
    let work = dir.path().join("work");
    let shards = dir.path().join("caller_shards");
    fs::create_dir(&shards).unwrap();
    let unrelated_txt = shards.join("keep.txt");
    let unrelated_json = shards.join("keep.json");
    fs::write(&unrelated_txt, "do not delete me").unwrap();
    fs::write(&unrelated_json, "{\"count\":999}").unwrap();
    write_jsonl(&input, &[json!({"id":"a"})]);

    retl()
        .args([
            "aggregate",
            "--no-progress",
            "--work-dir",
            work.to_str().unwrap(),
            "--shards-dir",
            shards.to_str().unwrap(),
            "--out",
            out.to_str().unwrap(),
            input.to_str().unwrap(),
        ])
        .assert()
        .success();

    assert_eq!(
        fs::read_to_string(&unrelated_txt).unwrap(),
        "do not delete me"
    );
    assert_eq!(
        fs::read_to_string(&unrelated_json).unwrap(),
        "{\"count\":999}"
    );
    let aggregate: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&out).unwrap()).unwrap();
    assert_eq!(aggregate["count"].as_u64(), Some(1));
}
