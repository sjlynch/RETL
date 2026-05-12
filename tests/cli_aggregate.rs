use assert_cmd::Command;
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
