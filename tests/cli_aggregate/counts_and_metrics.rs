use crate::common::cli::{read_text_lines as read_lines, retl, write_jsonl};
use serde_json::json;

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
            "--parallelism",
            "1",
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
fn aggregate_sum_large_integer_metric_uses_plain_decimal_tsv() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.jsonl");
    let out = dir.path().join("scores.tsv");
    write_jsonl(
        &input,
        &[
            json!({"subreddit":"rust", "score":500_000_000_000_000_i64}),
            json!({"subreddit":"rust", "score":500_000_000_000_000_i64}),
        ],
    );

    retl()
        .args([
            "aggregate",
            "--no-progress",
            "--by",
            "subreddit",
            "--metric",
            "sum:/score",
            "--out",
            out.to_str().unwrap(),
            input.to_str().unwrap(),
        ])
        .assert()
        .success();

    assert_eq!(read_lines(&out), vec!["rust\t1000000000000000"]);
}

#[test]
fn aggregate_avg_keeps_more_than_six_decimal_places() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.jsonl");
    let out = dir.path().join("avg.tsv");
    let mut rows = vec![json!({"subreddit":"rust", "score":1})];
    rows.extend((0..6).map(|_| json!({"subreddit":"rust", "score":0})));
    write_jsonl(&input, &rows);

    retl()
        .args([
            "aggregate",
            "--no-progress",
            "--by",
            "subreddit",
            "--metric",
            "avg:/score",
            "--out",
            out.to_str().unwrap(),
            input.to_str().unwrap(),
        ])
        .assert()
        .success();

    assert_eq!(read_lines(&out), vec!["rust\t0.142857142857142857"]);
}

#[test]
fn aggregate_sum_preserves_integer_precision_beyond_f64_range() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.jsonl");
    let out = dir.path().join("scores.tsv");
    write_jsonl(
        &input,
        &[
            json!({"subreddit":"rust", "score":9_007_199_254_740_993_u64}),
            json!({"subreddit":"rust", "score":1}),
        ],
    );

    retl()
        .args([
            "aggregate",
            "--no-progress",
            "--by",
            "subreddit",
            "--metric",
            "sum:/score",
            "--out",
            out.to_str().unwrap(),
            input.to_str().unwrap(),
        ])
        .assert()
        .success();

    assert_eq!(read_lines(&out), vec!["rust\t9007199254740994"]);
}

#[test]
fn aggregate_min_max_numeric_strings_preserve_integer_precision() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.jsonl");
    let min_out = dir.path().join("min.tsv");
    let max_out = dir.path().join("max.tsv");
    write_jsonl(
        &input,
        &[
            json!({"subreddit":"rust", "score":"9007199254740993"}),
            json!({"subreddit":"rust", "score":"9007199254740992"}),
        ],
    );

    for (metric, out) in [("min:/score", &min_out), ("max:/score", &max_out)] {
        retl()
            .args([
                "aggregate",
                "--no-progress",
                "--by",
                "subreddit",
                "--metric",
                metric,
                "--out",
                out.to_str().unwrap(),
                input.to_str().unwrap(),
            ])
            .assert()
            .success();
    }

    assert_eq!(read_lines(&min_out), vec!["rust\t9007199254740992"]);
    assert_eq!(read_lines(&max_out), vec!["rust\t9007199254740993"]);
}
