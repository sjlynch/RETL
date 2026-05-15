use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::json;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command as StdCommand, Stdio};

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

#[test]
fn aggregate_fails_when_all_inputs_fail() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("counts.json");
    let missing_a = dir.path().join("missing-a.jsonl");
    let missing_b = dir.path().join("missing-b.jsonl");

    retl()
        .args([
            "aggregate",
            "--no-progress",
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
fn aggregate_fails_when_all_inputs_are_malformed() {
    let dir = tempfile::tempdir().unwrap();
    let bad = dir.path().join("bad.jsonl");
    let out = dir.path().join("counts.json");
    fs::write(&bad, "not-json\n").unwrap();

    retl()
        .args([
            "aggregate",
            "--no-progress",
            "--out",
            out.to_str().unwrap(),
            bad.to_str().unwrap(),
        ])
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("bad.jsonl")
                .and(predicate::str::contains("malformed JSON"))
                .and(predicate::str::contains("line 1"))
                .and(predicate::str::contains(
                    "aggregate failed: 1 of 1 input(s) failed",
                )),
        );

    assert!(
        !out.exists(),
        "total malformed input should not publish an empty aggregate"
    );
}

#[test]
fn aggregate_reports_each_failed_input_by_filename() {
    let dir = tempfile::tempdir().unwrap();
    let good_a = dir.path().join("good-a.jsonl");
    let good_b = dir.path().join("good-b.jsonl");
    let bad = dir.path().join("bad-input.jsonl");
    let out = dir.path().join("counts.json");
    write_jsonl(&good_a, &[json!({"id":"a"})]);
    write_jsonl(&good_b, &[json!({"id":"b"})]);
    fs::write(&bad, "not-json\n").unwrap();

    retl()
        .args([
            "aggregate",
            "--no-progress",
            "--out",
            out.to_str().unwrap(),
            good_a.to_str().unwrap(),
            bad.to_str().unwrap(),
            good_b.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stderr(
            predicate::str::contains("Aggregate input(s) failed during shard build")
                .and(predicate::str::contains("bad-input.jsonl"))
                .and(predicate::str::contains("malformed JSON"))
                .and(predicate::str::contains(
                    "1 input(s) failed during shard build",
                )),
        );

    let aggregate: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&out).unwrap()).unwrap();
    assert_eq!(aggregate["count"].as_u64(), Some(2));
}

#[test]
fn aggregate_reports_partial_read_and_does_not_merge_it() {
    let dir = tempfile::tempdir().unwrap();
    let good = dir.path().join("good.jsonl");
    let partial = dir.path().join("partial.jsonl");
    let out = dir.path().join("counts.json");
    write_jsonl(&good, &[json!({"id":"good"})]);
    fs::write(&partial, b"{\"id\":\"partial-before-error\"}\n{\"id\":\"").unwrap();
    fs::OpenOptions::new()
        .append(true)
        .open(&partial)
        .unwrap()
        .write_all(&[0xff, b'\n'])
        .unwrap();

    retl()
        .args([
            "aggregate",
            "--no-progress",
            "--out",
            out.to_str().unwrap(),
            good.to_str().unwrap(),
            partial.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stderr(
            predicate::str::contains("Aggregate input(s) skipped after partial read")
                .and(predicate::str::contains("partial.jsonl"))
                .and(predicate::str::contains(
                    "1 input(s) skipped after partial read",
                )),
        );

    let aggregate: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&out).unwrap()).unwrap();
    assert_eq!(aggregate["count"].as_u64(), Some(1));
}

#[test]
fn aggregate_creates_output_parent_and_uses_staging_dir() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.jsonl");
    let out = dir.path().join("nested").join("counts.json");
    write_jsonl(&input, &[json!({"id":"a"})]);

    retl()
        .args([
            "aggregate",
            "--no-progress",
            "--out",
            out.to_str().unwrap(),
            input.to_str().unwrap(),
        ])
        .assert()
        .success();

    assert!(out.exists());
    assert!(out.parent().unwrap().join("_staging").is_dir());
    assert!(
        !out.with_extension("json.inprogress").exists(),
        "aggregate must not stage next to the final output"
    );
}

#[test]
fn aggregate_concurrent_same_output_does_not_corrupt_staging_or_final() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.jsonl");
    let out = dir.path().join("counts.json");
    let rows: Vec<_> = (0..2_000).map(|i| json!({"id": i})).collect();
    write_jsonl(&input, &rows);

    let exe = assert_cmd::cargo::cargo_bin("retl");
    let args = [
        "aggregate",
        "--no-progress",
        "--parallelism",
        "1",
        "--out",
        out.to_str().unwrap(),
        input.to_str().unwrap(),
    ];
    let first = StdCommand::new(&exe)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let second = StdCommand::new(&exe)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let first_out = first.wait_with_output().unwrap();
    let second_out = second.wait_with_output().unwrap();

    assert!(
        first_out.status.success(),
        "first aggregate failed: {}",
        String::from_utf8_lossy(&first_out.stderr)
    );
    assert!(
        second_out.status.success(),
        "second aggregate failed: {}",
        String::from_utf8_lossy(&second_out.stderr)
    );

    let aggregate: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&out).unwrap()).unwrap();
    assert_eq!(aggregate["count"].as_u64(), Some(rows.len() as u64));
    let staging = out.parent().unwrap().join("_staging");
    let leftovers: Vec<_> = fs::read_dir(&staging)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".inprogress"))
        .collect();
    assert!(leftovers.is_empty(), "leftover staged files: {leftovers:?}");
}

#[test]
fn aggregate_pretty_field_indents_json() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.jsonl");
    let out = dir.path().join("counts.json");
    write_jsonl(&input, &[json!({"id":"a"})]);

    retl()
        .args([
            "aggregate",
            "--no-progress",
            "--pretty",
            "--out",
            out.to_str().unwrap(),
            input.to_str().unwrap(),
        ])
        .assert()
        .success();

    let s = fs::read_to_string(&out).unwrap();
    assert!(s.contains("\n  \"count\": 1\n"), "got: {s}");
}

#[test]
fn aggregate_plain_count_does_not_warn_about_resume() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.jsonl");
    let out = dir.path().join("counts.json");
    write_jsonl(&input, &[json!({"id":"a"})]);

    retl()
        .args([
            "aggregate",
            "--no-progress",
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
