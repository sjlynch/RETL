use crate::common::cli::{retl, write_jsonl};
use predicates::prelude::*;
use serde_json::json;
use std::fs;
use std::io::Write;

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
fn aggregate_strict_fails_on_any_fatal_input() {
    let dir = tempfile::tempdir().unwrap();
    let good_a = dir.path().join("good-a.jsonl");
    let good_b = dir.path().join("good-b.jsonl");
    let bad = dir.path().join("bad-input.jsonl");
    let out = dir.path().join("counts.json");
    write_jsonl(&good_a, &[json!({"id":"a"})]);
    write_jsonl(&good_b, &[json!({"id":"b"})]);
    fs::write(&bad, "not-json\n").unwrap();

    // The same 2-good/1-bad batch succeeds 2/3 without `--strict` (see
    // `aggregate_reports_each_failed_input_by_filename`). `--strict` turns the
    // single fatal input into a hard failure: non-zero exit, no output.
    retl()
        .args([
            "aggregate",
            "--strict",
            "--no-progress",
            "--out",
            out.to_str().unwrap(),
            good_a.to_str().unwrap(),
            bad.to_str().unwrap(),
            good_b.to_str().unwrap(),
        ])
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("bad-input.jsonl")
                .and(predicate::str::contains("aggregate --strict"))
                .and(predicate::str::contains(
                    "1 of 3 input(s) failed during shard build",
                )),
        );

    assert!(
        !out.exists(),
        "strict mode must not publish output when an input is fatal"
    );
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
