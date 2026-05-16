use crate::common::cli::{retl, write_jsonl};
use predicates::prelude::*;
use serde_json::json;
use std::fs;
use std::process::{Command as StdCommand, Stdio};

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
