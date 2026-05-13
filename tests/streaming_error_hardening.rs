#[path = "common/mod.rs"]
mod common;

use assert_cmd::Command;
use common::{make_corpus_basic, write_zst_lines};
use predicates::prelude::*;
use retl::{KeyExtractor, RedditETL, Sources, YearMonth};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
}

fn malformed_comment_corpus() -> (PathBuf, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.keep();
    let rc = base.join("comments").join("RC_2006-01.zst");
    let lines = vec![
        json!({
            "id": "c1",
            "author": "alice",
            "subreddit": "programming",
            "created_utc": 1136073600_i64,
            "score": 1,
            "body": "ok"
        })
        .to_string(),
        "{\"id\":\"bad\",\"author\":".to_string(),
        json!({
            "id": "c2",
            "author": "bob",
            "subreddit": "programming",
            "created_utc": 1136073601_i64,
            "score": 2,
            "body": "also ok"
        })
        .to_string(),
    ];
    write_zst_lines(&rc, &lines);
    (base, rc)
}

fn etl_for_bad_corpus(base: &Path) -> RedditETL {
    RedditETL::new()
        .base_dir(base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
}

fn assert_malformed(err: anyhow::Error, path: &Path, line: u64) {
    let msg = format!("{err:#}");
    assert!(msg.contains("malformed JSON"), "unexpected error: {msg}");
    assert!(
        msg.contains(&path.display().to_string()),
        "unexpected error: {msg}"
    );
    assert!(
        msg.contains(&format!("line {line}")),
        "unexpected error: {msg}"
    );
}

#[test]
fn count_by_month_fails_on_malformed_json_line() {
    let (base, rc) = malformed_comment_corpus();

    let err = etl_for_bad_corpus(&base)
        .scan()
        .count_by_month()
        .expect_err("malformed JSON must fail counts, not be skipped");

    assert_malformed(err, &rc, 2);
}

#[test]
fn extract_to_jsonl_fails_on_malformed_json_line_without_publishing() {
    let (base, rc) = malformed_comment_corpus();
    let out = base.join("out.jsonl");

    let err = etl_for_bad_corpus(&base)
        .scan()
        .extract_to_jsonl(&out)
        .expect_err("malformed JSON must fail extract, not be skipped");

    assert_malformed(err, &rc, 2);
    assert!(
        !out.exists(),
        "failed extract must not publish final output"
    );
}

#[test]
fn spool_fails_on_malformed_json_line_without_manifest_entry() {
    let (base, rc) = malformed_comment_corpus();
    let out_dir = base.join("spool");

    let err = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .resume(true)
        .scan()
        .extract_spool_monthly(&out_dir)
        .expect_err("malformed JSON must fail spool, not mark the month complete");

    assert_malformed(err, &rc, 2);
    assert!(!out_dir.join("part_RC_2006-01.jsonl").exists());
    let manifest = out_dir.join("_progress.json");
    if manifest.exists() {
        let text = fs::read_to_string(&manifest).unwrap();
        assert!(
            !text.contains("RC_2006-01"),
            "failed month was committed: {text}"
        );
    }
}

#[test]
fn dedupe_keys_to_lines_fails_on_malformed_json_line() {
    let (base, rc) = malformed_comment_corpus();
    let out = base.join("authors.txt");

    let err = etl_for_bad_corpus(&base)
        .scan()
        .dedupe_keys_to_lines(&KeyExtractor::author_lowercase_fast(), &out)
        .expect_err("malformed JSON must fail dedupe input scan");

    assert_malformed(err, &rc, 2);
    assert!(!out.exists(), "failed dedupe must not publish output");
}

#[test]
fn spool_publish_failure_is_fatal_and_does_not_commit_manifest() {
    let base = make_corpus_basic();
    let out_dir = base.join("spool_publish_failure");
    let final_part = out_dir.join("part_RC_2006-01.jsonl");
    fs::create_dir_all(&final_part).unwrap();

    let err = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .resume(true)
        .scan()
        .extract_spool_monthly(&out_dir)
        .expect_err("publish failure must be fatal, not a skipped month");

    let msg = format!("{err:#}");
    assert!(
        msg.contains("part_RC_2006-01.jsonl"),
        "unexpected error: {msg}"
    );
    assert!(
        final_part.is_dir(),
        "pre-existing directory should not be replaced by a partial file"
    );
    let manifest = out_dir.join("_progress.json");
    if manifest.exists() {
        let text = fs::read_to_string(&manifest).unwrap();
        assert!(
            !text.contains("RC_2006-01"),
            "failed publish was committed: {text}"
        );
    }
}

#[test]
fn cli_spool_publish_failure_exits_nonzero() {
    let base = make_corpus_basic();
    let cwd = tempfile::tempdir().unwrap();
    let out_dir = cwd.path().join("spool");
    fs::create_dir_all(out_dir.join("part_RC_2006-01.jsonl")).unwrap();

    retl()
        .arg("export")
        .arg("--data-dir")
        .arg(&base)
        .arg("--work-dir")
        .arg(cwd.path().join("work"))
        .args([
            "--source",
            "rc",
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--format",
            "spool",
            "--no-progress",
            "--resume",
            "--out",
        ])
        .arg(&out_dir)
        .assert()
        .failure()
        .stderr(predicate::str::contains("part_RC_2006-01.jsonl"));
}
