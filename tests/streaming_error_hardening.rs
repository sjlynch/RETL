#[path = "common/mod.rs"]
mod common;

use assert_cmd::Command;
use common::{make_corpus_basic, make_truncated_zst, read_jsonl_values, write_zst_lines};
use predicates::prelude::*;
use retl::{
    project_whitelist_line_for_tests, ExportFormat, KeyExtractor, RedditETL, Sources, YearMonth,
};
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
fn whitelist_slow_path_malformed_json_reports_path_and_line() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("RC_2006-01.zst");
    let fields = vec!["id".to_string()];

    let projected =
        project_whitelist_line_for_tests(r#"[{"id":"not-a-top-level-object"}]"#, &fields, &path, 1)
            .expect("top-level non-object forces the Value slow path but is valid JSON");
    assert_eq!(projected, "{}");

    let err = project_whitelist_line_for_tests(r#"{"id":"bad","author":}"#, &fields, &path, 2)
        .expect_err("malformed slow-path JSON must carry file/line context");
    assert_malformed(err, &path, 2);
}

#[test]
fn minimal_record_type_drift_in_unused_fields_does_not_drop_record() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.keep();
    let rc = base.join("comments").join("RC_2006-01.zst");
    let line = r#"{"id":"drift","author":"alice","subreddit":"programming","score":1.5,"created_utc":{"unexpected":true},"body":"kept"}"#;
    write_zst_lines(&rc, &[line.to_string()]);
    fs::create_dir_all(base.join("submissions")).unwrap();

    let out_dir = base.join("partitioned");
    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .progress(false)
        .scan()
        .subreddit("programming")
        .include_pseudo_users()
        .export_partitioned(&out_dir, ExportFormat::Jsonl)
        .expect("unused score/created_utc type drift must not drop valid JSON records");

    let out = out_dir.join("comments").join("RC_2006-01.jsonl");
    let values = read_jsonl_values(&out);
    assert_eq!(values.len(), 1);
    assert_eq!(values[0]["id"], "drift");
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
fn cli_allow_partial_count_reports_skipped_file_json() {
    let base = tempfile::tempdir().unwrap().keep();
    let rc = base.join("comments").join("RC_2006-03.zst");
    make_truncated_zst(&rc, 500, 256);
    fs::create_dir_all(base.join("submissions")).unwrap();

    retl()
        .arg("count")
        .arg("--data-dir")
        .arg(&base)
        .arg("--work-dir")
        .arg(base.join("work"))
        .args([
            "--source",
            "rc",
            "--start",
            "2006-03",
            "--end",
            "2006-03",
            "--subreddit",
            "programming",
            "--include-deleted",
            "--no-progress",
            "--allow-partial",
        ])
        .assert()
        .success()
        .stderr(
            predicate::str::contains("skipped_file_count")
                .and(predicate::str::contains("RC_2006-03.zst")),
        );
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

    // The blocker is a directory at the publish destination, which makes
    // `rename` return ERROR_ACCESS_DENIED — correctly classified retriable
    // (for production AV / sharing hiccups) but means the test waits out
    // ~10–20 s of retry budget here for no extra signal. Under
    // `--features test-utils` we cap the budget to 1 try / 0 ms delay so
    // the failure surfaces immediately; the invariants below ("publish
    // failure is fatal", "manifest is not committed") are independent of
    // how long the failure took. Without the feature the test still runs
    // and passes, just slowly.
    #[cfg(feature = "test-utils")]
    let _backoff_cap = retl::cap_backoff_budget_for_test(1, 0);

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

// Former `cli_spool_publish_failure_exits_nonzero` moved to
// `src/bin_handlers/tests.rs::export_spool_publish_failure_surfaces_helpful_error`
// — calling `run_export` in-process exercises the same handler logic and
// asserts on the returned `anyhow::Error`'s Display, which is what would
// have been written to stderr and surfaced as a non-zero exit. The
// in-process version uses `cap_backoff_budget_for_test` so the failure
// surfaces in microseconds instead of ~21 s of retry-budget exhaustion.
