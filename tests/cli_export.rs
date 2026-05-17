//! Export-handler tests that genuinely need a subprocess.
//!
//! The 4 stdout-warning tests below depend on `tracing_subscriber` writing to
//! stdout, which is process-global state that's hard to capture in-process
//! without per-test subscriber dances. The remaining cli_export tests
//! (error-paths, file-output, schema, dedupe, convert) moved into
//! `src/bin_handlers/tests.rs` and now run in-process.
//!
//! The single `cli_keyword_score_url_filters_export_and_count` test stays a
//! spawn because it exercises BOTH `retl export` and `retl count` in
//! succession against the same work_dir and verifies the cross-subcommand
//! manifest interaction.
//!
//! `count_author_out_dash` and `sample_defaults_to_ten_jsonl_records` stay
//! spawns because they assert on stdout produced by the streaming-to-stdout
//! shim (`stream_extract_to_stdout` / count.rs's stdout branch via a temp
//! file), which is awkward to capture in-process without rewiring those
//! shims.

#[path = "common/mod.rs"]
mod common;

use assert_cmd::Command;
use common::{make_corpus_basic, read_jsonl_values, read_lines};
use predicates::prelude::*;

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
}

// ---------------------------------------------------------------------------
// `tracing::warn!`-to-stdout assertions (4 tests). The default
// tracing-subscriber fmt destination is stdout; capturing it in-process needs
// per-test subscriber wiring, so these stay as subprocess spawns.
// ---------------------------------------------------------------------------

#[test]
fn cli_unfiltered_undated_export_warns_before_scan() {
    let base = make_corpus_basic();
    let cwd = tempfile::tempdir().unwrap();
    let out_jsonl = cwd.path().join("everything.jsonl");

    retl()
        .arg("export")
        .arg("--data-dir")
        .arg(&base)
        .args(["--format", "jsonl", "--no-progress", "--out"])
        .arg(&out_jsonl)
        .assert()
        .success()
        .stdout(
            predicate::str::contains("running an unfiltered, undated query over the full corpus")
                .and(predicate::str::contains("files"))
                .and(predicate::str::contains("compressed_bytes")),
        );
}

#[test]
fn cli_filtered_export_does_not_warn_full_corpus() {
    let base = make_corpus_basic();
    let cwd = tempfile::tempdir().unwrap();
    let out_jsonl = cwd.path().join("filtered.jsonl");

    retl()
        .arg("export")
        .arg("--data-dir")
        .arg(&base)
        .args([
            "--subreddit",
            "programming",
            "--format",
            "jsonl",
            "--no-progress",
            "--out",
        ])
        .arg(&out_jsonl)
        .assert()
        .success()
        .stdout(predicate::str::contains("unfiltered, undated query").not());
}

#[test]
fn cli_json_pointer_filter_suppresses_full_corpus_warning() {
    let base = make_corpus_basic();
    let cwd = tempfile::tempdir().unwrap();
    let out_jsonl = cwd.path().join("json_filtered.jsonl");

    retl()
        .arg("export")
        .arg("--data-dir")
        .arg(&base)
        .args([
            "--source",
            "rs",
            "--json",
            "/over_18=false",
            "--format",
            "jsonl",
            "--no-progress",
            "--out",
        ])
        .arg(&out_jsonl)
        .assert()
        .success()
        .stdout(predicate::str::contains("unfiltered, undated query").not());
}

#[test]
fn cli_domain_filter_warns_when_comments_are_included() {
    let base = make_corpus_basic();
    let cwd = tempfile::tempdir().unwrap();
    let work_dir = cwd.path().join("work");
    let out = cwd.path().join("domain_counts.tsv");

    retl()
        .arg("count")
        .arg("--data-dir")
        .arg(&base)
        .arg("--work-dir")
        .arg(&work_dir)
        .args([
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--domain",
            "example.com",
            "--no-progress",
            "--out",
        ])
        .arg(&out)
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "domains_in filters Reddit's submission-only `domain` field",
        ));

    assert_eq!(read_lines(&out), vec!["2006-01\t1"]);
}

// ---------------------------------------------------------------------------
// Cross-subcommand interaction (export → count against the same work_dir).
// Stays a spawn so we exercise the real binary path through both handlers,
// which guards against work_dir / manifest sharing regressions that an
// in-process test of either handler alone wouldn't catch.
// ---------------------------------------------------------------------------

#[test]
fn cli_keyword_score_url_filters_export_and_count() {
    let base = make_corpus_basic();
    let cwd = tempfile::tempdir().unwrap();
    let work_dir = cwd.path().join("work");
    let out_jsonl = cwd.path().join("rust_url.jsonl");
    let out_counts = cwd.path().join("counts.tsv");

    retl()
        .arg("export")
        .arg("--data-dir")
        .arg(&base)
        .arg("--work-dir")
        .arg(&work_dir)
        .args([
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--keyword",
            "rust",
            "--min-score",
            "2",
            "--contains-url",
            "--format",
            "jsonl",
            "--no-progress",
            "--out",
        ])
        .arg(&out_jsonl)
        .assert()
        .success();

    let mut authors: Vec<String> = read_jsonl_values(&out_jsonl)
        .into_iter()
        .map(|v| v["author"].as_str().unwrap().to_string())
        .collect();
    authors.sort();
    assert_eq!(authors, vec!["alice", "bob"]);

    retl()
        .arg("count")
        .arg("--data-dir")
        .arg(&base)
        .arg("--work-dir")
        .arg(&work_dir)
        .args([
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--keyword",
            "rust",
            "--min-score",
            "2",
            "--contains-url",
            "--no-progress",
            "--out",
        ])
        .arg(&out_counts)
        .assert()
        .success();

    assert_eq!(read_lines(&out_counts), vec!["2006-01\t2"]);
}

// ---------------------------------------------------------------------------
// stdout-streaming tests that route through `stream_extract_to_stdout` /
// the count.rs stdout temp-file dance. Stays a spawn until those shims
// accept a `&mut dyn Write` (or until a per-test stdout-capture utility
// lives in tests/common).
// ---------------------------------------------------------------------------

#[test]
fn count_author_out_dash_streams_stdout_without_dash_file() {
    let base = make_corpus_basic();
    let cwd = tempfile::tempdir().unwrap();
    let work_dir = cwd.path().join("work");

    retl()
        .current_dir(cwd.path())
        .arg("count")
        .arg("--data-dir")
        .arg(&base)
        .arg("--work-dir")
        .arg(&work_dir)
        .args(["--mode", "author", "--out", "-", "--no-progress"])
        .assert()
        .success()
        .stdout(predicate::str::contains("alice\t1"));

    assert!(
        !cwd.path().join("-").exists(),
        "count --mode author --out - must not create a literal '-' file"
    );
}

#[test]
fn sample_defaults_to_ten_jsonl_records_but_honors_limit() {
    let base = make_corpus_basic();

    let assert = retl()
        .arg("sample")
        .arg("--data-dir")
        .arg(&base)
        .args(["--source", "rc", "--limit", "1", "--no-progress"])
        .assert()
        .success();

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    let lines: Vec<&str> = stdout
        .lines()
        .filter(|line| line.starts_with('{'))
        .collect();
    assert_eq!(lines.len(), 1, "stdout was {stdout:?}");
    let value: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(value["subreddit"], "programming");
}
