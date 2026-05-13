#[path = "common/mod.rs"]
mod common;

use assert_cmd::Command;
use common::{decompress_zst_lines, make_corpus_basic, read_jsonl_values, read_lines};
use predicates::prelude::*;
use std::collections::BTreeSet;

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
}

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

#[test]
fn cli_author_allow_and_deny_filters_dedupe() {
    let base = make_corpus_basic();
    let cwd = tempfile::tempdir().unwrap();
    let work_dir = cwd.path().join("work");
    let out = cwd.path().join("authors.txt");

    retl()
        .arg("dedupe")
        .arg("--data-dir")
        .arg(&base)
        .arg("--work-dir")
        .arg(&work_dir)
        .args([
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--author",
            "alice",
            "--author-in",
            "bob",
            "--exclude-author",
            "AutoModerator",
            "--key",
            "author",
            "--no-progress",
            "--out",
        ])
        .arg(&out)
        .assert()
        .success();

    assert_eq!(read_lines(&out), vec!["alice", "bob"]);
}

#[test]
fn cli_invalid_author_regex_errors_before_scan() {
    let base = make_corpus_basic();
    let cwd = tempfile::tempdir().unwrap();
    let work_dir = cwd.path().join("work");

    retl()
        .arg("count")
        .arg("--data-dir")
        .arg(&base)
        .arg("--work-dir")
        .arg(&work_dir)
        .args(["--author-regex", "[", "--no-progress"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("author_regex"));
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

#[test]
fn count_rejects_export_only_whitelist_flag() {
    retl()
        .args(["count", "--whitelist", "author"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--whitelist"));
}

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
fn export_format_zst_writes_partitioned_decodable_corpus() {
    let base = make_corpus_basic();
    let cwd = tempfile::tempdir().unwrap();
    let work_dir = cwd.path().join("work");
    let out_dir = cwd.path().join("mini_corpus");

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
            "--subreddit",
            "programming",
            "--include-deleted",
            "--format",
            "zst",
            "--zst-level",
            "1",
            "--whitelist",
            "id,author,created_utc,subreddit",
            "--human-timestamps",
            "--no-progress",
            "--out",
        ])
        .arg(&out_dir)
        .assert()
        .success();

    let rc = out_dir.join("comments").join("RC_2006-01.zst");
    assert!(rc.exists(), "RC zst not found at {}", rc.display());

    let lines = decompress_zst_lines(&rc);
    assert_eq!(lines.len(), 3, "comments (RC) should have 3 lines");

    let allowed: BTreeSet<&str> = ["id", "author", "created_utc", "subreddit"]
        .into_iter()
        .collect();
    for line in lines {
        let value: serde_json::Value = serde_json::from_str(&line).unwrap();
        let obj = value.as_object().expect("exported records are objects");
        assert!(
            obj.keys().all(|k| allowed.contains(k.as_str())),
            "non-whitelisted key leaked into {obj:?}"
        );
        assert!(
            obj.get("created_utc").is_some_and(|v| v.is_string()),
            "created_utc should be human-readable in {obj:?}"
        );
    }
}
