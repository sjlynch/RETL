#[path = "common/mod.rs"]
mod common;

use assert_cmd::Command;
use common::{make_corpus_basic, read_lines};
use predicates::prelude::*;
use predicates::str::contains;
use retl::{KeyExtractor, RedditETL, YearMonth};

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
}

#[test]
fn dedupe_keys_to_lines_applies_author_regex() {
    let base = make_corpus_basic();
    let out = base.join("regex_authors.txt");
    let work = base.join("work_regex_authors");

    let n = RedditETL::new()
        .base_dir(&base)
        .work_dir(&work)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .author_regex("^a")
        .dedupe_keys_to_lines(&KeyExtractor::author_lowercase_fast(), &out)
        .unwrap();

    assert_eq!(n, 1);
    assert_eq!(read_lines(&out), vec!["alice"]);
}

#[test]
fn dedupe_keys_to_lines_rejects_invalid_author_regex() {
    let base = make_corpus_basic();
    let out = base.join("invalid_regex.txt");
    let work = base.join("work_invalid_regex");

    let err = RedditETL::new()
        .base_dir(&base)
        .work_dir(&work)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .author_regex("[")
        .dedupe_keys_to_lines(&KeyExtractor::author_lowercase_fast(), &out)
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("author_regex"), "{msg}");
}

#[test]
fn dedupe_keys_to_lines_rejects_contradictory_score_range() {
    let base = tempfile::tempdir().unwrap().keep();
    let out = base.join("bad_scores.txt");
    let work = base.join("work_bad_scores");

    let err = RedditETL::new()
        .base_dir(&base)
        .work_dir(&work)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .min_score(10)
        .max_score(1)
        .dedupe_keys_to_lines(&KeyExtractor::author_lowercase_fast(), &out)
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("min_score"), "{msg}");
    assert!(msg.contains("max_score"), "{msg}");
}

#[test]
fn dedupe_cli_emits_unique_authors() {
    let base = make_corpus_basic();
    let out = base.join("authors.txt");
    let work = base.join("work");

    retl()
        .args([
            "dedupe",
            "--data-dir",
            base.to_str().unwrap(),
            "--work-dir",
            work.to_str().unwrap(),
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--subreddit",
            "programming",
            "--key",
            "author",
            "--out",
            out.to_str().unwrap(),
            "--no-progress",
        ])
        .assert()
        .success();

    assert_eq!(
        read_lines(&out),
        vec!["alice", "automoderator", "bob", "charlie"]
    );
}

#[test]
fn dedupe_cli_accepts_json_pointer_keys() {
    let base = make_corpus_basic();
    let out = base.join("parents.txt");
    let work = base.join("work_json_pointer");

    retl()
        .args([
            "dedupe",
            "--data-dir",
            base.to_str().unwrap(),
            "--work-dir",
            work.to_str().unwrap(),
            "--source",
            "rc",
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--subreddit",
            "programming",
            "--key",
            "json:/parent_id",
            "--out",
            out.to_str().unwrap(),
            "--no-progress",
        ])
        .assert()
        .success();

    assert_eq!(read_lines(&out), vec!["t1_c1", "t3_s1"]);
}

#[test]
fn dedupe_cli_warns_and_summarizes_records_dropped_without_key() {
    let base = make_corpus_basic();
    let out = base.join("parents_both.txt");
    let work = base.join("work_json_pointer_both");

    retl()
        .env("RUST_LOG", "warn")
        .args([
            "dedupe",
            "--data-dir",
            base.to_str().unwrap(),
            "--work-dir",
            work.to_str().unwrap(),
            "--source",
            "both",
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--subreddit",
            "programming",
            "--key",
            "json:/parent_id",
            "--out",
            out.to_str().unwrap(),
            "--no-progress",
        ])
        .assert()
        .success()
        .stdout(
            contains("WARN")
                .and(contains("dedupe dropped 2 matching record(s) without an extractable key")),
        )
        .stderr(contains(
            "Deduped 2 unique keys from 4 matching records (2 dropped without key; 50.00% drop rate)",
        ));

    assert_eq!(read_lines(&out), vec!["t1_c1", "t3_s1"]);
}

#[test]
fn dedupe_cli_strict_key_errors_when_any_matched_record_lacks_key() {
    let base = make_corpus_basic();
    let out = base.join("parents_strict.txt");
    let work = base.join("work_json_pointer_strict");

    retl()
        .env("RUST_LOG", "warn")
        .args([
            "dedupe",
            "--data-dir",
            base.to_str().unwrap(),
            "--work-dir",
            work.to_str().unwrap(),
            "--source",
            "both",
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--subreddit",
            "programming",
            "--key",
            "json:/parent_id",
            "--out",
            out.to_str().unwrap(),
            "--strict-key",
            "--no-progress",
        ])
        .assert()
        .failure()
        .stderr(contains("--strict-key").and(contains(
            "2 of 4 matching record(s) did not contain extractable key json:/parent_id",
        )));

    assert!(
        !out.exists(),
        "strict-key should fail before publishing output"
    );
}
