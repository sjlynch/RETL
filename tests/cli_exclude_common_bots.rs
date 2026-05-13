#[path = "common/mod.rs"]
mod common;

use assert_cmd::Command;
use common::{make_corpus_basic, read_lines};
use std::fs;

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
}

fn dedupe_authors(base: &std::path::Path, out_name: &str) -> Command {
    let mut cmd = retl();
    cmd.arg("dedupe")
        .arg("--data-dir")
        .arg(base)
        .arg("--work-dir")
        .arg(base.join(format!("work_{out_name}")))
        .args([
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--subreddit",
            "programming",
            "--key",
            "author",
            "--no-progress",
            "--out",
        ])
        .arg(base.join(out_name))
        .env_remove("ETL_EXCLUDE_AUTHORS")
        .env_remove("ETL_EXCLUDE_AUTHORS_FILE");
    cmd
}

#[test]
fn cli_exclude_common_bots_excludes_default_bot_list() {
    let base = make_corpus_basic();
    let out = base.join("authors_no_bots.txt");

    dedupe_authors(&base, "authors_no_bots.txt")
        .arg("--exclude-common-bots")
        .assert()
        .success();

    assert_eq!(read_lines(&out), vec!["alice", "bob", "charlie"]);
}

#[test]
fn cli_exclude_common_bots_composes_with_exclude_author() {
    let base = make_corpus_basic();
    let out = base.join("authors_no_bots_or_alice.txt");

    dedupe_authors(&base, "authors_no_bots_or_alice.txt")
        .args(["--exclude-common-bots", "--exclude-author", "alice"])
        .assert()
        .success();

    assert_eq!(read_lines(&out), vec!["bob", "charlie"]);
}

#[test]
fn cli_exclude_common_bots_composes_with_env_and_file_augments() {
    let base = make_corpus_basic();
    let out = base.join("authors_all_excluded.txt");
    let excludes_file = base.join("more_excludes.txt");
    fs::write(&excludes_file, "bob\ncharlie\n").unwrap();

    dedupe_authors(&base, "authors_all_excluded.txt")
        .arg("--exclude-common-bots")
        .env("ETL_EXCLUDE_AUTHORS", "alice")
        .env("ETL_EXCLUDE_AUTHORS_FILE", &excludes_file)
        .assert()
        .success();

    assert!(read_lines(&out).is_empty());
}

#[test]
fn cli_include_deleted_stays_separate_from_common_bot_exclusion() {
    let base = make_corpus_basic();
    let out = base.join("authors_include_deleted_no_bots.txt");

    dedupe_authors(&base, "authors_include_deleted_no_bots.txt")
        .args(["--exclude-common-bots", "--include-deleted"])
        .assert()
        .success();

    assert_eq!(
        read_lines(&out),
        vec!["[deleted]", "alice", "bob", "charlie"]
    );
}
