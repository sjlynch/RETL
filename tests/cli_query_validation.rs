use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
}

fn corrupt_corpus() -> tempfile::TempDir {
    let base = tempfile::tempdir().unwrap();
    fs::create_dir_all(base.path().join("comments")).unwrap();
    fs::create_dir_all(base.path().join("submissions")).unwrap();
    fs::write(
        base.path().join("comments").join("RC_2006-01.zst"),
        b"not a zstd frame",
    )
    .unwrap();
    base
}

fn base_scan_cmd(base: &tempfile::TempDir) -> Command {
    let mut cmd = retl();
    cmd.arg("scan")
        .arg("--data-dir")
        .arg(base.path())
        .arg("--work-dir")
        .arg(base.path().join("work"))
        .args(["--source", "rc", "--start", "2006-01", "--end", "2006-01"])
        .arg("--no-progress");
    cmd
}

fn assert_blank_filter_fails_before_scan(extra_args: &[&str], field: &str) {
    let base = corrupt_corpus();

    let mut cmd = base_scan_cmd(&base);
    cmd.args(extra_args)
        .env_remove("ETL_EXCLUDE_AUTHORS")
        .env_remove("ETL_EXCLUDE_AUTHORS_FILE")
        .assert()
        .failure()
        .stderr(
            predicate::str::contains(field)
                .and(predicate::str::contains("blank entries are not allowed"))
                .and(predicate::str::contains("zstd").not()),
        );
}

#[test]
fn cli_rejects_blank_subreddit_before_scanning() {
    assert_blank_filter_fails_before_scan(&["--subreddit", " "], "subreddits");
}

#[test]
fn cli_rejects_blank_id_before_scanning() {
    assert_blank_filter_fails_before_scan(&["--id", "t1_"], "ids_in");
}

#[test]
fn cli_rejects_blank_author_before_scanning() {
    assert_blank_filter_fails_before_scan(&["--author", ""], "authors_in");
}

#[test]
fn cli_rejects_blank_excluded_author_before_scanning() {
    assert_blank_filter_fails_before_scan(&["--exclude-author", "\t"], "authors_out");
}

#[test]
fn cli_rejects_blank_domain_before_scanning() {
    assert_blank_filter_fails_before_scan(&["--domain", " "], "domains_in");
}

#[test]
fn cli_rejects_blank_keyword_before_scanning() {
    assert_blank_filter_fails_before_scan(&["--keyword", ""], "keywords_any");
}

#[test]
fn cli_rejects_blank_keyword_all_before_scanning() {
    assert_blank_filter_fails_before_scan(&["--keyword-all", ""], "keywords_all");
}

#[test]
fn cli_rejects_blank_excluded_keyword_before_scanning() {
    assert_blank_filter_fails_before_scan(&["--exclude-keyword", ""], "keywords_exclude");
}

#[test]
fn cli_rejects_invalid_text_regex_before_scanning() {
    let base = corrupt_corpus();

    let mut cmd = base_scan_cmd(&base);
    cmd.args(["--text-regex", "["])
        .env_remove("ETL_EXCLUDE_AUTHORS")
        .env_remove("ETL_EXCLUDE_AUTHORS_FILE")
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("text_regex")
                .and(predicate::str::contains("invalid"))
                .and(predicate::str::contains("zstd").not()),
        );
}

#[test]
fn cli_rejects_blank_text_regex_before_scanning() {
    let base = corrupt_corpus();

    let mut cmd = base_scan_cmd(&base);
    cmd.args(["--text-regex", " "])
        .env_remove("ETL_EXCLUDE_AUTHORS")
        .env_remove("ETL_EXCLUDE_AUTHORS_FILE")
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("text_regex")
                .and(predicate::str::contains("blank regex patterns"))
                .and(predicate::str::contains("zstd").not()),
        );
}

#[test]
fn cli_exclude_common_bots_invalid_file_fails_before_scanning() {
    let base = corrupt_corpus();
    let excludes = base.path().join("bad_excludes.txt");
    fs::write(&excludes, b"\xff\n").unwrap();

    let mut cmd = base_scan_cmd(&base);
    cmd.arg("--exclude-common-bots")
        .env_remove("ETL_EXCLUDE_AUTHORS")
        .env("ETL_EXCLUDE_AUTHORS_FILE", &excludes)
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("ETL_EXCLUDE_AUTHORS_FILE")
                .and(predicate::str::contains("line 1"))
                .and(predicate::str::contains("zstd").not()),
        );
}
