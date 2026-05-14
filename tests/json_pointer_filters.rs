#[path = "common/mod.rs"]
mod common;

use assert_cmd::Command;
use common::{make_corpus_basic, read_jsonl_values};
use predicates::prelude::*;
use retl::{RedditETL, Sources, YearMonth};

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
}

#[test]
fn library_json_pointer_predicates_filter_full_records() {
    let base = make_corpus_basic();
    let counts = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .author("bob")
        .json_eq("/over_18", false)
        .json_number_gte("/num_comments", 10.0)
        .json_regex("/title", "(?i)rust")
        .count_by_month()
        .unwrap();

    assert_eq!(counts.get(&YearMonth::new(2006, 1)).copied(), Some(1));
}

#[test]
fn library_json_exists_matches_present_null_values() {
    let base = make_corpus_basic();
    let counts = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .json_exists("/distinguished")
        .count_by_month()
        .unwrap();

    // Three comments carry the field, but the default pseudo-user filter drops c3.
    assert_eq!(counts.get(&YearMonth::new(2006, 1)).copied(), Some(2));
}

#[test]
fn cli_json_pointer_predicates_export_records() {
    let base = make_corpus_basic();
    let cwd = tempfile::tempdir().unwrap();
    let out = cwd.path().join("filtered.jsonl");

    retl()
        .arg("export")
        .arg("--data-dir")
        .arg(&base)
        .args([
            "--source",
            "rs",
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--author",
            "bob",
            "--json",
            "/over_18=false",
            "--json",
            "/is_self=false",
            "--json",
            "/num_comments>=10",
            "--format",
            "jsonl",
            "--no-progress",
            "--out",
        ])
        .arg(&out)
        .assert()
        .success();

    let values = read_jsonl_values(&out);
    assert_eq!(values.len(), 1);
    assert_eq!(values[0]["id"], "s1");
}

#[test]
fn cli_json_pointer_validation_errors_name_bad_parts() {
    retl()
        .args(["count", "--json", "over_18=false", "--no-progress"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("pointer").and(predicate::str::contains("over_18")));

    retl()
        .args(["count", "--json", "/num_comments>=many", "--no-progress"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("value").and(predicate::str::contains("many")));

    retl()
        .args(["count", "--json", "/title~=[", "--no-progress"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("regex").and(predicate::str::contains("/title")));
}
