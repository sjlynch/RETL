#[path = "common/mod.rs"]
mod common;

use assert_cmd::Command;
use common::{decompress_zst_lines, make_corpus_basic};
use predicates::prelude::*;
use std::collections::BTreeSet;

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
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
