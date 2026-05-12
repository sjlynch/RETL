#[path = "common/mod.rs"]
mod common;

use assert_cmd::Command;
use common::{make_corpus_basic, read_lines};

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
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
