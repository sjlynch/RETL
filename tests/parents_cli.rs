#[path = "common/mod.rs"]
mod common;

use assert_cmd::Command;
use common::make_corpus_basic;
use predicates::str::contains;

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
}

#[test]
fn parents_cli_warns_when_whitelisted_spool_lacks_parent_fields() {
    let base = make_corpus_basic();
    let spool = base.join("spool_narrow");
    let export_work = base.join("work_export");

    retl()
        .env("RUST_LOG", "warn")
        .args([
            "export",
            "--data-dir",
            base.to_str().unwrap(),
            "--work-dir",
            export_work.to_str().unwrap(),
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--subreddit",
            "programming",
            "--include-deleted",
            "--whitelist",
            "author,id",
            "--format",
            "spool",
            "--out",
            spool.to_str().unwrap(),
            "--no-progress",
        ])
        .assert()
        .success();

    let cache = base.join("parents_cache_narrow");
    let out = base.join("parents_out_narrow");
    let parents_work = base.join("work_parents");

    retl()
        .env("RUST_LOG", "warn")
        .args([
            "parents",
            "--spool",
            spool.to_str().unwrap(),
            "--cache",
            cache.to_str().unwrap(),
            "--out",
            out.to_str().unwrap(),
            "--data-dir",
            base.to_str().unwrap(),
            "--work-dir",
            parents_work.to_str().unwrap(),
            "--window-months",
            "0",
            "--no-progress",
        ])
        .assert()
        .success()
        .stdout(contains("zero comment-shaped records"))
        .stdout(contains("body,parent_id,link_id"));
}
