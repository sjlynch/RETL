use crate::common::cli::retl;
use crate::common::parents::command_output_text;
use crate::common::{make_corpus_basic, read_jsonl_values};
use predicates::str::contains;

#[test]
fn parents_cli_fails_fast_when_whitelisted_spool_lacks_parent_fields() {
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
        .failure()
        .stderr(contains("parent_id"))
        .stderr(contains("link_id"))
        .stderr(contains("--whitelist"))
        .stderr(contains("author"))
        .stderr(contains("id"));

    assert!(
        !cache.join("comments").exists(),
        "parents should fail before resolver cache shards are created"
    );
}

#[test]
fn parents_cli_attaches_when_whitelist_keeps_parent_ids_but_drops_body() {
    let base = make_corpus_basic();
    let spool = base.join("spool_parent_ids_no_body");
    let export_work = base.join("work_export_parent_ids_no_body");

    retl()
        .args([
            "export",
            "--data-dir",
            base.to_str().unwrap(),
            "--work-dir",
            export_work.to_str().unwrap(),
            "--source",
            "rc",
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--subreddit",
            "programming",
            "--include-deleted",
            "--whitelist",
            "author,parent_id,link_id,created_utc,id,score,subreddit",
            "--format",
            "spool",
            "--out",
            spool.to_str().unwrap(),
            "--no-progress",
        ])
        .assert()
        .success();

    let cache = base.join("parents_cache_parent_ids_no_body");
    let out = base.join("parents_out_parent_ids_no_body");
    let parents_work = base.join("work_parents_parent_ids_no_body");

    let output = retl()
        .env("RUST_LOG", "error")
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
        .output()
        .expect("parents command should run");
    assert!(output.status.success(), "parents run failed: {output:?}");
    let combined = command_output_text(&output);
    assert!(
        combined.contains("relaxed parent_id-based matching"),
        "expected relaxed-matching diagnostic, got: {combined}"
    );

    let values = read_jsonl_values(&out.join("part_RC_2006-01.jsonl"));
    let c1 = values
        .iter()
        .find(|v| v.get("id").and_then(|id| id.as_str()) == Some("c1"))
        .expect("c1 should be present");
    assert!(
        c1.get("body").is_none(),
        "the child comment body should remain stripped by the whitelist"
    );
    assert_eq!(
        c1.pointer("/parent/title").and_then(|v| v.as_str()),
        Some("Rust news")
    );

    let c2 = values
        .iter()
        .find(|v| v.get("id").and_then(|id| id.as_str()) == Some("c2"))
        .expect("c2 should be present");
    assert_eq!(
        c2.pointer("/parent/body").and_then(|v| v.as_str()),
        Some("I love Rust http://rust-lang.org")
    );
}

#[test]
fn parents_cli_parent_fields_attach_additional_context() {
    let base = make_corpus_basic();
    let spool = base.join("spool_parent_fields");
    let export_work = base.join("work_export_parent_fields");

    retl()
        .args([
            "export",
            "--data-dir",
            base.to_str().unwrap(),
            "--work-dir",
            export_work.to_str().unwrap(),
            "--source",
            "rc",
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--subreddit",
            "programming",
            "--include-deleted",
            "--format",
            "spool",
            "--out",
            spool.to_str().unwrap(),
            "--no-progress",
        ])
        .assert()
        .success();

    let cache = base.join("parents_cache_parent_fields");
    let out = base.join("parents_out_parent_fields");
    let parents_work = base.join("work_parents_parent_fields");

    retl()
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
            "--parent-fields",
            "author,body,domain,score,title,url",
            "--no-progress",
        ])
        .assert()
        .success();

    let values = read_jsonl_values(&out.join("part_RC_2006-01.jsonl"));
    let c1 = values
        .iter()
        .find(|v| v.get("id").and_then(|id| id.as_str()) == Some("c1"))
        .expect("c1 should be present");
    assert_eq!(
        c1.pointer("/parent/author").and_then(|v| v.as_str()),
        Some("bob")
    );
    assert_eq!(
        c1.pointer("/parent/title").and_then(|v| v.as_str()),
        Some("Rust news")
    );
    assert_eq!(
        c1.pointer("/parent/domain").and_then(|v| v.as_str()),
        Some("example.com")
    );
    assert_eq!(
        c1.pointer("/parent/score").and_then(|v| v.as_i64()),
        Some(183)
    );

    let c2 = values
        .iter()
        .find(|v| v.get("id").and_then(|id| id.as_str()) == Some("c2"))
        .expect("c2 should be present");
    assert_eq!(
        c2.pointer("/parent/author").and_then(|v| v.as_str()),
        Some("alice")
    );
    assert_eq!(
        c2.pointer("/parent/body").and_then(|v| v.as_str()),
        Some("I love Rust http://rust-lang.org")
    );
    assert_eq!(
        c2.pointer("/parent/score").and_then(|v| v.as_i64()),
        Some(2)
    );
}
