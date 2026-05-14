#[path = "common/mod.rs"]
mod common;

use assert_cmd::Command;
use common::{make_corpus_basic, read_jsonl_values, write_zst_lines};
use predicates::str::contains;
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Output;

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
}

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

#[test]
fn parents_cli_warns_when_unresolved_rate_exceeds_threshold_only() {
    let base = make_cross_month_parent_corpus();
    let spool = write_cross_month_spool(&base);

    let tight = run_parents_for_warning_case(&base, &spool, "0", "tight");
    assert!(tight.status.success(), "tight run failed: {tight:?}");
    let tight_output = command_output_text(&tight);
    assert!(
        tight_output.contains("more than 5%"),
        "expected unresolved-rate warning in command output, got: {tight_output}"
    );

    let wide = run_parents_for_warning_case(&base, &spool, "1", "wide");
    assert!(wide.status.success(), "wide run failed: {wide:?}");
    let wide_output = command_output_text(&wide);
    assert!(
        !wide_output.contains("more than 5%"),
        "did not expect unresolved-rate warning in command output, got: {wide_output}"
    );

    let wide_values = read_jsonl_values(&base.join("parents_out_wide/part_RC_2006-01.jsonl"));
    assert_eq!(
        wide_values[0]
            .pointer("/parent/title")
            .and_then(|v| v.as_str()),
        Some("December parent")
    );
}

fn make_cross_month_parent_corpus() -> PathBuf {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.keep();

    write_zst_lines(
        &base.join("submissions").join("RS_2005-12.zst"),
        &[json!({
            "author": "parent_author",
            "created_utc": 1133395200,
            "domain": "example.com",
            "id": "s_dec",
            "score": 10,
            "selftext": "parent selftext",
            "title": "December parent",
            "subreddit": "programming"
        })
        .to_string()],
    );

    write_zst_lines(
        &base.join("comments").join("RC_2006-01.zst"),
        &[json!({
            "author": "child_author",
            "body": "child body",
            "created_utc": 1136073600,
            "id": "c_jan",
            "link_id": "t3_s_dec",
            "parent_id": "t3_s_dec",
            "score": 1,
            "subreddit": "programming"
        })
        .to_string()],
    );

    base
}

fn write_cross_month_spool(base: &Path) -> PathBuf {
    let spool = base.join("spool_cross_month");
    fs::create_dir_all(&spool).unwrap();
    fs::write(
        spool.join("part_RC_2006-01.jsonl"),
        format!(
            "{}\n",
            json!({
                "author": "child_author",
                "body": "child body",
                "created_utc": 1136073600,
                "id": "c_jan",
                "link_id": "t3_s_dec",
                "parent_id": "t3_s_dec",
                "score": 1,
                "subreddit": "programming"
            })
        ),
    )
    .unwrap();
    spool
}

fn command_output_text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn run_parents_for_warning_case(
    base: &Path,
    spool: &Path,
    window_months: &str,
    suffix: &str,
) -> Output {
    let cache = base.join(format!("parents_cache_{suffix}"));
    let out = base.join(format!("parents_out_{suffix}"));
    let work = base.join(format!("work_parents_{suffix}"));

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
            work.to_str().unwrap(),
            "--window-months",
            window_months,
            "--no-progress",
        ])
        .output()
        .expect("parents command should run")
}
