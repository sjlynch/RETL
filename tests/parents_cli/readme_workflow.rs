use crate::common::cli::retl;
use crate::common::{make_corpus_basic, read_jsonl_values, read_lines};

#[test]
fn parents_cli_readme_spool_attach_then_aggregate_workflow() {
    let base = make_corpus_basic();
    let spool = base.join("readme_spool");
    let attached = base.join("readme_spool_with_parents");
    let cache = base.join("readme_parents_cache");
    let export_work = base.join("readme_work_export");
    let parents_work = base.join("readme_work_parents");
    let counts = base.join("readme_parent_counts.tsv");

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
            "body,parent_id,link_id,created_utc,id,score,subreddit",
            "--format",
            "spool",
            "--out",
            spool.to_str().unwrap(),
            "--no-progress",
        ])
        .assert()
        .success();

    retl()
        .args([
            "parents",
            "--spool",
            spool.to_str().unwrap(),
            "--cache",
            cache.to_str().unwrap(),
            "--out",
            attached.to_str().unwrap(),
            "--data-dir",
            base.to_str().unwrap(),
            "--work-dir",
            parents_work.to_str().unwrap(),
            "--window-months",
            "0",
            "--no-progress",
        ])
        .assert()
        .success();

    retl()
        .args([
            "aggregate",
            "--spool",
            attached.to_str().unwrap(),
            "--by",
            "subreddit",
            "--out",
            counts.to_str().unwrap(),
            "--no-progress",
        ])
        .assert()
        .success();

    assert_eq!(read_lines(&counts), vec!["programming\t3"]);
    let attached_values = read_jsonl_values(&attached.join("part_RC_2006-01.jsonl"));
    assert!(attached_values
        .iter()
        .any(|v| v.pointer("/parent/title").and_then(|v| v.as_str()) == Some("Rust news")));
}
