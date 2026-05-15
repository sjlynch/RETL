#[path = "common/mod.rs"]
mod common;

use common::make_corpus_multi_month;
use retl::{
    discover_upstream_manifests_from_inputs, manifest_path_for_directory, manifest_path_for_file,
    RedditETL, Sources, YearMonth,
};
use serde_json::Value;
use std::fs;
use std::path::Path;

fn read_json(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).expect("read manifest")).expect("parse manifest")
}

fn manifest_fingerprint_for_output(path: &Path) -> String {
    read_json(&manifest_path_for_file(path))["manifest_fingerprint"]
        .as_str()
        .expect("manifest_fingerprint")
        .to_string()
}

fn base_scan(base: &Path) -> retl::ScanPlan {
    RedditETL::new()
        .base_dir(base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 2)))
        .progress(false)
        .scan()
        .subreddit("programming")
}

#[test]
fn manifest_fingerprint_changes_with_query_whitelist_and_date_range() {
    let base = make_corpus_multi_month(&[YearMonth::new(2006, 1), YearMonth::new(2006, 2)]);

    let baseline = base.join("baseline.jsonl");
    base_scan(&base).extract_to_jsonl(&baseline).unwrap();
    let baseline_fp = manifest_fingerprint_for_output(&baseline);

    let author_filtered = base.join("author.jsonl");
    base_scan(&base)
        .authors_in(["commenter_2006-01"])
        .extract_to_jsonl(&author_filtered)
        .unwrap();
    let author_fp = manifest_fingerprint_for_output(&author_filtered);
    assert_ne!(
        baseline_fp, author_fp,
        "query changes must alter manifest fingerprint"
    );

    let whitelist = base.join("whitelist.jsonl");
    base_scan(&base)
        .whitelist_fields(["id", "author", "created_utc"])
        .extract_to_jsonl(&whitelist)
        .unwrap();
    let whitelist_fp = manifest_fingerprint_for_output(&whitelist);
    assert_ne!(
        baseline_fp, whitelist_fp,
        "whitelist changes must alter manifest fingerprint"
    );

    let date_limited = base.join("date.jsonl");
    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .extract_to_jsonl(&date_limited)
        .unwrap();
    let date_fp = manifest_fingerprint_for_output(&date_limited);
    assert_ne!(
        baseline_fp, date_fp,
        "date range changes must alter manifest fingerprint"
    );

    let manifest = read_json(&manifest_path_for_file(&baseline));
    assert_eq!(manifest["generated_by"]["name"], "retl");
    assert_eq!(manifest["output"]["format"], "jsonl");
    assert!(
        manifest["corpus"]["selected_files"]
            .as_array()
            .unwrap()
            .len()
            >= 2
    );
    assert_eq!(manifest["counts"]["records_written"].as_u64(), Some(8));
}

#[test]
fn directory_spool_manifest_is_discoverable_from_downstream_inputs() {
    let base = make_corpus_multi_month(&[YearMonth::new(2006, 1)]);
    let spool = base.join("spool");

    let (parts, written) = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .extract_spool_monthly(&spool)
        .unwrap();

    assert_eq!(written, 4);
    let manifest_path = manifest_path_for_directory(&spool);
    assert!(
        manifest_path.exists(),
        "spool manifest missing at {}",
        manifest_path.display()
    );

    let manifest = read_json(&manifest_path);
    assert_eq!(manifest["operation"], "scan.extract_spool_monthly");
    assert_eq!(manifest["output"]["kind"], "directory");
    assert_eq!(
        manifest["counts"]["part_files"].as_u64(),
        Some(parts.len() as u64)
    );

    let upstream = discover_upstream_manifests_from_inputs(&parts);
    assert_eq!(
        upstream.len(),
        1,
        "downstream spool inputs should discover one manifest"
    );
    assert_eq!(
        upstream[0].path,
        manifest["output"]["manifest_path"].as_str().unwrap()
    );
    assert_eq!(
        upstream[0].fingerprint.as_deref(),
        manifest["manifest_fingerprint"].as_str()
    );
}
