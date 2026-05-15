mod common;

use common::{read_jsonl_values, write_zst_lines};
use retl::{RedditETL, Sources, YearMonth};
use serde_json::json;
use std::fs;

fn jan_2006_scan(base: &std::path::Path) -> retl::ScanPlan {
    RedditETL::new()
        .base_dir(base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
}

#[test]
fn library_filters_single_bare_record_id() {
    let base = common::make_corpus_basic();
    let out = base.join("single_id.jsonl");

    jan_2006_scan(&base)
        .ids(["c2"])
        .extract_to_jsonl(&out)
        .unwrap();

    let values = read_jsonl_values(&out);
    assert_eq!(values.len(), 1);
    assert_eq!(values[0]["id"], "c2");
}

#[test]
fn library_ids_file_skips_blank_and_comment_lines() {
    let base = common::make_corpus_basic();
    let ids_file = base.join("ids.txt");
    fs::write(&ids_file, "# hydrate one submission\n\n  t3_s1  \n").unwrap();
    let out = base.join("ids_file.jsonl");

    jan_2006_scan(&base)
        .ids_file(&ids_file)
        .unwrap()
        .extract_to_jsonl(&out)
        .unwrap();

    let values = read_jsonl_values(&out);
    assert_eq!(values.len(), 1);
    assert_eq!(values[0]["id"], "s1");
}

#[test]
fn prefixed_comment_ids_constrain_sources_when_possible() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();
    write_zst_lines(
        &base.join("comments").join("RC_2006-01.zst"),
        &[json!({
            "id": "c1",
            "author": "alice",
            "subreddit": "programming",
            "created_utc": 1136073600_i64,
            "score": 1,
            "body": "comment"
        })
        .to_string()],
    );
    fs::create_dir_all(base.join("submissions")).unwrap();
    fs::write(base.join("submissions").join("RS_2006-01.zst"), b"not zstd").unwrap();

    let counts = jan_2006_scan(base).ids(["t1_c1"]).count_by_month().unwrap();

    assert_eq!(counts.get(&YearMonth::new(2006, 1)), Some(&1));
}

#[test]
fn prefixed_ids_are_source_specific_and_do_not_match_same_bare_id_in_other_source() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();
    write_zst_lines(
        &base.join("comments").join("RC_2006-01.zst"),
        &[json!({
            "id": "same",
            "author": "alice",
            "subreddit": "programming",
            "created_utc": 1136073600_i64,
            "score": 1,
            "body": "comment"
        })
        .to_string()],
    );
    write_zst_lines(
        &base.join("submissions").join("RS_2006-01.zst"),
        &[json!({
            "id": "same",
            "author": "bob",
            "subreddit": "programming",
            "created_utc": 1136073600_i64,
            "score": 1,
            "title": "submission",
            "selftext": "",
            "domain": "example.com"
        })
        .to_string()],
    );

    let out_both = base.join("both.jsonl");
    jan_2006_scan(base)
        .ids(["t1_same"])
        .extract_to_jsonl(&out_both)
        .unwrap();
    let both_values = read_jsonl_values(&out_both);
    assert_eq!(both_values.len(), 1);
    assert_eq!(both_values[0]["body"], "comment");

    let out_mismatch = base.join("mismatch.jsonl");
    RedditETL::new()
        .base_dir(base)
        .sources(Sources::Submissions)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .ids(["t1_same"])
        .extract_to_jsonl(&out_mismatch)
        .unwrap();
    assert!(read_jsonl_values(&out_mismatch).is_empty());
}

#[test]
fn large_record_id_sets_match_without_full_parse_filters() {
    let base = common::make_corpus_basic();
    let mut ids: Vec<String> = (0..20_000).map(|i| format!("missing_{i}")).collect();
    ids.push("c2".to_string());

    let counts = jan_2006_scan(&base).ids(&ids).count_by_month().unwrap();

    assert_eq!(counts.get(&YearMonth::new(2006, 1)), Some(&1));
}
