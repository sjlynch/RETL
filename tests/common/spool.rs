#![allow(dead_code)]

use retl::{RedditETL, Sources, YearMonth};
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};

use super::write_zst_lines;

pub fn make_two_subreddit_comment_corpus() -> PathBuf {
    let base = tempfile::tempdir().unwrap().keep();
    let rc_path = base.join("comments").join("RC_2006-01.zst");
    let lines = vec![
        json!({
            "id":"p1", "author":"alice", "subreddit":"programming",
            "created_utc":1136073600_i64, "score":1, "body":"programming"
        })
        .to_string(),
        json!({
            "id":"r1", "author":"ferris", "subreddit":"rust",
            "created_utc":1136073601_i64, "score":1, "body":"rust"
        })
        .to_string(),
    ];
    write_zst_lines(&rc_path, &lines);
    fs::create_dir_all(base.join("submissions")).unwrap();
    base
}

pub fn write_marker_comment_corpus(base: &Path, marker: &str) {
    let rc_path = base.join("comments").join("RC_2006-01.zst");
    let lines = vec![json!({
        "id": marker,
        "author":"alice",
        "subreddit":"programming",
        "created_utc":1136073600_i64,
        "score":1,
        "body": marker,
    })
    .to_string()];
    write_zst_lines(&rc_path, &lines);
    fs::create_dir_all(base.join("submissions")).unwrap();
}

pub fn make_over18_submission_corpus() -> PathBuf {
    let base = tempfile::tempdir().unwrap().keep();
    let rs_path = base.join("submissions").join("RS_2006-01.zst");
    let lines = vec![
        json!({
            "id":"safe", "author":"alice", "subreddit":"programming",
            "created_utc":1136073600_i64, "score":1, "domain":"example.com",
            "title":"safe", "selftext":"", "over_18": false, "is_self": true,
            "num_comments": 2
        })
        .to_string(),
        json!({
            "id":"nsfw", "author":"alice", "subreddit":"programming",
            "created_utc":1136073601_i64, "score":1, "domain":"example.com",
            "title":"nsfw", "selftext":"", "over_18": true, "is_self": true,
            "num_comments": 3
        })
        .to_string(),
    ];
    write_zst_lines(&rs_path, &lines);
    fs::create_dir_all(base.join("comments")).unwrap();
    base
}

pub fn spool_run(base: &Path, out_dir: &Path, resume: bool) -> u64 {
    let (_parts, n) = RedditETL::new()
        .base_dir(base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 2)))
        .progress(false)
        .resume(resume)
        .scan()
        .subreddit("programming")
        .extract_spool_monthly(out_dir)
        .unwrap();
    n
}

pub fn spool_part_names(out_dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(out_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
        .filter(|name| {
            (name.starts_with("part_RC_") || name.starts_with("part_RS_"))
                && name.ends_with(".jsonl")
        })
        .collect();
    names.sort();
    names
}

pub fn read_progress_manifest(out_dir: &Path) -> Value {
    serde_json::from_slice(&fs::read(out_dir.join("_progress.json")).unwrap()).unwrap()
}

pub fn assert_manifest_months(out_dir: &Path, expected: &[&str]) {
    let manifest = read_progress_manifest(out_dir);
    let months = manifest["months"].as_object().unwrap();
    assert_eq!(months.len(), expected.len());
    for key in expected {
        assert!(months.contains_key(*key), "manifest missing key {key}");
    }
}
