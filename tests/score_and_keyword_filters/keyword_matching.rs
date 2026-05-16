use crate::common::cli::retl;
use crate::common::{read_jsonl_values, write_zst_lines};
use retl::{RedditETL, Sources, YearMonth};
use serde_json::json;
use std::path::PathBuf;

fn corpus_with_keywords_in_submissions() -> PathBuf {
    let dir = tempfile::tempdir().unwrap().keep();
    let rs = dir.join("submissions").join("RS_2006-01.zst");
    let lines: Vec<String> = vec![
        // Match in title only.
        json!({
            "subreddit":"programming","author":"u_a","id":"s1",
            "domain":"example.com","title":"Rust news","selftext":"",
            "created_utc":1136074600_i64,"score":1_i64
        })
        .to_string(),
        // Match in selftext only.
        json!({
            "subreddit":"programming","author":"u_b","id":"s2",
            "domain":"example.com","title":"Hi","selftext":"discussing rust internals",
            "created_utc":1136074601_i64,"score":1_i64
        })
        .to_string(),
        // No match anywhere.
        json!({
            "subreddit":"programming","author":"u_c","id":"s3",
            "domain":"example.com","title":"unrelated","selftext":"about python",
            "created_utc":1136074602_i64,"score":1_i64
        })
        .to_string(),
    ];
    write_zst_lines(&rs, &lines);
    std::fs::create_dir_all(dir.join("comments")).unwrap();
    dir
}

#[test]
fn keywords_any_matches_title_field_on_submissions() {
    let base = corpus_with_keywords_in_submissions();
    let counts = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Submissions)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .keywords_any(["rust"])
        .count_by_month()
        .unwrap();
    // Both s1 (title) and s2 (selftext) match.
    assert_eq!(counts.get(&YearMonth::new(2006, 1)).copied(), Some(2));
}

#[test]
fn keywords_any_is_case_insensitive() {
    let base = corpus_with_keywords_in_submissions();
    // Search uppercase keyword — should still match "Rust news" / "rust internals".
    let counts = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Submissions)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .keywords_any(["RUST"])
        .count_by_month()
        .unwrap();
    assert_eq!(counts.get(&YearMonth::new(2006, 1)).copied(), Some(2));
}

fn corpus_with_unicode_keywords() -> PathBuf {
    let dir = tempfile::tempdir().unwrap().keep();
    let rc = dir.join("comments").join("RC_2006-01.zst");
    let rc_lines: Vec<String> = vec![
        json!({"subreddit":"programming","author":"u_upper","id":"c1","body":"CAFÉ meetup","parent_id":"t3_s1","created_utc":1136074600_i64,"score":1_i64}).to_string(),
        json!({"subreddit":"programming","author":"u_title","id":"c2","body":"Café review","parent_id":"t3_s1","created_utc":1136074601_i64,"score":1_i64}).to_string(),
        json!({"subreddit":"programming","author":"u_lower","id":"c3","body":"café notes","parent_id":"t3_s1","created_utc":1136074602_i64,"score":1_i64}).to_string(),
        json!({"subreddit":"programming","author":"u_none","id":"c4","body":"tea notes","parent_id":"t3_s1","created_utc":1136074603_i64,"score":1_i64}).to_string(),
    ];
    write_zst_lines(&rc, &rc_lines);

    let rs = dir.join("submissions").join("RS_2006-01.zst");
    let rs_lines: Vec<String> = vec![
        json!({
            "subreddit":"programming","author":"u_submit_title","id":"s1",
            "domain":"example.com","title":"Café launch","selftext":"",
            "created_utc":1136074700_i64,"score":1_i64
        })
        .to_string(),
        json!({
            "subreddit":"programming","author":"u_submit_selftext","id":"s2",
            "domain":"example.com","title":"hello","selftext":"CAFÉ details",
            "created_utc":1136074701_i64,"score":1_i64
        })
        .to_string(),
        json!({
            "subreddit":"programming","author":"u_submit_none","id":"s3",
            "domain":"example.com","title":"tea","selftext":"plain",
            "created_utc":1136074702_i64,"score":1_i64
        })
        .to_string(),
    ];
    write_zst_lines(&rs, &rs_lines);
    dir
}

#[test]
fn keywords_any_unicode_case_insensitive_matches_comments_and_submissions() {
    let base = corpus_with_unicode_keywords();
    let counts = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .keywords_any(["café"])
        .count_by_month()
        .unwrap();
    assert_eq!(counts.get(&YearMonth::new(2006, 1)).copied(), Some(5));
}

#[test]
fn cli_keyword_unicode_case_insensitive() {
    let base = corpus_with_unicode_keywords();
    let cwd = tempfile::tempdir().unwrap();
    let out = cwd.path().join("unicode_keyword.jsonl");

    retl()
        .arg("export")
        .arg("--data-dir")
        .arg(&base)
        .args([
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--subreddit",
            "programming",
            "--keyword",
            "café",
            "--format",
            "jsonl",
            "--no-progress",
            "--out",
        ])
        .arg(&out)
        .assert()
        .success();

    assert_eq!(read_jsonl_values(&out).len(), 5);
}

fn corpus_with_keyword_family_combinations() -> PathBuf {
    let dir = tempfile::tempdir().unwrap().keep();
    let rc = dir.join("comments").join("RC_2006-01.zst");
    let rc_lines: Vec<String> = vec![
        json!({"subreddit":"programming","author":"u_keep","id":"c1","body":"Rust async guide","parent_id":"t3_s1","created_utc":1136074600_i64,"score":1_i64}).to_string(),
        json!({"subreddit":"programming","author":"u_spam","id":"c2","body":"Rust async guide spam","parent_id":"t3_s1","created_utc":1136074601_i64,"score":1_i64}).to_string(),
        json!({"subreddit":"programming","author":"u_missing_all","id":"c3","body":"Rust async notes","parent_id":"t3_s1","created_utc":1136074602_i64,"score":1_i64}).to_string(),
        json!({"subreddit":"programming","author":"u_missing_any","id":"c4","body":"Go async guide","parent_id":"t3_s1","created_utc":1136074603_i64,"score":1_i64}).to_string(),
    ];
    write_zst_lines(&rc, &rc_lines);

    let rs = dir.join("submissions").join("RS_2006-01.zst");
    let rs_lines: Vec<String> = vec![json!({
        "subreddit":"programming","author":"u_submit_keep","id":"s1",
        "domain":"self.programming","is_self":true,
        "title":"Rust guide","selftext":"async details",
        "created_utc":1136074700_i64,"score":1_i64
    })
    .to_string()];
    write_zst_lines(&rs, &rs_lines);
    dir
}

#[test]
fn keywords_any_all_and_exclude_combine_across_text_fields() {
    let base = corpus_with_keyword_family_combinations();
    let counts = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .keywords_any(["rust", "python"])
        .keywords_all(["async", "guide"])
        .exclude_keywords(["spam"])
        .count_by_month()
        .unwrap();

    assert_eq!(counts.get(&YearMonth::new(2006, 1)).copied(), Some(2));
}

fn corpus_with_unicode_keyword_families() -> PathBuf {
    let dir = tempfile::tempdir().unwrap().keep();
    let rc = dir.join("comments").join("RC_2006-01.zst");
    let lines: Vec<String> = vec![
        json!({"subreddit":"programming","author":"u_excluded","id":"c1","body":"CAFÉ crème brûlée","parent_id":"t3_s1","created_utc":1136074600_i64,"score":1_i64}).to_string(),
        json!({"subreddit":"programming","author":"u_keep","id":"c2","body":"CAFÉ crème notes","parent_id":"t3_s1","created_utc":1136074601_i64,"score":1_i64}).to_string(),
        json!({"subreddit":"programming","author":"u_ascii","id":"c3","body":"cafe creme notes","parent_id":"t3_s1","created_utc":1136074602_i64,"score":1_i64}).to_string(),
    ];
    write_zst_lines(&rc, &lines);
    std::fs::create_dir_all(dir.join("submissions")).unwrap();
    dir
}

#[test]
fn keyword_all_and_exclude_use_unicode_case_folding() {
    let base = corpus_with_unicode_keyword_families();
    let counts = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .keywords_all(["café", "CRÈME"])
        .exclude_keywords(["BRÛLÉE"])
        .count_by_month()
        .unwrap();

    assert_eq!(counts.get(&YearMonth::new(2006, 1)).copied(), Some(1));
}

#[test]
fn text_regex_matches_body_selftext_or_title() {
    let base = corpus_with_keyword_family_combinations();
    let counts = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .text_regex(r"(?i)async (guide|details)")
        .count_by_month()
        .unwrap();

    assert_eq!(counts.get(&YearMonth::new(2006, 1)).copied(), Some(4));
}
