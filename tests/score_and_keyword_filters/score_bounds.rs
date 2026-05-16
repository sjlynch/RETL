use crate::common::write_zst_lines;
use retl::{RedditETL, Sources, YearMonth};
use serde_json::json;
use std::path::PathBuf;

fn corpus_with_scores() -> PathBuf {
    let dir = tempfile::tempdir().unwrap().keep();
    let rc = dir.join("comments").join("RC_2006-01.zst");
    let lines: Vec<String> = vec![
        json!({"subreddit":"programming","author":"u_neg","id":"c0","body":"x","parent_id":"t3_s1","created_utc":1136074600_i64,"score":-5_i64}).to_string(),
        json!({"subreddit":"programming","author":"u_zero","id":"c1","body":"x","parent_id":"t3_s1","created_utc":1136074601_i64,"score":0_i64}).to_string(),
        json!({"subreddit":"programming","author":"u_one","id":"c2","body":"x","parent_id":"t3_s1","created_utc":1136074602_i64,"score":1_i64}).to_string(),
        json!({"subreddit":"programming","author":"u_ten","id":"c3","body":"x","parent_id":"t3_s1","created_utc":1136074603_i64,"score":10_i64}).to_string(),
    ];
    write_zst_lines(&rc, &lines);
    std::fs::create_dir_all(dir.join("submissions")).unwrap();
    dir
}

#[test]
fn min_score_is_inclusive_at_boundary() {
    let base = corpus_with_scores();
    // min_score(0) keeps {0, 1, 10}; rejects {-5}.
    let counts = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .min_score(0)
        .count_by_month()
        .unwrap();
    assert_eq!(counts.get(&YearMonth::new(2006, 1)).copied(), Some(3));
}

#[test]
fn max_score_is_inclusive_at_boundary() {
    let base = corpus_with_scores();
    // max_score(0) keeps {-5, 0}; rejects {1, 10}.
    let counts = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .max_score(0)
        .count_by_month()
        .unwrap();
    assert_eq!(counts.get(&YearMonth::new(2006, 1)).copied(), Some(2));
}

#[test]
fn min_and_max_score_combine_as_a_closed_interval() {
    let base = corpus_with_scores();
    // [0, 1] keeps {0, 1}; rejects {-5, 10}.
    let counts = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .min_score(0)
        .max_score(1)
        .count_by_month()
        .unwrap();
    assert_eq!(counts.get(&YearMonth::new(2006, 1)).copied(), Some(2));
}
