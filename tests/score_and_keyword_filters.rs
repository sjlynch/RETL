//! Behavioral coverage for filter branches that the existing tests touch only
//! superficially:
//!   - `min_score` / `max_score` boundary inclusivity
//!   - `keywords_any` matching in `selftext` and `title` (not just `body`)
//!   - `contains_url` matching `https://` (not only `http://`)
//!
//! All of these are specified by `src/filters.rs::matches_minimal` but the
//! basic corpus only ever exercises body-keyword + http://, leaving the
//! other branches uncovered.

#[path = "common/mod.rs"]
mod common;

use common::*;
use retl::{RedditETL, Sources, YearMonth};
use serde_json::json;
use std::path::PathBuf;

/// Build a corpus of 4 RC records with controlled author/score values so
/// boundary checks land exactly where we want them.
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

/// Keyword search must hit `selftext` and `title` on submission records, not
/// only `body` on comments.
fn corpus_with_keywords_in_submissions() -> PathBuf {
    let dir = tempfile::tempdir().unwrap().keep();
    let rs = dir.join("submissions").join("RS_2006-01.zst");
    let lines: Vec<String> = vec![
        // Match in title only.
        json!({
            "subreddit":"programming","author":"u_a","id":"s1",
            "domain":"example.com","title":"Rust news","selftext":"",
            "created_utc":1136074600_i64,"score":1_i64
        }).to_string(),
        // Match in selftext only.
        json!({
            "subreddit":"programming","author":"u_b","id":"s2",
            "domain":"example.com","title":"Hi","selftext":"discussing rust internals",
            "created_utc":1136074601_i64,"score":1_i64
        }).to_string(),
        // No match anywhere.
        json!({
            "subreddit":"programming","author":"u_c","id":"s3",
            "domain":"example.com","title":"unrelated","selftext":"about python",
            "created_utc":1136074602_i64,"score":1_i64
        }).to_string(),
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

/// `contains_url` should match `https://` as well, not only `http://`.
/// `ascii_ci_contains_http` looks for the literal "http" sequence so https
/// is naturally a superset, but we lock that in with an explicit test.
fn corpus_with_https_only() -> PathBuf {
    let dir = tempfile::tempdir().unwrap().keep();
    let rc = dir.join("comments").join("RC_2006-01.zst");
    let lines: Vec<String> = vec![
        json!({"subreddit":"programming","author":"u_https","id":"c1","body":"see HTTPS://example.com today","parent_id":"t3_s1","created_utc":1136074600_i64,"score":1_i64}).to_string(),
        json!({"subreddit":"programming","author":"u_none","id":"c2","body":"nothing here","parent_id":"t3_s1","created_utc":1136074601_i64,"score":1_i64}).to_string(),
    ];
    write_zst_lines(&rc, &lines);
    std::fs::create_dir_all(dir.join("submissions")).unwrap();
    dir
}

#[test]
fn contains_url_true_matches_https_uppercase_in_body() {
    let base = corpus_with_https_only();
    let counts = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .contains_url(true)
        .count_by_month()
        .unwrap();
    assert_eq!(
        counts.get(&YearMonth::new(2006, 1)).copied(),
        Some(1),
        "HTTPS://... should be matched by contains_url(true) (case-insensitive http prefix)"
    );
}

/// `domains_in` filter must REJECT comments (which have no `domain` field) when
/// the filter is set, even when the subreddit matches. This locks down the
/// "comments fall through when domain filter is set" branch in
/// `matches_minimal`.
#[test]
fn domains_in_filter_rejects_comments_when_set() {
    let base = make_corpus_basic();
    let counts = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .domains_in(["example.com"])
        .count_by_month()
        .unwrap();
    // Only the s1 submission has domain=example.com; comments are discarded.
    assert_eq!(counts.get(&YearMonth::new(2006, 1)).copied(), Some(1));
}
