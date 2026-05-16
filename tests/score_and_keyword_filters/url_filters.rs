use crate::common::{read_jsonl_values, write_zst_lines};
use retl::{RedditETL, Sources, YearMonth};
use serde_json::json;
use std::path::PathBuf;

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

#[test]
fn contains_url_false_is_same_as_default_no_filter() {
    let base = corpus_with_https_only();

    let default_counts = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .count_by_month()
        .unwrap();
    let false_counts = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .contains_url(false)
        .count_by_month()
        .unwrap();

    assert_eq!(false_counts, default_counts);
    assert_eq!(false_counts.get(&YearMonth::new(2006, 1)).copied(), Some(2));
}

/// Link-post submissions store the outbound URL in top-level `url`; title and
/// selftext may contain no URL-looking text.
fn corpus_with_link_post_url_only() -> PathBuf {
    let dir = tempfile::tempdir().unwrap().keep();
    let rs = dir.join("submissions").join("RS_2006-01.zst");
    let lines: Vec<String> = vec![
        json!({
            "subreddit":"programming","author":"u_link","id":"s1",
            "domain":"example.com","title":"Interesting article","selftext":"",
            "url":"HTTPS://example.com/article",
            "created_utc":1136074600_i64,"score":1_i64
        })
        .to_string(),
        json!({
            "subreddit":"programming","author":"u_self","id":"s2",
            "domain":"self.programming","title":"No link here","selftext":"plain discussion",
            "created_utc":1136074601_i64,"score":1_i64
        })
        .to_string(),
    ];
    write_zst_lines(&rs, &lines);
    std::fs::create_dir_all(dir.join("comments")).unwrap();
    dir
}

#[test]
fn contains_url_true_matches_submission_top_level_url() {
    let base = corpus_with_link_post_url_only();
    let counts = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Submissions)
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
        "link-post top-level url should satisfy contains_url(true) even when title/selftext do not contain http"
    );
}

fn corpus_for_no_url_policy() -> PathBuf {
    let dir = tempfile::tempdir().unwrap().keep();
    let rc = dir.join("comments").join("RC_2006-01.zst");
    let rc_lines: Vec<String> = vec![
        json!({"subreddit":"programming","author":"u_comment_url","id":"c_url","body":"see https://example.com","parent_id":"t3_s1","created_utc":1136074600_i64,"score":1_i64}).to_string(),
        json!({"subreddit":"programming","author":"u_comment_plain","id":"c_plain","body":"plain discussion","parent_id":"t3_s1","created_utc":1136074601_i64,"score":1_i64}).to_string(),
    ];
    write_zst_lines(&rc, &rc_lines);

    let rs = dir.join("submissions").join("RS_2006-01.zst");
    let rs_lines: Vec<String> = vec![
        json!({
            "subreddit":"programming","author":"u_link","id":"s_link",
            "domain":"example.com","is_self":false,
            "title":"Link post","selftext":"plain title text",
            "url":"https://example.com/article",
            "created_utc":1136074700_i64,"score":1_i64
        })
        .to_string(),
        json!({
            "subreddit":"programming","author":"u_self","id":"s_self",
            "domain":"self.programming","is_self":true,
            "title":"Self post","selftext":"plain discussion",
            "url":"https://www.reddit.com/r/programming/comments/s_self/self_post/",
            "created_utc":1136074701_i64,"score":1_i64
        })
        .to_string(),
        json!({
            "subreddit":"programming","author":"u_self_text_url","id":"s_self_text_url",
            "domain":"self.programming","is_self":true,
            "title":"Self post with text URL","selftext":"see HTTP://example.com in text",
            "url":"https://www.reddit.com/r/programming/comments/s_self_text_url/self_post/",
            "created_utc":1136074702_i64,"score":1_i64
        })
        .to_string(),
    ];
    write_zst_lines(&rs, &rs_lines);
    dir
}

#[test]
fn no_url_keeps_plain_comments_and_self_posts_but_drops_url_records() {
    let base = corpus_for_no_url_policy();
    let out = base.join("no_url.jsonl");

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .no_url()
        .extract_to_jsonl(&out)
        .unwrap();

    let mut ids: Vec<String> = read_jsonl_values(&out)
        .into_iter()
        .map(|v| v["id"].as_str().unwrap().to_string())
        .collect();
    ids.sort();

    assert_eq!(ids, vec!["c_plain".to_string(), "s_self".to_string()]);
}
