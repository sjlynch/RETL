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

use assert_cmd::Command;
use common::*;
use retl::{RedditETL, Sources, YearMonth};
use serde_json::json;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing_subscriber::fmt::MakeWriter;

#[derive(Clone, Default)]
struct CaptureLogs {
    buf: Arc<Mutex<Vec<u8>>>,
}

impl CaptureLogs {
    fn contents(&self) -> String {
        let bytes = self.buf.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }
}

struct CaptureWriter {
    buf: Arc<Mutex<Vec<u8>>>,
}

impl io::Write for CaptureWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.buf.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CaptureLogs {
    type Writer = CaptureWriter;

    fn make_writer(&'a self) -> Self::Writer {
        CaptureWriter {
            buf: self.buf.clone(),
        }
    }
}

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
}

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

/// `domains_in` is submission-only: comments have no `domain` field, so they
/// are rejected, and `Sources::Both` should warn instead of silently looking
/// like a zero-match query.
#[test]
fn domains_in_filter_rejects_comments_and_warns_when_sources_include_comments() {
    let base = make_corpus_basic();
    let logs = CaptureLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .with_writer(logs.clone())
        .finish();

    let counts = tracing::subscriber::with_default(subscriber, || {
        RedditETL::new()
            .base_dir(&base)
            .sources(Sources::Both)
            .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
            .progress(false)
            .scan()
            .subreddit("programming")
            .domains_in(["example.com"])
            .count_by_month()
            .unwrap()
    });

    // Only the s1 submission has domain=example.com; comments are discarded.
    assert_eq!(counts.get(&YearMonth::new(2006, 1)).copied(), Some(1));

    let logs = logs.contents();
    assert!(
        logs.contains("domains_in filters Reddit's submission-only `domain` field"),
        "expected domains_in warning, got logs: {logs:?}"
    );
    assert!(logs.contains("comment records have no domain and will be dropped"));
}
