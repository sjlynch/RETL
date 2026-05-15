mod common;

use common::write_zst_lines;
use retl::{RedditETL, ScanPlan, Sources, YearMonth};

fn err_msg<E: std::fmt::Display>(err: E) -> String {
    err.to_string()
}

fn expect_query_build_error(plan: ScanPlan, expected: &[&str]) {
    let err = plan
        .build()
        .err()
        .expect("expected ScanPlan::build to fail");
    let msg = err_msg(err);
    for needle in expected {
        assert!(msg.contains(needle), "expected {needle:?} in {msg:?}");
    }
}

#[test]
fn build_rejects_min_score_greater_than_max_score() {
    let err = RedditETL::new()
        .scan()
        .min_score(10)
        .max_score(5)
        .build()
        .err()
        .expect("expected ScanPlan::build to fail");
    let msg = err_msg(err);
    assert!(msg.contains("min_score"), "{msg}");
    assert!(msg.contains("max_score"), "{msg}");
}

#[test]
fn build_rejects_author_allow_and_deny_overlap() {
    let err = RedditETL::new()
        .scan()
        .authors_in(["alice"])
        .authors_out(["Alice"])
        .build()
        .err()
        .expect("expected ScanPlan::build to fail");
    let msg = err_msg(err);
    assert!(msg.contains("authors_in"), "{msg}");
    assert!(msg.contains("authors_out"), "{msg}");
}

#[test]
fn build_rejects_empty_subreddit_list() {
    let empty: [&str; 0] = [];
    expect_query_build_error(
        RedditETL::new().scan().subreddits(empty),
        &["subreddits", "empty list"],
    );
}

#[test]
fn build_rejects_empty_explicit_string_lists() {
    let empty: [&str; 0] = [];
    expect_query_build_error(
        RedditETL::new().scan().authors_in(empty),
        &["authors_in", "empty list"],
    );
    expect_query_build_error(
        RedditETL::new().scan().authors_out(empty),
        &["authors_out", "empty list"],
    );
    expect_query_build_error(
        RedditETL::new().scan().authors_out(empty).exclude_common_bots(),
        &["authors_out", "empty list"],
    );
    expect_query_build_error(
        RedditETL::new().scan().domains_in(empty),
        &["domains_in", "empty list"],
    );
    expect_query_build_error(
        RedditETL::new().scan().keywords_any(empty),
        &["keywords_any", "empty list"],
    );
}

#[test]
fn build_rejects_blank_normalized_filter_values() {
    expect_query_build_error(
        RedditETL::new().scan().subreddit(" r/ "),
        &["subreddits", "blank entries are not allowed"],
    );
    expect_query_build_error(
        RedditETL::new().scan().authors_in([""]),
        &["authors_in", "blank entries are not allowed"],
    );
    expect_query_build_error(
        RedditETL::new().scan().author("  "),
        &["authors_in", "blank entries are not allowed"],
    );
    expect_query_build_error(
        RedditETL::new().scan().authors_out(["\t"]),
        &["authors_out", "blank entries are not allowed"],
    );
    expect_query_build_error(
        RedditETL::new().scan().domains_in([" "]),
        &["domains_in", "blank entries are not allowed"],
    );
    expect_query_build_error(
        RedditETL::new().scan().keywords_any(["\n"]),
        &["keywords_any", "blank entries are not allowed"],
    );

    #[allow(deprecated)]
    let legacy_blank_subreddit = RedditETL::new().subreddit(" ").scan();
    expect_query_build_error(
        legacy_blank_subreddit,
        &["subreddits", "blank entries are not allowed"],
    );
}

#[test]
fn build_rejects_invalid_author_regex_pattern() {
    let err = RedditETL::new()
        .scan()
        .author_regex("[")
        .build()
        .err()
        .expect("expected ScanPlan::build to fail");
    let msg = err_msg(err);
    assert!(msg.contains("author_regex"), "{msg}");
}

#[test]
fn etl_subreddit_default_merges_with_scanplan_subreddits() {
    let dir = tempfile::tempdir().unwrap().keep();
    let rc = dir.join("comments").join("RC_2006-01.zst");
    let lines = vec![
        r#"{"subreddit":"a","author":"alice","created_utc":1136073600,"score":1,"id":"a1","body":"x"}"#.to_string(),
        r#"{"subreddit":"b","author":"bob","created_utc":1136073601,"score":1,"id":"b1","body":"x"}"#.to_string(),
        r#"{"subreddit":"c","author":"carol","created_utc":1136073602,"score":1,"id":"c1","body":"x"}"#.to_string(),
    ];
    write_zst_lines(&rc, &lines);

    #[allow(deprecated)]
    let counts = RedditETL::new()
        .base_dir(&dir)
        .subreddit("a")
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddits(["b"])
        .count_by_month()
        .unwrap();

    assert_eq!(counts.get(&YearMonth::new(2006, 1)), Some(&2));
}

#[test]
fn exclude_common_bots_composes_with_authors_out_in_either_order() {
    let base = common::make_corpus_basic();

    let mut authors_out_then_bots: Vec<String> = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .authors_out(["alice"])
        .exclude_common_bots()
        .usernames()
        .unwrap()
        .collect();
    authors_out_then_bots.sort();
    assert_eq!(
        authors_out_then_bots,
        vec!["bob".to_string(), "charlie".to_string()]
    );

    let mut bots_then_authors_out: Vec<String> = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .exclude_common_bots()
        .authors_out(["alice"])
        .usernames()
        .unwrap()
        .collect();
    bots_then_authors_out.sort();
    assert_eq!(
        bots_then_authors_out,
        vec!["bob".to_string(), "charlie".to_string()]
    );
}

#[test]
fn include_pseudo_users_keeps_deleted_removed_and_empty_authors() {
    let dir = tempfile::tempdir().unwrap().keep();
    let rc = dir.join("comments").join("RC_2006-01.zst");
    let lines = vec![
        r#"{"subreddit":"programming","author":"real","created_utc":1136073600,"score":1,"id":"r1","body":"x"}"#.to_string(),
        r#"{"subreddit":"programming","author":"[deleted]","created_utc":1136073601,"score":1,"id":"d1","body":"x"}"#.to_string(),
        r#"{"subreddit":"programming","author":"[removed]","created_utc":1136073602,"score":1,"id":"m1","body":"x"}"#.to_string(),
        r#"{"subreddit":"programming","author":"","created_utc":1136073603,"score":1,"id":"e1","body":"x"}"#.to_string(),
    ];
    write_zst_lines(&rc, &lines);

    let default_counts = RedditETL::new()
        .base_dir(&dir)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .count_by_month()
        .unwrap();
    assert_eq!(default_counts.get(&YearMonth::new(2006, 1)), Some(&1));

    let included_counts = RedditETL::new()
        .base_dir(&dir)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .include_pseudo_users()
        .count_by_month()
        .unwrap();
    assert_eq!(included_counts.get(&YearMonth::new(2006, 1)), Some(&4));
}
