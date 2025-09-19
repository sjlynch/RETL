#[path = "common/mod.rs"]
mod common;

use common::*;
use retl::{RedditETL, Sources, YearMonth};
use regex::Regex;

/// Demonstrates advanced `scan().usernames()` with keyword, score and URL filters:
///   - keywords_any("rust") matches title "Rust news" (submission) and a comment body
///   - contains_url(true) keeps only records that include http(s) in title/body/selftext
///   - min_score(2) ensures low-score noise is removed
/// Outcome: authors should include "alice" (comment with URL) but not "bob" (title lacks URL)
#[test]
fn usernames_with_filters_keywords_url_score() {
    let base = make_corpus_basic();
    let mut it = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .keywords_any(["rust"])
        .contains_url(true)
        .min_score(2)
        .usernames()
        .unwrap();

    let mut got = Vec::<String>::new();
    while let Some(u) = it.next() { got.push(u.to_lowercase()); }
    got.sort();
    got.dedup();
    assert_eq!(got, vec!["alice"]);
}

/// Demonstrates domain filtering (`domains_in`) which applies to submissions only.
/// We intentionally **do not** exclude bots here, to prove that domain filtering works.
/// Outcome: `nytimes.com` matches only AutoModerator's submission.
#[test]
fn usernames_with_domain_filter_submissions_only() {
    let base = make_corpus_basic();
    let it = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .domains_in(["nytimes.com"])
        .usernames()
        .unwrap();

    let mut got: Vec<String> = it.collect();
    for s in got.iter_mut() { *s = s.to_lowercase(); }
    got.sort();
    got.dedup();
    assert_eq!(got, vec!["automoderator"]);
}

/// Demonstrates author allow/deny lists and regex:
///   - authors_in(["alice","charlie"]) whitelists those two
///   - authors_out(["charlie"]) removes charlie again
///   - author_regex(^a.*) keeps only authors starting with 'a' (alice)
#[test]
fn usernames_authors_in_out_and_regex() {
    let base = make_corpus_basic();
    let re = Regex::new(r"^a.*").unwrap();
    let it = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .authors_in(["alice", "charlie"])
        .authors_out(["charlie"])
        .author_regex(re)
        .usernames()
        .unwrap();

    let got: Vec<String> = it.collect();
    assert_eq!(got, vec!["alice"]);
}

/// Demonstrates excluding a default set of bot/service accounts via `exclude_common_bots`.
/// Outcome: "AutoModerator" will be excluded, leaving only human authors.
/// (The tiny corpus contains "bob", "alice", "charlie", "AutoModerator".)
#[test]
fn usernames_exclude_common_bots() {
    let base = make_corpus_basic();
    let it = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .exclude_common_bots()
        .usernames()
        .unwrap();

    let mut got: Vec<String> = it.collect();
    got.sort();
    assert_eq!(got, vec!["alice", "bob", "charlie"]);
}
