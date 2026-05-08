//! Verify `ScanPlan::exclude_common_bots()` honors the documented env vars:
//!   - ETL_EXCLUDE_AUTHORS:    comma/semicolon/space separated names
//!   - ETL_EXCLUDE_AUTHORS_FILE: newline-separated path
//!
//! README documents these but until now nothing tested them. We use `serial_test`
//! because env-var mutation is process-global and would race across the test
//! threadpool.

#[path = "common/mod.rs"]
mod common;

use common::*;
use retl::{RedditETL, Sources, YearMonth};
use serial_test::serial;
use std::fs;
use std::io::Write;

/// Helper: clear env vars between tests so cases don't leak into each other.
fn clear_env() {
    std::env::remove_var("ETL_EXCLUDE_AUTHORS");
    std::env::remove_var("ETL_EXCLUDE_AUTHORS_FILE");
}

/// Helper: build a tiny corpus that has author "alice" so we can prove
/// the env-var-driven exclusion took effect.
fn corpus_with_alice() -> std::path::PathBuf {
    make_corpus_basic()
}

#[test]
#[serial]
fn etl_exclude_authors_env_var_excludes_listed_names() {
    clear_env();
    // Before: alice should be present.
    let base = corpus_with_alice();
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
    assert!(got.contains(&"alice".to_string()));
    drop(got);

    // Now exclude "alice" (and "bob") via env var. Use mixed separators.
    std::env::set_var("ETL_EXCLUDE_AUTHORS", "alice, bob; charlie");
    let base2 = corpus_with_alice();
    let it = RedditETL::new()
        .base_dir(&base2)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .exclude_common_bots()
        .usernames()
        .unwrap();
    let got: Vec<String> = it.collect();
    assert!(!got.contains(&"alice".to_string()), "got: {:?}", got);
    assert!(!got.contains(&"bob".to_string()), "got: {:?}", got);
    assert!(!got.contains(&"charlie".to_string()), "got: {:?}", got);
    // With all human authors excluded, only nothing remains (AutoModerator already excluded by defaults).
    assert!(got.is_empty(), "expected empty username list, got: {:?}", got);

    clear_env();
}

#[test]
#[serial]
fn etl_exclude_authors_file_env_var_reads_newline_separated_names() {
    clear_env();

    // Write a file with one name per line, including blank lines and case differences.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("excludes.txt");
    {
        let mut f = fs::File::create(&path).unwrap();
        // Names get normalized to lowercase, so test mixed case.
        writeln!(f, "Alice").unwrap();
        writeln!(f).unwrap(); // blank
        writeln!(f, "BOB").unwrap();
    }
    std::env::set_var("ETL_EXCLUDE_AUTHORS_FILE", path.display().to_string());

    let base = corpus_with_alice();
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
    assert!(!got.contains(&"alice".to_string()), "alice should be excluded; got {:?}", got);
    assert!(!got.contains(&"bob".to_string()), "bob should be excluded; got {:?}", got);
    // charlie remains.
    assert_eq!(got, vec!["charlie".to_string()]);

    clear_env();
}

#[test]
#[serial]
fn etl_exclude_authors_env_combines_with_file_and_defaults() {
    clear_env();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("more.txt");
    fs::write(&path, "charlie\n").unwrap();
    std::env::set_var("ETL_EXCLUDE_AUTHORS", "alice");
    std::env::set_var("ETL_EXCLUDE_AUTHORS_FILE", path.display().to_string());

    let base = corpus_with_alice();
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
    // alice (env) and charlie (file) both excluded; AutoModerator excluded by defaults; bob remains.
    assert_eq!(got, vec!["bob".to_string()]);

    clear_env();
}
