#[path = "common/mod.rs"]
mod common;

use common::*;
use retl::{ExportFormat, RedditETL, Sources, YearMonth};

/// Basic usernames example:
/// - Focuses on the "programming" subreddit in Jan 2006
/// - Uses `exclude_common_bots()` so bot/service accounts like AutoModerator are filtered out
/// - Collects unique usernames across comments + submissions
/// Expectation: only human authors remain ("alice", "bob", "charlie")
#[test]
fn usernames_basic() {
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

/// Scan builder + JSONL extraction:
/// - Select "programming" in Jan 2006
/// - **Allow pseudo users** so `[deleted]` is included
/// - Extract to a single JSONL file (RS + RC stitched in temporal order)
/// Expectation: 5 total lines == 2 submissions (RS) + 3 comments (RC)
#[test]
fn extract_jsonl_scan_builder() {
    let base = make_corpus_basic();
    let out = base.join("scan_extract.jsonl");

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .allow_pseudo_users() // include `[deleted]`
        .extract_to_jsonl(&out)
        .unwrap();

    let lines = read_lines(&out);
    // 1 RS(2), 1 RC(3) → total 5 lines
    assert_eq!(lines.len(), 5, "1 RS(2), 1 RC(3) → total 5 lines");
}

/// Partitioned export (JSONL) by month/source:
/// - Select "programming" in Jan 2006
/// - **Allow pseudo users** so `[deleted]` is included
/// - Export to out_base_dir/comments/RC_2006-01.jsonl and out_base_dir/submissions/RS_2006-01.jsonl
/// Expectation: RC file has 3 lines, RS file has 2 lines, total 5
#[test]
fn export_partitioned_jsonl() {
    let base = make_corpus_basic();
    let out_dir = base.join("export_partitioned");

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .allow_pseudo_users() // include `[deleted]`
        .export_partitioned(&out_dir, ExportFormat::Jsonl)
        .unwrap();

    let rc = out_dir.join("comments").join("RC_2006-01.jsonl");
    let rs = out_dir.join("submissions").join("RS_2006-01.jsonl");

    assert!(rc.exists(), "RC output not found at {}", rc.display());
    assert!(rs.exists(), "RS output not found at {}", rs.display());

    let rc_lines = read_lines(&rc).len();
    let rs_lines = read_lines(&rs).len();

    assert_eq!(rc_lines, 3, "comments (RC) should have 3 lines");
    assert_eq!(rs_lines, 2, "submissions (RS) should have 2 lines");
    assert_eq!(rc_lines + rs_lines, 5, "total lines should be 5");
}
