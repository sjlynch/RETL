#[path = "common/mod.rs"]
mod common;

use common::*;
use retl::{ExportFormat, RedditETL, Sources, YearMonth};

/// Extract with a whitelist and human-readable timestamps:
/// - Focus "programming" in Jan 2006
/// - Whitelist only a few fields to keep records small
/// - Turn on human-readable timestamps (RFC3339) for numeric time fields
/// - Extract to a single JSONL file
/// Checks: file exists, records parse, `created_utc` becomes a string (when present)
#[test]
fn extract_with_whitelist_and_human_timestamps() {
    let base = make_corpus_basic();
    let out = base.join("whitelist_human.jsonl");

    RedditETL::new()
        .base_dir(&base)
        .timestamps_human_readable(true)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .whitelist_fields(["author", "created_utc", "subreddit", "id"])
        .extract_to_jsonl(&out)
        .unwrap();

    assert!(out.exists(), "output JSONL should exist");

    let lines = read_jsonl_values(&out);
    assert!(!lines.is_empty());

    // If created_utc exists, it should now be a string (RFC3339).
    for v in lines {
        if let Some(cu) = v.get("created_utc") {
            assert!(cu.is_string(), "created_utc should be RFC3339 string when present");
        }
    }
}

/// Partitioned export to ZST:
/// - Focus "programming" in Jan 2006
/// - **Allow pseudo users** to include the `[deleted]` record
/// - Export partitioned per source (comments/submissions) as ZST
/// Expectation: RC_2006-01.zst has 3 lines, RS_2006-01.zst has 2 lines (total 5)
#[test]
fn export_partitioned_zst() {
    let base = make_corpus_basic();
    let out_dir = base.join("export_zst");

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .allow_pseudo_users() // include `[deleted]`
        .export_partitioned(&out_dir, ExportFormat::Zst)
        .unwrap();

    let rc = out_dir.join("comments").join("RC_2006-01.zst");
    let rs = out_dir.join("submissions").join("RS_2006-01.zst");
    assert!(rc.exists(), "RC zst not found at {}", rc.display());
    assert!(rs.exists(), "RS zst not found at {}", rs.display());

    let rc_lines = decompress_zst_lines(&rc).len();
    let rs_lines = decompress_zst_lines(&rs).len();

    assert_eq!(rc_lines, 3, "comments (RC) should have 3 lines");
    assert_eq!(rs_lines, 2, "submissions (RS) should have 2 lines");
    assert_eq!(rc_lines + rs_lines, 5, "total lines should be 5");
}
