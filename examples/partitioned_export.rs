//! Re-emit a date-windowed slice of the corpus as RC/RS partitioned monthly
//! files (JSONL or ZST). Demonstrates `RedditETL::scan().export_partitioned`.
//!
//! Requires an on-disk corpus laid out as:
//!   ./data/comments/RC_YYYY-MM.zst
//!   ./data/submissions/RS_YYYY-MM.zst
//!
//! Run with:
//!   cargo run --example partitioned_export

use anyhow::Result;
use retl::{ExportFormat, RedditETL, Sources, YearMonth};
use std::path::Path;

fn main() -> Result<()> {
    retl::init_tracing_for_binary();

    let base_dir = "./data";
    let start = YearMonth::new(2014, 1);
    let end = YearMonth::new(2014, 3);

    // All subreddits, JSONL output. With no whitelist this preserves the
    // original record bytes.
    RedditETL::new()
        .base_dir(base_dir)
        .sources(Sources::Both)
        .date_range(Some(start), Some(end))
        .parallelism(8)
        .scan()
        .export_partitioned(Path::new("out_corpus_jsonl"), ExportFormat::Jsonl)?;

    // Single subreddit, ZST output (re-encoded via the configured zstd level).
    RedditETL::new()
        .base_dir(base_dir)
        .sources(Sources::Both)
        .date_range(Some(start), Some(end))
        .scan()
        .subreddit("programming")
        .export_partitioned(Path::new("out_corpus_zst"), ExportFormat::Zst)?;

    Ok(())
}
