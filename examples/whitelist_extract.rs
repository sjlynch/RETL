//! Extract filtered records to JSONL with a field whitelist (compact schema)
//! and human-readable timestamps. Mirrors what the `retl export` CLI does
//! with `--whitelist a,b,c --human-timestamps --format jsonl`.
//!
//! Requires an on-disk corpus under `./data/`. See `quickstart.rs` for the
//! expected layout.
//!
//! Run with:
//!   cargo run --example whitelist_extract

use anyhow::Result;
use retl::{RedditETL, Sources, YearMonth};
use std::path::Path;

fn main() -> Result<()> {
    retl::init_tracing_for_binary();

    // Comments-only, askscience, Q1 2016, with a compact comment schema.
    RedditETL::new()
        .base_dir("./data")
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2016, 1)), Some(YearMonth::new(2016, 3)))
        .timestamps_human_readable(true)
        .scan()
        .subreddit("askscience")
        .whitelist_fields([
            "author", "body", "created_utc", "subreddit",
            "parent_id", "link_id", "id", "score",
        ])
        .extract_to_jsonl(Path::new("askscience_comments_q1_2016_minimal.jsonl"))?;

    // Submissions-only, technology, full year 2015, with a domain/title-friendly schema.
    RedditETL::new()
        .base_dir("./data")
        .sources(Sources::Submissions)
        .date_range(Some(YearMonth::new(2015, 1)), Some(YearMonth::new(2015, 12)))
        .scan()
        .subreddit("technology")
        .whitelist_fields([
            "id", "author", "created_utc", "title", "selftext",
            "url", "domain", "score", "subreddit",
        ])
        .extract_to_jsonl(Path::new("technology_submissions_2015_minimal.jsonl"))?;

    // Filter submissions to a curated domain set.
    RedditETL::new()
        .base_dir("./data")
        .sources(Sources::Submissions)
        .date_range(Some(YearMonth::new(2018, 1)), Some(YearMonth::new(2018, 6)))
        .scan()
        .subreddit("worldnews")
        .domains_in(["bbc.co.uk", "nytimes.com", "theguardian.com"])
        .extract_to_jsonl(Path::new("worldnews_submissions_h1_2018_selected_domains.jsonl"))?;

    Ok(())
}
