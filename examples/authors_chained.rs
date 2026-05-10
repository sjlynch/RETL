//! Two-phase pipeline:
//!   1. Discover the union of authors who participated across some subreddits
//!      in a date window (`scan().usernames()`).
//!   2. Pull every record those authors wrote in a focused set of subreddits
//!      and write it to a single JSONL file (`scan().authors_in().extract_to_jsonl()`).
//!
//! Requires an on-disk corpus under `./data/`. See `quickstart.rs` for the
//! expected layout.
//!
//! Run with:
//!   cargo run --example authors_chained

use anyhow::Result;
use retl::{RedditETL, Sources, YearMonth};
use std::collections::HashSet;
use std::path::Path;

fn main() -> Result<()> {
    retl::init_tracing_for_binary();

    let start = YearMonth::new(2017, 1);
    let end = YearMonth::new(2018, 12);

    // Phase 1: discover authors across ALL subreddits in the window.
    let mut authors = HashSet::<String>::new();
    let mut it = RedditETL::new()
        .base_dir("./data")
        .sources(Sources::Both)
        .date_range(Some(start), Some(end))
        .scan()
        .usernames()?;
    while let Some(u) = it.next() {
        authors.insert(u.to_lowercase());
    }

    // Phase 2: pull all content from those authors in a narrower set of subs.
    let authors_vec: Vec<_> = authors.into_iter().collect();
    RedditETL::new()
        .base_dir("./data")
        .sources(Sources::Both)
        .date_range(Some(start), Some(end))
        .parallelism(16)
        .scan()
        .subreddits(["bitcoin", "cryptocurrency"])
        .authors_in(authors_vec)
        .extract_to_jsonl(Path::new("crypto_authors_2017_2018.jsonl"))?;

    Ok(())
}
