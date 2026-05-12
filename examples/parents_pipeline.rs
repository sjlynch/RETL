//! End-to-end parents pipeline using the `ParentIds` / `ParentMaps` APIs:
//!
//!   1. Spool monthly JSONL parts for the records you care about
//!      (`scan().extract_spool_monthly`).
//!   2. Collect the union of `parent_id` / `link_id` references the spooled
//!      records point to (`collect_parent_ids_from_jsonls`).
//!   3. Resolve those IDs against the corpus and cache the parent payloads
//!      to disk (`resolve_parent_maps`).
//!   4. Re-stream the spool, attaching a `"parent": {...}` payload to every
//!      comment that has one (`attach_parents_jsonls_parallel`).
//!
//! An earlier hand-rolled version of this pipeline did all of step 3 by hand,
//! re-implementing zstd line streaming and parent-id collection. Prefer the
//! library APIs — they share the atomic-write / backpressure / Windows-retry
//! invariants that the rest of the pipeline depends on.
//!
//! Requires an on-disk corpus under `./data/`. See `quickstart.rs` for the
//! expected layout.
//!
//! Run with:
//!   cargo run --example parents_pipeline

use anyhow::Result;
use retl::{ParentIds, ParentMaps, RedditETL, Sources, YearMonth};
use std::path::Path;

fn main() -> Result<()> {
    retl::init_tracing_for_binary();

    let base_dir = "./data";
    let target_sub = "programming";
    let start = YearMonth::new(2006, 1);
    let end = YearMonth::new(2006, 3);

    // Step 1: spool monthly JSONL parts (one per source per month).
    let (spool_parts, _records_written) = RedditETL::new()
        .base_dir(base_dir)
        .sources(Sources::Both)
        .date_range(Some(start), Some(end))
        .progress(true)
        .scan()
        .subreddit(target_sub)
        .include_pseudo_users()
        .extract_spool_monthly(Path::new("spool"))?;

    // Step 2: collect parent IDs referenced by the spooled records.
    let ids: ParentIds = RedditETL::new()
        .base_dir(base_dir)
        .progress(true)
        .collect_parent_ids_from_jsonls(spool_parts.clone())?;

    // Step 3: resolve parent payloads from a slightly wider window so cross-month
    //         parents (a comment in 2006-02 referencing a submission from 2006-01)
    //         resolve.
    let parents: ParentMaps = RedditETL::new()
        .base_dir(base_dir)
        .date_range(
            Some(YearMonth::new(2005, 12)),
            Some(YearMonth::new(2006, 4)),
        )
        .progress(true)
        .resolve_parent_maps(&ids, Path::new("parents_cache"), /*resume=*/ true)?;

    // Step 4: attach `"parent"` payloads back onto each spooled comment.
    let _out_paths = RedditETL::new()
        .base_dir(base_dir)
        .progress(true)
        .attach_parents_jsonls_parallel(
            spool_parts,
            Path::new("spool_with_parents"),
            &parents,
            /*resume=*/ false,
        )?;

    Ok(())
}
