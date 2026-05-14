//! Export matching records directly to CSV with a fixed whitelist schema.
//!
//! Run with:
//!   cargo run --example csv_export

use retl::{RedditETL, Sources, TabularExportOptions, YearMonth};

fn main() -> anyhow::Result<()> {
    retl::init_tracing_for_binary();

    RedditETL::new()
        .base_dir("./data")
        .sources(Sources::Submissions)
        .date_range(Some(YearMonth::new(2018, 1)), Some(YearMonth::new(2018, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .limit(1_000)
        .extract_to_csv(
            std::path::Path::new("programming_sample.csv"),
            [
                "id",
                "author",
                "created_utc",
                "subreddit",
                "score",
                "title",
                "domain",
            ],
            TabularExportOptions::default(),
        )?;

    Ok(())
}
