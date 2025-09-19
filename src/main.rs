use anyhow::Result;
use retl::{RedditETL, Sources, YearMonth};
use std::fs;
use std::path::PathBuf;

const DATA_ROOT: &str = "./data";
const WORK_ROOT: &str = "./etl_work";

fn main() -> Result<()> {
    let base_dir = PathBuf::from(DATA_ROOT);
    let work_dir = PathBuf::from(WORK_ROOT);
    let hw = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8);
    let parallelism = hw;

    let target_subs = vec![
        "programming", "reddit.com"
    ];

    let start = YearMonth::new(2006, 1);
    let end   = YearMonth::new(2006, 4);

    fs::create_dir_all(&work_dir)?;
    let lib_tmp = work_dir.join("lib_tmp");
    fs::create_dir_all(&lib_tmp)?;

    let base = RedditETL::new()
        .base_dir(&base_dir)
        .work_dir(&lib_tmp)
        .parallelism(parallelism)
        .progress(true);

    let mut authors = vec![];
    base.clone()
        .file_concurrency(4)
        .progress_label("Collecting usernames")
        .sources(Sources::Both)
        .date_range(Some(start), Some(end))
        .scan()
        .subreddits(&target_subs)
        .for_each_username(|u| authors.push(u.to_lowercase()))?;
        
    authors.sort();
    authors.dedup();
    println!("Found {} usernames", authors.len());

    Ok(())
}
