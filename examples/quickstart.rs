//! No-corpus smoke test and library API quickstart.
//!
//! Run with:
//!   cargo run --example quickstart
//!
//! The example builds a tiny Reddit-style `.zst` corpus from the committed
//! `benches/data/sample.jsonl` fixture, then runs real RETL scans over it.

use anyhow::{Context, Result};
use retl::{RedditETL, Sources};
use serde_json::Value;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

const SAMPLE_JSONL: &str = include_str!("../benches/data/sample.jsonl");
const SAMPLE_ROOT: &str = "target/retl_quickstart_sample";

fn main() -> Result<()> {
    retl::init_tracing_for_binary();

    let base_dir = prepare_sample_corpus()?;
    let work_dir = PathBuf::from(SAMPLE_ROOT).join("etl_work");
    fs::create_dir_all(&work_dir)?;

    let base = RedditETL::new()
        .base_dir(&base_dir)
        .work_dir(&work_dir)
        .parallelism(1)
        .file_concurrency(1)
        .progress(false)
        .sources(Sources::Both);

    let counts = base
        .clone()
        .scan()
        .subreddit("programming")
        .count_by_month()?;

    let mut authors = Vec::new();
    base.scan()
        .subreddit("programming")
        .for_each_username(|u| authors.push(u.to_string()))?;
    authors.sort();

    println!("Prepared sample corpus from benches/data/sample.jsonl under {SAMPLE_ROOT}/data");
    println!("Feature demo: subreddit=programming, source=RC+RS");
    for (ym, n) in counts {
        println!("{ym}\t{n} records");
    }
    println!(
        "Found {} unique authors: {}",
        authors.len(),
        authors.join(", ")
    );

    Ok(())
}

fn prepare_sample_corpus() -> Result<PathBuf> {
    let data_dir = PathBuf::from(SAMPLE_ROOT).join("data");
    let comments_dir = data_dir.join("comments");
    let submissions_dir = data_dir.join("submissions");
    fs::create_dir_all(&comments_dir)?;
    fs::create_dir_all(&submissions_dir)?;

    let mut comments = Vec::new();
    let mut submissions = Vec::new();
    for line in SAMPLE_JSONL.lines().filter(|line| !line.trim().is_empty()) {
        let value: Value = serde_json::from_str(line).context("parse sample JSONL line")?;
        if value.get("body").is_some() {
            comments.push(line.to_owned());
        } else if value.get("title").is_some() {
            submissions.push(line.to_owned());
        }
    }

    write_zst_lines(&comments_dir.join("RC_2020-01.zst"), &comments)?;
    write_zst_lines(&submissions_dir.join("RS_2020-01.zst"), &submissions)?;

    Ok(data_dir)
}

fn write_zst_lines(path: &Path, lines: &[String]) -> Result<()> {
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut enc = zstd::stream::write::Encoder::new(file, 3).context("zstd encoder")?;
    enc.include_checksum(true).context("enable zstd checksum")?;
    for line in lines {
        enc.write_all(line.as_bytes())?;
        enc.write_all(b"\n")?;
    }
    enc.finish().context("finish zstd frame")?;
    Ok(())
}
