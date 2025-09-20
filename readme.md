# RETL (Reddit ETL)

**RETL** is a fast, memory‑aware, streaming ETL toolkit for working with the Reddit monthly **RC** (comments) and **RS** (submissions) corpora. It’s designed to scan large `.zst` JSONL drops efficiently, filter with an intuitive query builder, and export results for analysis — all with pragmatic attention to parallelism, backpressure, and Windows‑friendly file operations.

> TL;DR: Point RETL at a `comments/` and `submissions/` directory full of `RC_YYYY-MM.zst` / `RS_YYYY-MM.zst` files, build a query, then extract or analyze in one pass.

---

## Features

- 🚀 **Streaming, single‑pass processing** of `.zst` JSONL monthly dumps (RC/RS)
- 🧠 **Intuitive query builder (DSL)**: subreddits, authors (allow/deny), regex, keywords, URL presence, domains, score thresholds
- 🧰 **Exports**:
  - JSONL / JSON array (stitched)
  - Partitioned per source (RC/RS) as JSONL or ZST
- 📈 **Analytics helpers**: `count_by_month()`, per‑author counts, “first seen” index
- 👪 **Parent pipeline**: collect parent IDs → resolve content → attach parent payloads back to records
- 🧪 **Integrity checks** for corrupted monthly files (quick or full)
- 🧵 **Parallel, backpressure‑aware** I/O with file concurrency caps and cooperative throttling
- 🪟 **Windows‑friendly I/O** with robust retry/backoff on transient errors

---

## Table of Contents

- [Data Layout](#data-layout)
- [Install](#install)
- [Quick Start](#quick-start)
- [Usage Examples](#usage-examples)
  - [Extract to JSONL](#extract-to-jsonl)
  - [Partitioned Export (JSONL/ZST)](#partitioned-export-jsonlzst)
  - [Count by Month](#count-by-month)
  - [Usernames with Filters](#usernames-with-filters)
  - [Author Analytics (TSV)](#author-analytics-tsv)
  - [Parents Pipeline (Attach Parent Content)](#parents-pipeline-attach-parent-content)
  - [Integrity Checks](#integrity-checks)
- [Performance & Tuning](#performance--tuning)
- [Environment Aids](#environment-aids)
- [License](#license)

---

## Data Layout

RETL expects a base directory with two subfolders:

~~~text
<base_dir>/
  comments/
    RC_YYYY-MM.zst
    RC_YYYY-MM.zst
    ...
  submissions/
    RS_YYYY-MM.zst
    RS_YYYY-MM.zst
    ...
~~~

File name patterns are enforced at discovery time:
- Comments: `^RC_\d{4}-\d{2}\.zst$`
- Submissions: `^RS_\d{4}-\d{2}\.zst$`

---

You can browse and download monthly dumps via Academic Torrents:
https://academictorrents.com/browse.php?search=reddit

Note on scale & performance:
The monthly files become very large in later years. It’s normal for broader queries to run longer and consume significant I/O. RETL’s streaming design and throttling aim to keep resource use predictable; tune .file_concurrency(n), .parallelism(n), and .io_buffers(...) as appropriate for your hardware and dataset size.

## Install

### As a library (recommended)

Add RETL to your Cargo.toml via Git (replace the URL with your repo if needed):

~~~toml
[dependencies]
retl = { git = "https://github.com/sjlynch/retl", branch = "main" }
~~~

> This repository currently sets `publish = false` in `Cargo.toml`, so installing from crates.io is not expected.

### Build the demo binary

This repo also includes a small example binary in `src/main.rs`:

~~~sh
cargo build --release
./target/release/retl
~~~

The binary demonstrates author collection across a small date window, and is intentionally minimal — the library API is where RETL shines.

---

## Quick Start

Place your corpus under `./data/`:

~~~text
./data/comments/RC_2006-01.zst
./data/submissions/RS_2006-01.zst
...
~~~

Create a small driver:

~~~rust
use anyhow::Result;
use retl::{RedditETL, Sources, YearMonth};

fn main() -> Result<()> {
    let counts = RedditETL::new()
        .base_dir("./data")
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(true)
        .scan()
        .subreddit("programming")
        .keywords_any(["rust"])
        .contains_url(true)
        .count_by_month()?; // {"2006-01": N}

    for (ym, n) in counts {
        println!("{ym}\t{n}");
    }
    Ok(())
}
~~~

Run:

~~~sh
cargo run --release
~~~

---

## Usage Examples

All examples below operate on the same API you saw in the Quick Start.

### Extract to JSONL

~~~rust
use retl::{RedditETL, Sources, YearMonth};
use std::path::Path;

RedditETL::new()
    .base_dir("./data")
    .sources(Sources::Both)
    .date_range(Some(YearMonth::new(2016, 1)), Some(YearMonth::new(2016, 3)))
    .progress(true)
    .scan()
    .subreddit("askscience")
    .whitelist_fields([
        "author","body","created_utc","subreddit",
        "parent_id","link_id","id","score",
    ])
    .timestamps_human_readable(true)
    .extract_to_jsonl(Path::new("askscience_comments_q1_2016_minimal.jsonl"))?;
~~~

### Partitioned Export (JSONL/ZST)

~~~rust
use retl::{ExportFormat, RedditETL, Sources, YearMonth};

RedditETL::new()
    .base_dir("./data")
    .sources(Sources::Both)
    .date_range(Some(YearMonth::new(2016, 1)), Some(YearMonth::new(2016, 1)))
    .progress(true)
    .scan()
    .subreddit("programming")
    .allow_pseudo_users() // include "[deleted]"
    .export_partitioned(std::path::Path::new("out_corpus_zst"), ExportFormat::Zst)?;
~~~

Outputs:

~~~text
out_corpus_zst/comments/RC_2016-01.zst
out_corpus_zst/submissions/RS_2016-01.zst
~~~

### Count by Month

~~~rust
use retl::{RedditETL, Sources, YearMonth};

let counts = RedditETL::new()
    .base_dir("./data")
    .sources(Sources::Both)
    .date_range(Some(YearMonth::new(2016, 1)), Some(YearMonth::new(2016, 12)))
    .progress(false)
    .scan()
    .subreddit("worldnews")
    .keywords_any(["election","vote","ballot"])
    .contains_url(true)
    .min_score(10)
    .count_by_month()?;
~~~

### Usernames with Filters

~~~rust
use retl::{RedditETL, Sources, YearMonth};

let mut it = RedditETL::new()
    .base_dir("./data")
    .sources(Sources::Both)
    .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
    .progress(false)
    .scan()
    .subreddit("programming")
    .keywords_any(["rust"])
    .contains_url(true)
    .min_score(2)
    .usernames()?;

while let Some(u) = it.next() {
    println!("{}", u);
}
~~~

### Author Analytics (TSV)

Produce a TSV of total records per author:

~~~rust
use retl::{RedditETL, Sources, YearMonth};

RedditETL::new()
    .base_dir("./data")
    .sources(Sources::Both)
    .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
    .progress(false)
    .scan()
    .subreddit("programming")
    .author_counts_to_tsv(std::path::Path::new("author_counts.tsv"))?;
~~~

And the earliest “first seen” timestamp per author:

~~~rust
RedditETL::new()
    .base_dir("./data")
    .sources(Sources::Both)
    .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
    .progress(false)
    .scan()
    .subreddit("programming")
    .build_first_seen_index_to_tsv(std::path::Path::new("first_seen.tsv"))?;
~~~

### Parents Pipeline (Attach Parent Content)

Collect parent IDs from your spooled JSONL, resolve parent contents by scanning the corpus, then attach parents back onto your records:

~~~rust
use retl::{ParentIds, ParentMaps, RedditETL, Sources, YearMonth};
use std::path::Path;

let (spool_parts, _n) = RedditETL::new()
    .base_dir("./data")
    .sources(Sources::Both)
    .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
    .progress(true)
    .scan()
    .subreddit("programming")
    .allow_pseudo_users()
    .extract_spool_monthly(Path::new("spool"), /*resume=*/false)?;

// Step 2: Collect parent IDs
let ids: ParentIds = RedditETL::new()
    .base_dir("./data")
    .progress(true)
    .collect_parent_ids_from_jsonls(spool_parts.clone())?;

// Step 3: Resolve to cache over ±1 month window
let parents: ParentMaps = RedditETL::new()
    .base_dir("./data")
    .date_range(Some(YearMonth::new(2005, 12)), Some(YearMonth::new(2006, 2)))
    .progress(true)
    .resolve_parent_maps(&ids, Path::new("parents_cache"), /*resume=*/true)?;

// Step 4: Attach parent payloads
let _out_paths = RedditETL::new()
    .base_dir("./data")
    .progress(true)
    .attach_parents_jsonls_parallel(spool_parts, Path::new("spool_with_parents"), &parents, /*resume=*/false)?;
~~~

Each comment will receive a `"parent"` object containing either the parent comment’s body (`t1_...`) or the submission’s title/selftext (`t3_...`).

### Integrity Checks

Quick sampling (fast) and full decode (slow but thorough):

~~~rust
use retl::{IntegrityMode, RedditETL, Sources, YearMonth};

let bad_quick = RedditETL::new()
    .base_dir("./data")
    .sources(Sources::Comments)
    .date_range(Some(YearMonth::new(2006, 2)), Some(YearMonth::new(2006, 2)))
    .progress(false)
    .check_corpus_integrity(IntegrityMode::Quick { sample_bytes: 64 * 1024 })?;

let bad_full = RedditETL::new()
    .base_dir("./data")
    .sources(Sources::Comments)
    .date_range(Some(YearMonth::new(2006, 2)), Some(YearMonth::new(2006, 2)))
    .progress(false)
    .check_corpus_integrity(IntegrityMode::Full)?;
~~~

---

## Performance & Tuning

- **Parallelism**: set Rayon threads based on your hardware:
  ~~~rust
  .parallelism(24)
  ~~~
- **File concurrency**: limit how many monthly files are decoded at once (helps with RAM/IO pressure):
  ~~~rust
  .file_concurrency(4)
  ~~~
- **Buffers**: tune read/write buffers if you’re IO‑bound:
  ~~~rust
  .io_buffers(256 * 1024, 256 * 1024)
  ~~~
- **Work directory**: point scratch space to fast storage:
  ~~~rust
  .work_dir("/mnt/nvme/etl_tmp")
  ~~~
- **Progress**: toggle progress bars and custom labels:
  ~~~rust
  .progress(true).progress_label("Counting")
  ~~~

RETL cooperates under low memory by adaptively throttling certain stages.

---

## Environment Aids

- **Logging**: respect `RUST_LOG`, e.g.:
  ~~~sh
  RUST_LOG=info cargo run --release
  ~~~
- **Exclude bots**: start from a conservative default list and extend via env/file:
  - `ETL_EXCLUDE_AUTHORS="bot_a, bot_b, service_c"`
  - `ETL_EXCLUDE_AUTHORS_FILE=/path/to/extra_exclusions.txt`
  Then use:
  ~~~rust
  .exclude_common_bots()
  ~~~

---

## License

**MIT**. See `LICENSE` for details.

---

## Project Goals

- Provide an **intuitive, composable query system** that feels like a fluent DSL
- Keep **memory profile predictable** with adaptive buffering and backpressure
- Make **common workflows boringly easy** (extract, export, count, attach parents)
- Stay **robust on Windows** and networked filesystems via retry/backoff I/O
