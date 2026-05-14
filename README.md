# RETL (Reddit ETL)

**RETL** is a fast, memory‑aware, streaming ETL toolkit for working with the Reddit monthly **RC** (comments) and **RS** (submissions) corpora. It’s designed to scan large `.zst` JSONL drops efficiently, filter with an intuitive query builder, and export results for analysis — all with pragmatic attention to parallelism, backpressure, and Windows‑friendly file operations.

> TL;DR: Point RETL at a `comments/` and `submissions/` directory full of `RC_YYYY-MM.zst` / `RS_YYYY-MM.zst` files, build a query, then extract or analyze in one pass.

---

## Features

- 🚀 **Streaming, single‑pass processing** of `.zst` JSONL monthly dumps (RC/RS)
- 🧠 **Intuitive query builder (DSL)**: subreddits, authors (allow/deny), regex, keywords, URL presence, domains, score thresholds
- 🧰 **Exports**:
  - JSONL / JSON array (stitched)
  - Partitioned per source/month in corpus layout as JSONL or ZST (CLI: `retl export --format partitioned-jsonl` / `--format zst`)
- 📈 **Analytics helpers**: `count_by_month()`, per‑author counts, “first seen” index
- 👪 **Parent pipeline**: collect parent IDs → resolve content → attach parent payloads back to records
- 🧪 **Integrity checks** for corrupted monthly files (quick or full)
- 🧵 **Parallel, backpressure‑aware** I/O with file concurrency caps and cooperative throttling
- 🪟 **Windows‑friendly I/O** with robust retry/backoff on transient errors

---

## Table of Contents

- [Try it in 60 seconds (no corpus needed)](#try-it-in-60-seconds-no-corpus-needed)
- [Data Layout](#data-layout)
- [Install](#install)
- [Command-Line Interface](#command-line-interface)
- [Quick CLI start](#quick-cli-start)
- [Quick Start](#quick-start)
- [Usage Examples](#usage-examples)
  - [Extract to JSONL](#extract-to-jsonl)
  - [Partitioned Export (JSONL/ZST)](#partitioned-export-jsonlzst)
  - [Count by Month](#count-by-month)
  - [Usernames with Filters](#usernames-with-filters)
  - [Author Analytics (TSV)](#author-analytics-tsv)
  - [Parents Pipeline (Attach Parent Content)](#parents-pipeline-attach-parent-content)
  - [Integrity Checks](#integrity-checks)
- [Performance and tuning](#performance-and-tuning)
- [Benchmarks](#benchmarks)
- [Environment variables](#environment-variables)
- [Fuzzing](#fuzzing)
- [License](#license)
- [Project Goals](#project-goals)

---

## Try it in 60 seconds (no corpus needed)

You can verify a fresh checkout without downloading the multi-GB Reddit corpus.
This builds the release CLI binary, then runs the `quickstart` example against
the committed `benches/data/sample.jsonl` fixture. The example writes a tiny
Reddit-style `.zst` corpus under `target/retl_quickstart_sample/data` and runs
real RETL scans over it.

~~~sh
cargo build --release
cargo run --release --example quickstart
~~~

Expected output:

~~~text
Prepared sample corpus from benches/data/sample.jsonl under target/retl_quickstart_sample/data
Feature demo: subreddit=programming, source=RC+RS
2019-12	2 records
2020-01	3 records
Found 4 unique authors: alice, cory, kate, quinn
~~~

If that works and you already have monthly Reddit dumps, continue with the
full [Quick Start](#quick-start).

---

## Data Layout

For real corpus runs, RETL expects a base directory with two subfolders:

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

Add RETL to your Cargo.toml via Git:

~~~toml
[dependencies]
retl = { git = "https://github.com/sjlynch/retl", branch = "main" }
~~~

> This repository currently sets `publish = false` in `Cargo.toml`, so installing from crates.io is not expected.

### Build the CLI binary

This repo ships a `retl` binary (`src/main.rs`) that exposes the most common
ETL operations as subcommands. The original demo lives at
`examples/quickstart.rs`.

~~~sh
cargo build --release
./target/release/retl --help
~~~

For copy-pasteable per-subcommand invocations, see
[Command-Line Interface](#command-line-interface) below. The library API is
still the most flexible surface — the CLI is a thin wrapper over the same
builder methods.

---

## Command-Line Interface

The `retl` binary exposes the main ETL subcommands. Querying commands that scan
the corpus (`scan`, `dedupe`, `export`, `count`, and `first-seen`) accept a
shared set of selection/runtime flags (see `retl <subcommand> --help` for the
full list). `integrity` scans the corpus too, but only accepts corpus selection
and validation flags because it does not filter records by query:

| Common flag | Purpose |
| --- | --- |
| `--data-dir <PATH>` | Corpus base dir (default `./data`). Must contain `comments/` and `submissions/`. |
| `--work-dir <PATH>` | Scratch directory for sharded writers (default `./etl_work`). |
| `--start <YYYY-MM>` / `--end <YYYY-MM>` | Inclusive month range. Omit either to leave that side unbounded; the present side is still enforced against each record's `created_utc`. |
| `--source rc\|rs\|both` | Comments only, submissions only, or both (default). |
| `--subreddit <NAME>` (`-s`) | Subreddit selector. Repeatable; omit for "any subreddit". |
| `--author <NAME>` / `--author-in <NAME>` | Author allow-list selector. Repeatable. |
| `--exclude-author <NAME>` | Author deny-list selector. Repeatable. |
| `--exclude-common-bots` | Exclude RETL's built-in bot/service-account list plus `ETL_EXCLUDE_AUTHORS*` augments; composes with `--exclude-author`. |
| `--author-regex <REGEX>` | Keep authors matching a regex. |
| `--keyword <TEXT>` | Keep records containing a keyword in body/title/selftext. Repeatable. |
| `--min-score <N>` / `--max-score <N>` | Inclusive score thresholds. |
| `--contains-url` | Keep records with an HTTP(S) URL in text or submission URL. |
| `--domain <DOMAIN>` | Submission-domain allow-list. Repeatable; comments are dropped when this filter is active. |
| `--include-deleted` | Include pseudo-users (`[deleted]`, `[removed]`, and empty authors) that are filtered by default. |
| `--parallelism <N>` / `--file-concurrency <N>` | Rayon threads / concurrent monthly files. |
| `--no-progress` | Disable progress bars. |

### Pseudo-user filtering (default ON)

By default, scans exclude records whose `author` is `[deleted]`, `[removed]`, or the empty string. This keeps normal username/export queries focused on real author names, but it matters for deletion-rate, ban-wave, or corpus-completeness analysis. Pass `--include-deleted` (alias: `--include-pseudo-users`) on the CLI, or call `.include_pseudo_users()` on a `ScanPlan`, to keep those records. Bot/service-account exclusions are separate: use `--exclude-common-bots` or `.exclude_common_bots()` to drop accounts such as AutoModerator without changing pseudo-user handling.

### Date ranges and missing months

When `--start` or `--end` is set, RETL filters individual records by the same inclusive month bounds it uses for file planning. Records without `created_utc` are dropped while any date bound is active. If files are missing inside the requested range (for example Jan and Mar exist but Feb is absent), corpus-scanning commands emit a warning and `retl describe` reports the missing month list.

### `describe` — inspect the discovered corpus

Lists the monthly `.zst` files RETL sees under `--data-dir` without decoding
anything. Use it before a long scan/export to verify the `comments/` +
`submissions/` layout, available month ranges, selected file count, total
compressed bytes, and missing-month holes in the selected range:

~~~sh
retl describe --data-dir ./data --source both --start 2016-01 --end 2016-12
# source  available              files_in_range  compressed_bytes  missing_month_count  missing_months
# rc      2005-12..=2024-12      12              123456789012      0                    -
# rs      2005-06..=2024-12      12              23456789012       0                    -
# total                          24              146913578024      0                    -
~~~

Aliases: `retl ls`, `retl plan`.

### `scan` — emit unique usernames

Walks the corpus, applies the query selection, and writes one
deduped username per line to `--out` (or stdout):

~~~sh
retl scan \
  --data-dir ./data \
  --start 2006-01 --end 2006-04 \
  --subreddit programming --subreddit reddit.com \
  --out usernames.txt
~~~

### `dedupe` — emit unique keys

Walks the corpus, applies the query selection, and writes one distinct key per
line to `--out` (use `--out -` for stdout). `--key` reuses RETL's
`KeyExtractor`: `author` and `subreddit` use the `MinimalRecord` fast path;
`json:/pointer` parses full JSON only to extract the requested JSON Pointer.
Aliases: `unique`, `distinct`.

~~~sh
# Unique authors who posted in r/rust in 2020-2023
retl dedupe \
  --data-dir ./data \
  --start 2020-01 --end 2023-12 \
  --subreddit rust \
  --key author \
  --out unique_authors.txt

# Unique subreddits in the selected comments/submissions
retl dedupe --key subreddit --start 2021-01 --end 2021-12 --out subreddits.txt

# Unique parent IDs from comments using a JSON Pointer key
retl dedupe --source rc --key 'json:/parent_id' --start 2020-01 --end 2020-12 --out parent_ids.txt
~~~

### `export` — extract filtered records

Formats:

* `--format jsonl` → single stitched `.jsonl` file (default).
* `--format json`  → single `.json` file containing a JSON array (`--pretty` for pretty-print).
* `--format spool` → per-source per-month files (`part_RC_YYYY-MM.jsonl`, `part_RS_YYYY-MM.jsonl`) under the directory passed to `--out`. Use this for the parents-pipeline workflow.
* `--format zst` → corpus-style partitioned `.zst` output under `<out>/comments/RC_YYYY-MM.zst` and `<out>/submissions/RS_YYYY-MM.zst`.
* `--format partitioned-jsonl` → the same corpus-style directory layout, but as uncompressed `.jsonl` files.

Export-only modifiers include `--whitelist a,b,c`, `--strict-whitelist`, `--human-timestamps`, `--zst-level <N>`, and `--resume`. With `--resume`, `jsonl`/`json` exports checkpoint per-month `.part_*.jsonl` files under `--work-dir`, while `spool` checkpoints `part_RC_*` / `part_RS_*` and `_progress.json` under `--out`. The checkpoint includes a fingerprint of the query and output-affecting config; changing filters, sources, date range, whitelist fields, or `--human-timestamps` discards stale parts instead of mixing results from different runs. `--resume` is not currently supported for `zst` or `partitioned-jsonl`.

~~~sh
# JSONL with a field whitelist and human timestamps
retl export \
  --data-dir ./data \
  --start 2016-01 --end 2016-03 \
  --subreddit askscience \
  --whitelist author,body,created_utc,subreddit,parent_id,link_id,id,score \
  --human-timestamps \
  --format jsonl \
  --out askscience_2016Q1.jsonl

# Keyword + score + URL-filtered export
retl export \
  --data-dir ./data \
  --start 2020-01 --end 2020-12 \
  --subreddit rust \
  --keyword async \
  --min-score 10 \
  --contains-url \
  --format jsonl \
  --out rust_async_links_2020.jsonl

# Domain-only submissions query (comments have no submission `domain` field)
retl export \
  --data-dir ./data \
  --source rs \
  --start 2020-01 --end 2020-12 \
  --domain nytimes.com \
  --format jsonl \
  --out nytimes_submissions_2020.jsonl

# Spool monthly parts (input for parents pipeline / aggregation)
retl export --start 2006-01 --end 2006-01 -s programming --format spool --out ./spool

# Re-emit a filtered mini-corpus in the original RC/RS .zst layout
retl export \
  --data-dir ./data \
  --start 2020-01 --end 2020-12 \
  --subreddit rust \
  --format zst \
  --zst-level 10 \
  --out ./rust_2020_corpus
~~~

### `count` — record counts

Two modes:

* `--mode month` (default) writes `YYYY-MM\tcount` lines to `--out` (or stdout).
* `--mode author` writes a per-author count TSV to `--out` (required).

~~~sh
# Records per month, all comments + submissions
retl count --start 2016-01 --end 2016-12 --subreddit worldnews

# Author-level counts
retl count --mode author --subreddit programming --start 2006-01 --end 2006-04 --out authors.tsv
~~~

### `integrity` — validate `.zst` monthly files

~~~sh
# Quick: decode at most 64 KiB per file
retl integrity --mode quick --sample-bytes 65536 --source rc --start 2006-02 --end 2006-02

# Full: decode every byte (validates trailing checksum)
retl integrity --mode full --source both --start 2006-01 --end 2006-04
~~~

Bad files print one `path<TAB>error` line per failure on stdout and the
process exits with status `2`.

### `aggregate` — fold JSONL inputs into JSON or TSV rollups

Aggregates one or more already-filtered JSONL inputs using the
`retl::Aggregator` pipeline (each input is processed in parallel; per-input
shard intermediates land under `--shards-dir`, which defaults to `agg_shards/`
next to `--out`). `aggregate` does not scan the RC/RS corpus and does not
accept corpus selectors such as `--data-dir`, `--start`, or `--subreddit`; run
`retl export --format spool ...` first if you need to filter the corpus. Its
runtime flags are limited to `--parallelism`, `--no-progress`, and
`--shards-dir`. With no `--by`, the CLI keeps the original built-in
record-count fallback and writes a JSON aggregate state:

~~~sh
retl aggregate \
  --out agg.json --pretty \
  ./spool/part_RC_2006-01.jsonl ./spool/part_RS_2006-01.jsonl
~~~

Built-in grouped rollups write two-column TSV:

~~~sh
retl aggregate --by subreddit --out counts.tsv ./spool/*.jsonl
retl aggregate --by month --out months.tsv ./spool/*.jsonl
retl aggregate --by author --top 100 --out top_authors.tsv ./spool/*.jsonl
retl aggregate --by 'json:/subreddit' --metric 'sum:/score' --out scores.tsv ./spool/*.jsonl
~~~

`--metric` defaults to `count` and also supports `avg:/pointer`,
`min:/pointer`, and `max:/pointer` for numeric JSON-pointer values.

---

## Quick CLI start

If you just want to drive RETL from a shell, build the binary once and use
the subcommands directly:

~~~sh
cargo build --release
./target/release/retl --help
./target/release/retl export --help
~~~

Count comments in `r/programming` for one month:

~~~sh
./target/release/retl count \
  --data-dir ./data \
  --start 2006-01 --end 2006-01 \
  --subreddit programming
~~~

Export filtered records to JSONL with a field whitelist and human-readable
timestamps:

~~~sh
./target/release/retl export \
  --data-dir ./data \
  --start 2016-01 --end 2016-03 \
  --subreddit askscience \
  --whitelist author,body,created_utc,subreddit,parent_id,link_id,id,score \
  --human-timestamps \
  --format jsonl \
  --out askscience_2016Q1.jsonl
~~~

See [Command-Line Interface](#command-line-interface) for every subcommand
and flag. The library API below exposes the same builder methods if you need
finer-grained control or want to embed RETL in a larger Rust program.

---

## Quick Start

Place your corpus under `./data/` (the default base directory for both the CLI and library):

~~~text
./data/comments/RC_2006-01.zst
./data/submissions/RS_2006-01.zst
...
~~~

To use RETL as a library, create a separate binary crate that depends on this
repository (do **not** replace this repo's `src/main.rs`; that file is the
`retl` CLI):

~~~toml
[dependencies]
anyhow = "1"
retl = { git = "https://github.com/sjlynch/retl", branch = "main" }
~~~

Then put a small driver like this in that crate's `src/main.rs`:

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
        .count_by_month()?; // BTreeMap<YearMonth, u64>, e.g. 2006-01 => N

    for (ym, n) in counts {
        println!("{ym}\t{n}");
    }
    Ok(())
}
~~~

Run from that driver crate:

~~~sh
cargo run --release
~~~

For an in-repo executable example, see `examples/quickstart.rs`. The central
query builder type and its validation error are public, so downstream code can
name `retl::ScanPlan` and `retl::QueryBuildError` when wrapping or storing
plans.

---

## Usage Examples

All examples below operate on the same API you saw in the Quick Start.

### Query DSL filter notes

- `.contains_url(true)` keeps records with `http`/`https` in comment bodies,
  submission `selftext`/`title`, or a link-post submission whose top-level
  `url` starts with `http`/`https`.
- `.domains_in([...])` matches the submission-only top-level `domain` field.
  Reddit comments do not have `domain`; when used with `Sources::Both` or
  `Sources::Comments`, comments are dropped and RETL emits a warning. Use
  `Sources::Submissions` when you intend a domain-only scan.
- `.exclude_common_bots()` merges RETL's default bot/service-account deny-list
  with any `.authors_out(...)` entries and `ETL_EXCLUDE_AUTHORS*` augments,
  regardless of builder call order. It does not affect pseudo-users; use
  `.include_pseudo_users()` / `--include-deleted` for those.

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
    .include_pseudo_users() // include "[deleted]"
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

**Important:** if the spool is produced with `--whitelist` / `.whitelist_fields(...)`, preserve at least `parent_id` and `link_id` for any spool destined for the parents pipeline. Include `body` too when you want the child comment text retained in the attached output (recommended: `body,parent_id,link_id` alongside your analysis fields). If `parent_id`/`link_id` are dropped, `retl parents` fails fast with an actionable whitelist diagnostic instead of scanning the corpus for a no-op run.

~~~rust
use retl::{ParentIds, ParentMaps, RedditETL, Sources, YearMonth};
use std::path::Path;

let resume = true; // reuse matching spool/progress entries, parent-cache shards, and attached outputs

let (spool_parts, _n) = RedditETL::new()
    .base_dir("./data")
    .sources(Sources::Both)
    .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
    .progress(true)
    .resume(resume)
    .scan()
    .subreddit("programming")
    .include_pseudo_users()
    .extract_spool_monthly(Path::new("spool"))?;

// Step 2: Collect parent IDs
let ids: ParentIds = RedditETL::new()
    .base_dir("./data")
    .progress(true)
    .collect_parent_ids_from_jsonls(spool_parts.clone())?;

// Step 3: Resolve to cache over a ±3 month window (the CLI default)
let parents: ParentMaps = RedditETL::new()
    .base_dir("./data")
    .date_range(Some(YearMonth::new(2005, 10)), Some(YearMonth::new(2006, 4)))
    .progress(true)
    .resolve_parent_maps(&ids, Path::new("parents_cache"), resume)?;

// Step 4: Attach parent payloads; resume skips outputs that already exist
let _out_paths = RedditETL::new()
    .base_dir("./data")
    .progress(true)
    .attach_parents_jsonls_parallel(spool_parts, Path::new("spool_with_parents"), &parents, resume)?;
~~~

Resolved comments receive a `"parent"` object containing either the parent comment’s body (`t1_...`) or the submission’s title/selftext (`t3_...`). If a referenced parent cannot be resolved from the cache/window, `retl` leaves the `"parent"` key absent rather than writing an empty object; the CLI reports resolved/unresolved totals and warns when more than 5% are unresolved.

Note: extract/spool resume entries are fingerprinted by query/config. Parent-cache resume files are keyed by source/month and validated by parsing; regenerate the parent ID set when changing the upstream query.

The `parents` CLI uses `--window-months 3` by default, scanning three extra months on each side of the spool range. Larger windows catch more old cross-month parents, but scan more corpus bytes and create/use more parent-cache shard files; smaller windows are faster and lighter but can leave more parents unresolved.

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

## Performance and tuning

Defaults are conservative. The knobs below are the ones that matter on large
corpora.

### What to expect

Representative run, measured on a 16-core Ryzen 9 5950X workstation with
64 GB RAM and a PCIe 4.0 NVMe SSD, with `file_concurrency = 4` and local
`work_dir`/spool/cache directories:

- One typical comments month (`RC_2018-06.zst`) takes about 10–15 minutes
  wall-clock, peaks around 18–24 GB RSS, and can consume 75–125 GB of scratch
  disk while staging spool/cache/work files.
- A 12-year comments+submissions scan is an overnight-to-weekend job on this
  class of machine: roughly 24–48 hours when inputs and scratch are on local
  NVMe, longer on HDDs or network shares.
- Compressed input size is not the runtime footprint. Reddit zstd frames can
  require multi-GiB decoded windows, and each concurrently decoded monthly
  file carries its own window and parser buffers.
- `work_dir` + parent cache + spool output can temporarily balloon to several
  times the compressed input size. Put all three on fast local storage with
  ample free space; avoid mixing them with a slow network-mounted corpus path.

Suggested starting points for `.file_concurrency(n)` by total system RAM:

| RAM | Start with |
| --- | --- |
| < 16 GB | 1 |
| 16–31 GB | 2 |
| 32–63 GB | 4 |
| 64–127 GB | 8 |
| ≥ 128 GB | 8–12, then measure |

### Tuning knobs

- `.parallelism(n)` — Rayon worker threads. CPU-bound work (decompression,
  parsing) scales with this up to physical core count.
- `.file_concurrency(n)` — number of monthly files decoded in parallel. The
  per-file working set is large; raising this is the fastest way to use up
  RAM.
- `.inflight_bytes(bytes)` — hard cap for producer→consumer backpressure in
  bucketing/dedupe. Lower it to reduce peaks; raise only after measuring RAM.
- `.adaptive_mem(AdaptiveMemCfg { soft_low_frac, high_frac, adapt_cooldown_ms })`
  — cooperative buffer policy. `soft_low_frac` is the available-memory fraction
  below which RETL shrinks buffers and flushes sooner, `high_frac` is the
  fraction above which it allows larger buffers, and `adapt_cooldown_ms` is the
  minimum interval between target-size recomputations. Defaults are `0.18`,
  `0.85`, and `400` ms.
- `.io_buffers(read, write)` — read/write buffer sizes in bytes. The default
  is fine for SSD-backed local storage; increase to 1–4 MiB on networked
  filesystems.
- `.work_dir(path)` — scratch directory for intermediate shards and
  `.inprogress` files. Point this at fast local storage if the corpus lives
  on a network share.
- `.progress(true)` and `.progress_label("...")` — render an `indicatif`
  progress bar.

RETL throttles cooperatively when system memory falls below the configured
thresholds; you usually do not need to manage this manually beyond picking a
reasonable `file_concurrency` and scratch location.

---

## Benchmarks

The hot inner loops touched by perf work (record filtering, byte-level
timestamp rewrite, and zstd line streaming) are covered by a Criterion
harness at `benches/inner_loops.rs`. CI does not gate PRs on bench output —
Criterion's noise floor is too high for a binary pass/fail — but contributors
making perf-sensitive changes should run a local before/after comparison:

~~~sh
# Capture a baseline before your change:
cargo bench --bench inner_loops -- --save-baseline pre

# After your change, compare against `pre`:
cargo bench --bench inner_loops -- --baseline pre
~~~

Criterion will print mean/median/throughput deltas and HTML reports under
`target/criterion/`.

A starting-point baseline captured on `main` is committed under
`benches/baselines/main/` so contributors have a reference point without
rerunning the full suite. The fixture used for all three benchmark groups
lives at `benches/data/sample.jsonl` (representative Reddit JSONL).

The three benchmark groups:

- `for_each_line_cfg` — streams a precomputed `.zst` and counts lines,
  varying `read_buf_bytes` across 16K / 64K / 256K to surface buffer-size
  sensitivity.
- `matches_minimal` — runs `filters::matches_minimal` against subreddit
  target lists of size 1, 10, and 100 to track linear-scan cost as the list
  grows.
- `rewrite_human_timestamps_bytes` — measures the byte-level timestamp
  rewrite both on lines that contain all three timestamp keys (matching
  path) and on lines that contain none (no-match fast-skip).

---

## Environment variables

- `RUST_LOG` — standard `tracing` filter (e.g. `RUST_LOG=info`).
- `ETL_EXCLUDE_AUTHORS` — comma/semicolon/whitespace-separated authors to add
  to the default bot/service exclusion list.
- `ETL_EXCLUDE_AUTHORS_FILE` — path to a newline-separated file of additional
  authors to exclude.

The merged exclusion list is applied by `.exclude_common_bots()` on a
`ScanPlan`, or by passing `--exclude-common-bots` to scan-capable CLI commands
such as `scan`, `export`, `count`, `dedupe`, and `first-seen`. Explicit
`--exclude-author` / `.authors_out(...)` entries are merged with these defaults
instead of being overwritten.

---

## Fuzzing

RETL ships [cargo-fuzz](https://rust-fuzz.github.io/book/cargo-fuzz.html) targets
for the byte-facing paths that walk attacker-controlled JSONL lines:

- `retl::parse_minimal` (in `src/zstd_jsonl.rs`) — every JSONL line passes
  through this `serde_json::from_str` wrapper.
- `retl::rewrite_human_timestamps_bytes` (in `src/streaming.rs`) — a
  hand-rolled byte scanner that rewrites `"created_utc"`, `"retrieved_on"`,
  and `"edited"` integer values to RFC3339 strings without going through
  `serde_json::Value`.
- `retl::WhitelistTokenizer::tokenize_into` (in `src/json_whitelist.rs`) — a
  hand-rolled top-level object scanner that projects common Reddit fields for
  whitelist exports without a full `serde_json::Value` round-trip.

Setup (one-time):

~~~sh
cargo install cargo-fuzz
rustup toolchain install nightly
~~~

The `fuzz/` crate is pre-scaffolded — no `cargo fuzz init` needed. Targets
live under `fuzz/fuzz_targets/`, each seeded with a target-specific corpus at
`fuzz/corpus/<target>/`:

~~~sh
# Walks &str → parse_minimal; any panic/abort is a finding.
cargo +nightly fuzz run fuzz_parse_minimal -- -max_total_time=300

# Walks &str → rewrite_human_timestamps_bytes, then validates that
# (a) the output is valid UTF-8 (enforced by the String type),
# (b) the output round-trips through serde_json::from_str if the input did, and
# (c) on shallow top-level objects, the byte path matches the slow path
#     (apply_human_timestamps applied to the parsed Value).
cargo +nightly fuzz run fuzz_rewrite_timestamps -- -max_total_time=300

# Walks &str → WhitelistTokenizer::tokenize_into with a fixed Reddit-field
# whitelist, then validates no panic/abort, valid UTF-8 output, JSON
# round-tripping for valid JSON inputs, and projection equivalence for valid
# top-level objects.
cargo +nightly fuzz run fuzz_whitelist_tokenizer -- -max_total_time=300
~~~

The seed corpora include real Reddit-shaped JSONL lines plus adversarial
inputs: bodies that contain `\"created_utc\":` substrings (must be left
unchanged), `"`-escaped quotes around the keys, leading zeros / leading
`+` / lone `-`, very long integer strings, fractional and exponent forms,
`null`/`true`/`false` values, whitespace before and after the `:`, `i64`
extrema, empty/whitespace/non-object inputs, and a deliberate
`{...{"created_utc": ...}...}` nested-object case.

Triage:

- Any panic, abort, or OOM in any fuzz target is a bug — file as a separate
  task with the failing input.
- For the timestamp rewriter, a case where the byte path mutates a
  `"created_utc":` substring inside a JSON string-typed value (rather than
  leaving it alone) is a correctness bug — file separately.

Fuzzing is **not** wired into CI by default — it runs on demand under
nightly because libfuzzer is a nightly-only sanitizer.

---

## License

**MIT**. See `LICENSE` for details.

---

## Project Goals

- Provide an **intuitive, composable query system** that feels like a fluent DSL
- Keep **memory profile predictable** with adaptive buffering and backpressure
- Make **common workflows boringly easy** (extract, export, count, attach parents)
- Stay **robust on Windows** and networked filesystems via retry/backoff I/O
