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

The `retl` binary exposes the main ETL subcommands. All accept a shared set of
flags (see `retl <subcommand> --help` for the full list):

| Common flag | Purpose |
| --- | --- |
| `--data-dir <PATH>` | Corpus base dir (default `./data`). Must contain `comments/` and `submissions/`. |
| `--work-dir <PATH>` | Scratch directory for sharded writers (default `./etl_work`). |
| `--start <YYYY-MM>` / `--end <YYYY-MM>` | Inclusive month range. Omit either to leave that side unbounded. |
| `--source rc\|rs\|both` | Comments only, submissions only, or both (default). |
| `--subreddit <NAME>` (`-s`) | Subreddit selector. Repeatable; omit for "any subreddit". |
| `--whitelist a,b,c` | Restrict export to the listed JSON fields (comma-separated). |
| `--human-timestamps` | Emit `created_utc` as RFC3339 strings. |
| `--parallelism <N>` / `--file-concurrency <N>` | Rayon threads / concurrent monthly files. |
| `--no-progress` | Disable progress bars. |

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

Three formats:

* `--format jsonl` → single stitched `.jsonl` file (default).
* `--format json`  → single `.json` file containing a JSON array (`--pretty` for pretty-print).
* `--format spool` → per-source per-month files (`part_RC_YYYY-MM.jsonl`, `part_RS_YYYY-MM.jsonl`) under the directory passed to `--out`. Use this for the parents-pipeline workflow.

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

# Spool monthly parts (input for parents pipeline / aggregation)
retl export --start 2006-01 --end 2006-01 -s programming --format spool --out ./spool
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

Aggregates one or more JSONL inputs using the `retl::Aggregator` pipeline
(each input is processed in parallel; per-input shard intermediates land under
`--shards-dir`, which defaults to `agg_shards/` next to `--out`). With no
`--by`, the CLI keeps the original built-in record-count fallback and writes a
JSON aggregate state:

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
    .extract_spool_monthly(Path::new("spool"))?;

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

## Performance and tuning

Defaults are conservative. The knobs below are the ones that matter on large
corpora:

- `.parallelism(n)` — Rayon worker threads. CPU-bound work (decompression,
  parsing) scales with this up to physical core count.
- `.file_concurrency(n)` — number of monthly files decoded in parallel. The
  per-file working set is large; raising this is the fastest way to use up
  RAM. Start at 4–8.
- `.io_buffers(read, write)` — read/write buffer sizes in bytes. The default
  is fine for SSD-backed local storage; increase to 1–4 MiB on networked
  filesystems.
- `.work_dir(path)` — scratch directory for intermediate shards and
  `.inprogress` files. Point this at fast local storage if the corpus lives
  on a network share.
- `.progress(true)` and `.progress_label("...")` — render an `indicatif`
  progress bar.

RETL throttles cooperatively when system memory falls below a threshold; you
do not need to manage this manually.

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
- `ETL_EXCLUDE_AUTHORS` — comma-separated authors to add to the default
  bot/service exclusion list.
- `ETL_EXCLUDE_AUTHORS_FILE` — path to a newline-separated file of additional
  authors to exclude.

The merged exclusion list is applied by `.exclude_common_bots()` on a
`ScanPlan`.

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
