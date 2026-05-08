# RETL

A Rust library for streaming ETL over the Reddit monthly comment (`RC`) and
submission (`RS`) zstd-compressed JSONL dumps. RETL scans the monthly archives
in a single pass, applies filters via a builder API, and writes results to
JSONL, ZST, or TSV.

## Why RETL

The Reddit monthly dumps are large (tens of GB compressed per month in later
years) and are distributed as one `.zst`-compressed JSONL file per month. Most
analyses don't need the records in memory at once — they need to filter,
count, or re-export them.

RETL is built around that workflow:

- Streams records line-by-line; memory use does not scale with corpus size.
- Filters (subreddit, author, score, keywords, regex, URL/domain) run during
  decompression so the unmatched majority is dropped before allocation.
- Parallelizes across files with a configurable concurrency cap, so wide date
  ranges do not exhaust RAM or file descriptors.
- Atomic writes (`*.inprogress` → rename) so a killed run never leaves
  partial output files behind.
- Tested on Windows: file operations retry with backoff to tolerate the
  short-lived sharing-violation errors common with antivirus and indexers.

## Data layout

RETL expects a base directory with two subdirectories of monthly files:

- [Data Layout](#data-layout)
- [Install](#install)
- [Command-Line Interface](#command-line-interface)
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

```text
<base_dir>/
  comments/
    RC_YYYY-MM.zst
  submissions/
    RS_YYYY-MM.zst
```

Filenames must match `RC_\d{4}-\d{2}\.zst` and `RS_\d{4}-\d{2}\.zst`. Files
that do not match are ignored at discovery time.

Monthly dumps are available via Academic Torrents:
https://academictorrents.com/browse.php?search=reddit

## Install

RETL is not published to crates.io. Add it to your `Cargo.toml` from Git:

```toml
[dependencies]
retl = { git = "https://github.com/sjlynch/retl", branch = "main" }
```

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

The `retl` binary exposes five subcommands. All accept a shared set of flags
(see `retl <subcommand> --help` for the full list):

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

### `aggregate` — fold JSONL inputs into one JSON

Aggregates one or more JSONL inputs into a single JSON file using a built-in
record-count aggregator (each input is processed in parallel; per-input shard
intermediates land under `--shards-dir`, which defaults to `agg_shards/`
next to `--out`).

~~~sh
retl aggregate \
  --out agg.json --pretty \
  ./spool/part_RC_2006-01.jsonl ./spool/part_RS_2006-01.jsonl
~~~

For more interesting aggregations, implement `retl::Aggregator` for your own
type and call `RedditETL::aggregate_jsonls_parallel::<MyAgg>(...)` from a
small driver — the CLI ships only the simple record-count case.

---

## Quick start

Place the corpus under `./data/`:

```
./data/comments/RC_2006-01.zst
./data/submissions/RS_2006-01.zst
```

Count matching records by month:

```rust
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
        .count_by_month()?;

    for (ym, n) in counts {
        println!("{ym}\t{n}");
    }
    Ok(())
}
```

## API overview

Every operation follows the same two-stage builder:

1. `RedditETL::new()` configures the corpus and execution options
   (`base_dir`, `sources`, `date_range`, `parallelism`, `file_concurrency`,
   `io_buffers`, `work_dir`, `progress`).
2. `.scan()` enters query mode where filters (`subreddit`, `authors_in`,
   `authors_out`, `min_score`, `keywords_any`, `domains_in`, `contains_url`,
   `author_regex`) and a terminal operation are chained.

Terminal operations:

| Method                              | Output                                                    |
| ----------------------------------- | --------------------------------------------------------- |
| `count_by_month()`                  | `BTreeMap<YearMonth, u64>`                                |
| `usernames()`                       | Streaming iterator of deduped author names                |
| `for_each_username(\|u\| ...)`      | Callback variant of `usernames()`                         |
| `extract_to_jsonl(path)`            | Single stitched JSONL file                                |
| `extract_to_json(path, pretty)`     | Single JSON array file                                    |
| `extract_spool_monthly(dir)`        | One JSONL per month (input for the parents pipeline)      |
| `export_partitioned(dir, format)`   | Re-exports filtered corpus partitioned by month/source    |
| `author_counts_to_tsv(path)`        | TSV of `author<TAB>count`                                 |
| `build_first_seen_index_to_tsv(p)`  | TSV of `author<TAB>earliest_created_utc`                  |
| `check_corpus_integrity(mode)`      | List of `(path, error)` for files that failed validation  |

## Examples

### Extract filtered records to JSONL

`whitelist_fields` keeps only the named fields per record.
`timestamps_human_readable(true)` rewrites `created_utc` (and similar fields)
to ISO-8601 UTC strings.

```rust
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
        "author", "body", "created_utc", "subreddit",
        "parent_id", "link_id", "id", "score",
    ])
    .timestamps_human_readable(true)
    .extract_to_jsonl(Path::new("askscience_q1_2016.jsonl"))?;
```

### Re-export a filtered subset as a partitioned corpus

`ExportFormat::Zst` writes recompressed `.zst` files; `ExportFormat::Jsonl`
writes plain JSONL. The output directory mirrors the input layout.

```rust
use retl::{ExportFormat, RedditETL, Sources, YearMonth};
use std::path::Path;

RedditETL::new()
    .base_dir("./data")
    .sources(Sources::Both)
    .date_range(Some(YearMonth::new(2016, 1)), Some(YearMonth::new(2016, 1)))
    .progress(true)
    .scan()
    .subreddit("programming")
    .allow_pseudo_users() // include "[deleted]"
    .export_partitioned(Path::new("out_corpus_zst"), ExportFormat::Zst)?;
```

### Stream usernames

```rust
use retl::{RedditETL, Sources, YearMonth};

let mut authors = RedditETL::new()
    .base_dir("./data")
    .sources(Sources::Both)
    .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
    .scan()
    .subreddit("programming")
    .keywords_any(["rust"])
    .contains_url(true)
    .min_score(2)
    .usernames()?;

while let Some(u) = authors.next() {
    println!("{u}");
}
```

### Author analytics

```rust
use retl::{RedditETL, Sources, YearMonth};
use std::path::Path;

let etl = RedditETL::new()
    .base_dir("./data")
    .sources(Sources::Both)
    .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)));

etl.clone()
    .scan()
    .subreddit("programming")
    .author_counts_to_tsv(Path::new("author_counts.tsv"))?;

etl.scan()
    .subreddit("programming")
    .build_first_seen_index_to_tsv(Path::new("first_seen.tsv"))?;
```

### Parents pipeline

Comments reference their parent by `parent_id` (`t1_<id>` for a comment,
`t3_<id>` for a submission). The parents pipeline resolves those references
and attaches a `"parent"` object to each record.

The pipeline runs in three passes so each stage can be parallelized and
resumed independently:

1. Spool filtered records to per-month JSONL files.
2. Collect parent IDs from the spool.
3. Resolve parent IDs against the corpus, then attach the parent payloads.

```rust
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

let ids: ParentIds = RedditETL::new()
    .base_dir("./data")
    .progress(true)
    .collect_parent_ids_from_jsonls(spool_parts.clone())?;

// Widen the date range here so parents written shortly before the
// comment's month are still resolvable.
let parents: ParentMaps = RedditETL::new()
    .base_dir("./data")
    .date_range(Some(YearMonth::new(2005, 12)), Some(YearMonth::new(2006, 2)))
    .progress(true)
    .resolve_parent_maps(&ids, Path::new("parents_cache"), /* resume */ true)?;

RedditETL::new()
    .base_dir("./data")
    .progress(true)
    .attach_parents_jsonls_parallel(
        spool_parts,
        Path::new("spool_with_parents"),
        &parents,
        /* resume */ false,
    )?;
```

`resume: true` skips work whose output already exists, which makes the
pipeline restartable after an interruption.

### Integrity checks

`Quick` decodes only the first `sample_bytes` of each file (catches early
corruption fast); `Full` decodes the entire stream and verifies checksums.

```rust
use retl::{IntegrityMode, RedditETL, Sources, YearMonth};

let bad = RedditETL::new()
    .base_dir("./data")
    .sources(Sources::Comments)
    .date_range(Some(YearMonth::new(2006, 2)), Some(YearMonth::new(2006, 2)))
    .check_corpus_integrity(IntegrityMode::Quick { sample_bytes: 64 * 1024 })?;

for (path, err) in bad {
    eprintln!("{}: {err}", path.display());
}
```

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

## Environment variables

- `RUST_LOG` — standard `tracing` filter (e.g. `RUST_LOG=info`).
- `ETL_EXCLUDE_AUTHORS` — comma-separated authors to add to the default
  bot/service exclusion list.
- `ETL_EXCLUDE_AUTHORS_FILE` — path to a newline-separated file of additional
  authors to exclude.

The merged exclusion list is applied by `.exclude_common_bots()` on a
`ScanPlan`.

## License

MIT. See `LICENSE`.
