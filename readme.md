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

```
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

The repo also includes a small example binary in `src/main.rs` that collects
authors over a small date window. The library API is the intended entry point.

```sh
cargo build --release
./target/release/retl
```

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

## Benchmarks

The hot inner loops touched by perf work (record filtering, byte-level
timestamp rewrite, and zstd line streaming) are covered by a Criterion
harness at `benches/inner_loops.rs`. CI does not gate PRs on bench output —
Criterion's noise floor is too high for a binary pass/fail — but contributors
making perf-sensitive changes should run a local before/after comparison:

```sh
# Capture a baseline before your change:
cargo bench --bench inner_loops -- --save-baseline pre

# After your change, compare against `pre`:
cargo bench --bench inner_loops -- --baseline pre
```

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
