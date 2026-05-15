# RETL (Reddit ETL)

**RETL** is a fast, memory‑aware, streaming ETL toolkit for working with the Reddit monthly **RC** (comments) and **RS** (submissions) corpora. It’s designed to scan large `.zst` JSONL drops efficiently, filter with an intuitive query builder, and export results for analysis — all with pragmatic attention to parallelism, backpressure, and Windows‑friendly file operations.

> TL;DR: Point RETL at a `comments/` and `submissions/` directory full of `RC_YYYY-MM.zst` / `RS_YYYY-MM.zst` files, build a query, then extract or analyze in one pass.

---

## Features

- 🚀 **Streaming, single‑pass processing** of `.zst` JSONL monthly dumps (RC/RS)
- 🧠 **Intuitive query builder (DSL)**: subreddits, authors (allow/deny), regex, Unicode-aware keywords, URL presence, domains, score thresholds, and arbitrary JSON-pointer predicates
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
The monthly files become very large in later years. It’s normal for broader queries to run longer and consume significant I/O. RETL’s streaming design and throttling aim to keep resource use predictable; tune `.file_concurrency(n)`, `.parallelism(n)`, and `.io_buffers(...)` as appropriate for your hardware and dataset size. Oversized resource knobs are clamped with a warning rather than allowed to create unbounded threads, zstd decoders, or shard files.

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
| `--subreddit <NAME>` (`-s`) | Subreddit selector. Repeatable; omit for "any subreddit". Blank values are rejected. |
| `--author <NAME>` / `--author-in <NAME>` | Author allow-list selector. Repeatable. Blank values are rejected. |
| `--exclude-author <NAME>` | Author deny-list selector. Repeatable. Blank values are rejected. |
| `--exclude-common-bots` | Exclude RETL's built-in bot/service-account list plus `ETL_EXCLUDE_AUTHORS*` augments; composes with `--exclude-author`. |
| `--author-regex <REGEX>` | Keep authors matching a regex. |
| `--keyword <TEXT>` | Keep records containing a keyword in body/title/selftext. Repeatable; blank values are rejected. |
| `--min-score <N>` / `--max-score <N>` | Inclusive score thresholds. |
| `--contains-url` | Keep records with an HTTP(S) URL in text or submission URL. This is a positive-only filter. |
| `--domain <DOMAIN>` | Submission-domain allow-list. Repeatable; comments are dropped when this filter is active. Blank values are rejected. |
| `--json <PREDICATE>` | Full-record JSON Pointer predicate. Repeatable. Examples: `exists:/link_flair_text`, `/over_18=false`, `/is_self=true`, `/num_comments>=100`, `/link_flair_text~=^Question` (quote predicates containing `>` or `<` in shells). |
| `--include-deleted` | Include pseudo-users (`[deleted]`, `[removed]`, and empty authors) that are filtered by default. |
| `--parallelism <N>` / `--file-concurrency <N>` | Rayon threads / concurrent monthly files; oversized values are clamped to RETL's documented safety caps. |
| `--no-progress` | Disable progress bars. |
| `--no-manifest` | Do not write provenance sidecars next to outputs. |

### Provenance manifests

By default, user-facing file outputs get a `<output>.retl-manifest.json` sidecar and directory outputs (spool, partitioned exports, parents output) get `<out_dir>/_retl_manifest.json`. The manifest records the RETL version (and build git hash when provided), operation/API surface, normalized query/options, selected corpus file identities (path, kind, month, size/mtime), output path/format, counts, partial-read skips, resume/checkpoint fingerprint when relevant, timestamps, warnings, and upstream spool manifest links used by downstream parents/aggregate flows.

Manifests intentionally contain local filesystem paths to make runs auditable. Treat them as reproducibility artifacts: redact or omit them before sharing if paths reveal private directory names. Use `--no-manifest` on the CLI or `.run_manifest(false)` on `RedditETL` to disable sidecar emission.

### Pseudo-user filtering (default ON)

By default, scans exclude records whose `author` is `[deleted]`, `[removed]`, or the empty string. This keeps normal username/export queries focused on real author names, but it matters for deletion-rate, ban-wave, or corpus-completeness analysis. Pass `--include-deleted` (alias: `--include-pseudo-users`) on the CLI, or call `.include_pseudo_users()` on a `ScanPlan`, to keep those records. Bot/service-account exclusions are separate: use `--exclude-common-bots` or `.exclude_common_bots()` to drop accounts such as AutoModerator without changing pseudo-user handling.

### Date ranges and missing months

When `--start` or `--end` is set, RETL filters individual records by the same inclusive month bounds it uses for file planning. Records without `created_utc` are dropped while any date bound is active. If files are missing inside the requested range (for example Jan and Mar exist but Feb is absent), corpus-scanning commands emit a warning and `retl describe` reports the missing month list. Discovery errors (for example a `comments/` path that is a file or cannot be read) fail fast with the directory name; filenames like `RC_2024-00.zst` or `RS_2024-99.zst` are warned and skipped because their months are invalid.

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

### Analyst-facing exports & exploration

Start with a cheap preview before launching a long scan:

~~~sh
retl sample --data-dir ./data --source rc --subreddit programming --limit 10 --out -
~~~

`retl sample` (aliases: `preview`, `head`) defaults to `--limit 10 --format jsonl --out -` and accepts the same query flags as `export`. `retl export` and `retl scan` also accept `--limit N` (alias `--head N`); the streaming loop stops as soon as the shared limit is reached. With `--file-concurrency > 1`, workers already decoding a monthly file may produce a small bounded over-shoot.

Discover fields without decoding a whole month:

~~~sh
retl schema --data-dir ./data --source rs --start 2018-01 --end 2018-12 --sample 100
# field  type    presence_pct  present_records  sampled_records
~~~

`retl describe --schema` is equivalent and honors `--start`, `--end`, and `--source`. Schema output is TSV by default; pass `--format json` (or `describe --schema-format json`) for tooling.

Export directly to spreadsheet/duckdb-friendly delimited text:

~~~sh
retl export \
  --data-dir ./data \
  --source rs \
  --start 2020-01 --end 2020-12 \
  --subreddit rust \
  --format csv \
  --whitelist id,author,created_utc,subreddit,score,title,domain \
  --out rust_submissions_2020.csv
~~~

CSV uses standard doubled-quote escaping and CRLF row endings. TSV uses literal tab separators and refuses values containing tabs; use CSV when fields may contain arbitrary text. Missing whitelisted fields render as empty cells.

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

Records that match the query but do not contain the extracted key are omitted
from the dedupe output (for example, `--key json:/parent_id --source both`
drops submissions because they have no `parent_id`). The CLI prints a summary
with the drop count and warns when more than 1% of matching records lack the
key. Add `--strict-key` to make any missing key a hard error instead.

### `export` — extract filtered records

Formats:

* `--format jsonl` → single stitched `.jsonl` file (default).
* `--format json`  → single `.json` file containing a JSON array (`--pretty`
  field-indents records, matching `aggregate --pretty`).
* `--format csv` → single RFC4180-style CSV file. Requires `--whitelist` to define the fixed column order; missing fields render as empty cells.
* `--format tsv` → single tab-separated file. Requires `--whitelist`; values containing literal tabs are rejected with a warning because TSV has no standard escaping.
* `--format spool` → per-source per-month files (`part_RC_YYYY-MM.jsonl`, `part_RS_YYYY-MM.jsonl`) under the directory passed to `--out`. Use this for the parents-pipeline workflow.
* `--format zst` → corpus-style partitioned `.zst` output under `<out>/comments/RC_YYYY-MM.zst` and `<out>/submissions/RS_YYYY-MM.zst`.
* `--format partitioned-jsonl` → the same corpus-style directory layout, but as uncompressed `.jsonl` files.

Export-only modifiers include `--whitelist a,b,c`, `--strict-whitelist`, `--human-timestamps`, `--limit N`, `--zst-level <N>`, and `--resume`. With `--resume`, `jsonl`/`json` exports checkpoint per-month `.part_*.jsonl` files under `--work-dir`; `spool`, `zst`, and `partitioned-jsonl` use `_progress.json` under `--out`. The checkpoint includes a fingerprint of the query and output-affecting config; changing filters, sources, date range, whitelist fields, `--limit`, `--human-timestamps`, or (for ZST) `--zst-level` discards stale parts instead of mixing results from different runs. Partitioned ZST resume validates completed `.zst` outputs with a full decode before skipping them.

Corpus scans and exports are strict by default: zstd decode errors fail the command instead of returning plausible partial results. Pass `--allow-partial` to preserve the explicit lossy mode; skipped file counts and paths are emitted as a JSON object on stderr, and skipped months are not committed to resume manifests.

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

# JSON-pointer predicates on full records
retl export \
  --data-dir ./data \
  --source rs \
  --start 2020-01 --end 2020-12 \
  --json '/over_18=false' \
  --json '/is_self=true' \
  --json '/num_comments>=25' \
  --format jsonl \
  --out self_posts_with_discussion.jsonl

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
# Quick: decode the first 64 KiB (decompressed) per file
retl integrity --mode quick --sample-bytes 65536 --source rc --start 2006-02 --end 2006-02

# Full: decode every byte (validates trailing checksum)
retl integrity --mode full --source both --start 2006-01 --end 2006-04
~~~

Quick mode validates only the first `--sample-bytes` decompressed bytes of each
file. `--sample-bytes` must be positive (default: 65536); very small samples
(for example, below 4096 bytes) only cover a tiny prefix and emit a warning. Use
`--mode full` for complete payload/trailer validation.

Bad files print one `path<TAB>error` line per failure on stdout as soon as
they are discovered, and the process exits with status `2`. Pass `--collect`
to buffer the failure list and print it only after all files finish.

### `aggregate` — fold JSONL inputs into JSON or TSV rollups

Aggregates one or more already-filtered JSONL inputs using the
`retl::Aggregator` pipeline (each input is processed in parallel; per-input
shard intermediates land under `--shards-dir`, which defaults to `agg_shards/`
next to `--out`). `aggregate` does not scan the RC/RS corpus and does not
accept corpus selectors such as `--data-dir`, `--start`, or `--subreddit`; run
`retl export --format spool ...` first if you need to filter the corpus. Its
runtime flags are limited to `--parallelism`, `--no-progress`, and
`--shards-dir`. Use `--spool DIR` to discover `part_RC_YYYY-MM.jsonl` /
`part_RS_YYYY-MM.jsonl` spool parts chronologically (cross-platform and
recommended); explicit JSONL file paths are still accepted for advanced use,
but cannot be combined with `--spool`. With no `--by`, the CLI keeps the
original built-in record-count fallback and writes a JSON aggregate state:

~~~sh
retl aggregate --spool ./spool --out agg.json --pretty
~~~

Built-in grouped rollups write two-column TSV:

~~~sh
retl aggregate --spool ./spool --by subreddit --out counts.tsv
retl aggregate --spool ./spool --by month --out months.tsv
retl aggregate --spool ./spool --by author --top 100 --out top_authors.tsv
retl aggregate --spool ./spool --by 'json:/subreddit' --metric 'sum:/score' --out scores.tsv
~~~

`--pretty` field-indents the final JSON when `--by` is omitted, matching
`export --format json --pretty`. Grouped TSV metrics render integer-valued
numbers as plain decimal strings by default (for example large `sum:/score`
values do not use scientific notation); pass `--scientific` to opt back into
Rust's default `f64` formatting.

`--metric` defaults to `count` and also supports `avg:/pointer`,
`min:/pointer`, and `max:/pointer` for numeric JSON-pointer values.

Partial-read policy: if a JSONL input hits a mid-file read error, `aggregate`
reports that path on stderr, drops that partial shard from the merged result,
and continues with other inputs. Inputs that fail to open, contain malformed
JSON, or fail shard publishing are reported separately as fatal build failures.
If every input fails or is partial, the CLI exits non-zero and publishes no
final output.

---

## Quick CLI start

If you just want to drive RETL from a shell, build the binary once and use
the subcommands directly:

~~~sh
cargo build --release
./target/release/retl --help
./target/release/retl export --help
~~~

Preview a few matching records first:

~~~sh
./target/release/retl sample \
  --data-dir ./data \
  --start 2006-01 --end 2006-01 \
  --subreddit programming \
  --limit 10
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
  `url` starts with `http`/`https`. `.contains_url(false)` clears/disables
  that positive filter and is equivalent to omitting it; RETL does not yet
  provide a negative "without URL" filter.
- `.domains_in([...])` matches the submission-only top-level `domain` field.
  Reddit comments do not have `domain`; when used with `Sources::Both` or
  `Sources::Comments`, comments are dropped and RETL emits a warning. Use
  `Sources::Submissions` when you intend a domain-only scan.
- Empty explicit lists are invalid; omit a filter to match all values for that
  field. Blank normalized entries in `.subreddits(...)`, `.authors_in(...)`,
  `.authors_out(...)`, `.domains_in(...)`, and `.keywords_any(...)` return a
  `QueryBuildError` before any corpus file is scanned.
- `.keywords_any([...])` is case-insensitive for Unicode text too: ASCII-only
  keyword/haystack pairs stay on the zero-allocation Aho-Corasick fast path,
  while non-ASCII keywords or text fields use a lowercase fallback.
- `.json_exists("/path")`, `.json_eq("/path", value)`, `.json_number_gte(...)`,
  and `.json_regex("/path", pattern)` filter on arbitrary JSON Pointer fields.
  These predicates opt that query into full-record parsing only when present.
  CLI syntax mirrors the API: `--json exists:/link_flair_text`,
  `--json '/over_18=false'`, `--json '/is_self=true'`, and
  `--json '/num_comments>=100'`.
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
    // Optional: request more context than the legacy body/title/selftext payload.
    .parent_fields(["author", "body", "score", "created_utc", "subreddit", "title", "selftext"])
    .resolve_parent_maps(&ids, Path::new("parents_cache"), resume)?;

// Step 4: Attach parent payloads; resume skips outputs that already exist
let _out_paths = RedditETL::new()
    .base_dir("./data")
    .progress(true)
    .attach_parents_jsonls_parallel(spool_parts, Path::new("spool_with_parents"), &parents, resume)?;
~~~

By default, resolved comments receive a `"parent"` object containing either the parent comment’s body (`t1_...`) or the submission’s title/selftext (`t3_...`). Use `.parent_fields([...])` or CLI `--parent-fields author,body,score,created_utc,subreddit,domain,url,title,selftext` to attach extra top-level parent fields; use `.parent_full(true)` / `--parent-full` to attach the full parent JSON record. `kind` and `id` are always included for resolved parents. If a referenced parent cannot be resolved from the cache/window, `retl` leaves the `"parent"` key absent rather than writing an empty object; the CLI reports resolved/unresolved totals and warns when more than 5% are unresolved.

If you already have parent IDs from SQL/Python, build `ParentIds` directly instead of writing a fake spool:

~~~rust
let mut ids = ParentIds::new();
ids.extend_prefixed(["t1_comment_id", "t3_submission_id"]); // validates prefixes, de-dupes
ids.insert_t1("bare_comment_id");
ids.insert_t3("bare_submission_id");

let parents = RedditETL::new()
    .base_dir("./data")
    .date_range(Some(YearMonth::new(2005, 10)), Some(YearMonth::new(2006, 4)))
    .parent_fields(["author", "body", "score", "created_utc", "subreddit", "title", "selftext"])
    .resolve_parent_maps(&ids, Path::new("parents_cache"), true)?;
~~~

Note: extract/spool resume entries are fingerprinted by query/config. Parent-cache and attach resume sidecars include the parent ID set, selected parent fields, payload format, corpus file identity, and resolution window, so widening `--parent-fields` rebuilds stale narrow cache shards instead of reusing them.

The `parents` CLI uses `--window-months 3` by default, scanning three extra months on each side of the spool range. Larger windows catch more old cross-month parents, but scan more corpus bytes and create/use more parent-cache shard files; smaller windows are faster and lighter but can leave more parents unresolved.

### Integrity Checks

Quick sampling validates only the first positive `sample_bytes` decompressed
bytes per file (fast, prefix-only); full decode validates the complete stream
(slow but thorough):

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

// The return value materializes the full failure list. For long runs, stream
// failures incrementally while still receiving the final Vec:
let bad_streamed = RedditETL::new()
    .base_dir("./data")
    .sources(Sources::Comments)
    .date_range(Some(YearMonth::new(2006, 2)), Some(YearMonth::new(2006, 2)))
    .progress(false)
    .check_corpus_integrity_with_failure_sink(IntegrityMode::Full, |path, err| {
        println!("{}\t{}", path.display(), err);
        Ok(())
    })?;
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
| ≥ 128 GB | 8 (the built-in maximum), then tune other bottlenecks |

### Tuning knobs

- `.parallelism(n)` — Rayon worker threads. CPU-bound work (decompression,
  parsing) scales with this up to physical core count. RETL clamps requests to
  a conservative runtime limit (a small multiple of available CPUs, never above
  `MAX_RAYON_THREADS = 256`) and logs a warning when it clamps.
- `.file_concurrency(n)` — number of monthly files decoded in parallel. The
  per-file working set is large; raising this is the fastest way to use up
  RAM because every in-flight Reddit zstd frame can reserve a multi-GiB decode
  window. RETL clamps this to `MAX_FILE_CONCURRENCY = 8`.
- `.shard_count(n)` — number of open-all-writer scratch shards for username
  and key/value reductions. RETL clamps this to `MAX_SHARDS = 256` so a typo
  cannot open millions of files or allocate millions of writer buffers.
- `.inflight_bytes(bytes)` — per-flush byte budget for the bucketing/dedupe
  producer (`per_flush_cap = inflight_bytes / 2`). On its own this caps one
  in-memory map; the bucketing channel adds more buffered groups on top
  (see below). CLI subcommands expose this as `--inflight-bytes`.
- `.inflight_groups(n)` — bounded-channel depth for bucketing group handoff
  (default `8`). The dedupe stage hard-codes channel capacity to 1, so this
  only affects bucketing. Lower it to queue fewer groups; raise only when
  the consumer is bursty and you have measured RAM headroom.
- `.inflight_budget(bytes)` — helper that sets `inflight_bytes = bytes` and
  `inflight_groups = 1`, so the worst-case bucketing peak is bounded by the
  declared value. Prefer this when the budget you pass should be the actual
  RAM ceiling.

> **These two knobs are not independent.** The bucketing worst-case peak is
>
> ```text
> peak ≈ (1 + inflight_groups) * (inflight_bytes / 2)
> ```
>
> With the defaults (`inflight_bytes = 256 MiB`, `inflight_groups = 8`) the
> bucketing peak is ≈ 1.125 GiB, not 256 MiB. The dedupe stage pins channel
> capacity to 1, so its peak is ≈ `inflight_bytes`. `retl` emits a one-shot
> `tracing::warn!` (visible when `RETL_LOG`/`RUST_LOG` is enabled) if the
> configured pair would exceed roughly 2× the declared `inflight_bytes`.
> Use `.inflight_budget(bytes)` to set both together, or lower
> `--inflight-groups` if you need the declared budget to be the ceiling.
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
  uniquely named `.inprogress` files. Point this at fast local storage if the
  corpus lives on a network share.
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
  authors to exclude. If this variable is set to a non-blank path, the file
  must be opened and read completely as UTF-8; missing/unreadable/invalid files
  are fatal build errors before scanning starts.

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
