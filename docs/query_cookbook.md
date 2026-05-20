# Query cookbook — composing `retl` queries

This doc is the cold-read reference for anyone (human or LLM) building a
multi-filter `retl` query. It complements the per-flag help in
`retl <subcmd> --help` by showing how filters **compose**, plus the
non-obvious semantics that bite first-time users.

The companion docs are:

- `docs/monitoring.md` — observability surface for long runs.
- `docs/default_bots.md` — what `--exclude-common-bots` actually excludes.

---

## 1. How filters compose

All filters on a single `retl` invocation are **AND-joined** at record level
(a record must pass every filter to be emitted). Within a single repeatable
filter the join depends on the flag:

| Flag (repeatable) | Within-flag join |
|---|---|
| `--subreddit`, `--id`, `--author`, `--domain` | OR — record matches if it equals *any* of the values. |
| `--keyword`, `--exclude-keyword` | OR — record matches if *any* keyword is found. |
| `--keyword-all` | AND — every entry must be found. |
| `--exclude-author` | OR — any match drops the record. |
| `--json` | AND — every predicate must hold. |

Across different flags, everything is AND. So
`--author alice --author bob --keyword rust --keyword tokio` means
`(author ∈ {alice, bob}) AND (body|selftext|title contains "rust" OR "tokio")`.

---

## 2. The semantic landmines (these have caused real bad queries)

### 2a. `--json` predicates drop missing-field records

Every operator except `exists:` evaluates to **false** when the JSON pointer
target is absent. That means:

```sh
# WRONG if you want "all records": records lacking /is_self are dropped.
retl export --source both --json '/is_self=true' ...
```

`is_self` only exists on submissions. With `--source both`, every comment
fails the predicate and is dropped — usually not what you meant.

Fix one of these ways:

```sh
# Option A — restrict to submissions explicitly.
retl export --source rs --json '/is_self=true' ...

# Option B — keep both sources, only filter when the field is present.
# (Requires two passes: there's no OR across --json predicates today.
# In practice, scan rs and rc separately and concatenate.)
retl export --source rs --json '/is_self=true' --out rs.jsonl
retl export --source rc                                --out rc.jsonl
cat rc.jsonl rs.jsonl > both.jsonl
```

Cheatsheet:

| Operator | On missing field |
|---|---|
| `exists:/path` | **true if any value present (including null)** |
| `/path=value` | false |
| `/path!=value` | false |
| `/path>N` etc. | false |
| `/path~=REGEX` | false |

So `--json 'exists:/is_self'` is **safe** for "records that have the field".

### 2b. `--contains-url` matches text URLs AND link-post URLs

`--contains-url` keeps a record if either:

- Its `body` / `selftext` / `title` contains an `http` substring
  (case-insensitive), OR
- (submissions only) its `url` field starts with `http`.

So `--contains-url --source rs` includes *self-posts that mention an URL in
their text*, not just link-posts. To get link-posts only:

```sh
# Submissions that are link-posts to an external URL (not self-posts).
retl export --source rs --contains-url --json '/is_self=false' ...
```

Or use a domain allow-list, which implicitly excludes self-posts because
`domain` is absent on those:

```sh
retl export --source rs --domain youtube.com --domain youtu.be ...
```

The inverse filter is `--no-url`: it keeps only records with **no** `http`
substring in `body` / `selftext` / `title` and (for submissions) no `http`
`url` field. `--no-url` and `--contains-url` are mutually exclusive — passing
both is rejected before any file is scanned.

```sh
# Comments with no link of any kind (text-only discussion).
retl export --source rc --no-url --out text_only_comments.jsonl

# Self-posts only: a submission with no link and no URL in its text.
retl export --source rs --no-url --out pure_self_posts.jsonl
```

### 2c. `--domain` is submissions-only and matches exact equality

With `--source both --domain example.com`, every *comment* is silently
dropped because comments have no `domain` field. The CLI prints a one-time
warning to stderr when this combination is detected.

For suffix or pattern matching, use `--json` regex instead:

```sh
# Submissions whose domain ends in ".gov"
retl export --source rs --json '/domain~=\.gov$' ...
```

### 2d. Subreddit names are normalized

`--subreddit Bitcoin`, `--subreddit bitcoin`, and `--subreddit r/Bitcoin` all
match the same records. The matcher lowercases and strips `r/`. Casing of
the source data is also normalized, so a record stored as `"subreddit": "Bitcoin"`
matches all three forms.

### 2e. Keyword search is substring, case-insensitive, across all three text fields

`--keyword cat` matches `concatenate`, `Cat in the hat`, and `CATSARECOOL`
in `body` / `selftext` / `title`. No word-boundary option exists. For
word boundaries, use `--text-regex '(?i)\bcat\b'`.

### 2f. Authors match case-insensitively but exactly

`--author Alice` matches records whose author is `"alice"`, `"ALICE"`, etc.
But it does **not** match `"alice123"` — the match is full-string, not
substring. Use `--author-regex '^alice.*'` for prefix matches.

---

## 3. Worked examples

Every example uses `--data-dir ./data --no-progress` for brevity; substitute
your real corpus path. All commands work on PowerShell and bash; quoting
differences are called out where they bite.

### 3a. All submissions by users matching `^crypto.*` in r/Bitcoin or r/Ethereum, 2020, score ≥ 100, link-post to an external URL, no bots

```sh
retl export \
  --data-dir ./data --source rs --no-progress \
  --start 2020-01 --end 2020-12 \
  --subreddit Bitcoin --subreddit Ethereum \
  --author-regex '^crypto.*' \
  --min-score 100 \
  --contains-url --json '/is_self=false' \
  --exclude-common-bots \
  --format jsonl --out crypto_submissions_2020.jsonl
```

Walk-through:

- `--source rs` — submissions only. Skip even reading RC files for the date range.
- `--start`/`--end` — month bounds (inclusive). For finer granularity use `--after`/`--before` with RFC3339 / `YYYY-MM-DD` / epoch seconds.
- `--subreddit X --subreddit Y` — OR within the flag.
- `--author-regex` — case-sensitive by default; use `(?i)` for case-insensitive.
- `--contains-url --json '/is_self=false'` — composes "URL anywhere AND not a self-post" → link-post to an outbound URL. `--json '/is_self=false'` would drop comments if `--source both` were used; safe here because we're already `rs`-only.
- `--exclude-common-bots` — see `docs/default_bots.md`.

### 3b. Distinct authors who posted ≥1 comment in 2021 containing BOTH "rust" AND "tokio", but NOT "javascript", with score ≥ 5

```sh
retl dedupe \
  --data-dir ./data --source rc --no-progress \
  --start 2021-01 --end 2021-12 \
  --keyword-all rust --keyword-all tokio \
  --exclude-keyword javascript \
  --min-score 5 \
  --key author --out rust_tokio_authors_2021.txt
```

Walk-through:

- `dedupe --key author` is the canonical "distinct authors" path. It uses the dedupe pipeline (sorted runs + merge) so the output is sorted and deduplicated even across very large corpora.
- `--keyword-all` repeated → both must be present.
- `--exclude-keyword javascript` → reject if substring present.
- Pseudo-users (`[deleted]`, `[removed]`, empty) are excluded by default. Pass `--include-deleted` to keep them.

### 3c. Count per month, full corpus, only `.gov`-domain link-posts

```sh
retl count \
  --data-dir ./data --source rs --no-progress \
  --mode month \
  --json '/is_self=false' --json '/domain~=\.gov$' \
  --out counts_gov_link_posts.tsv
```

Walk-through:

- `--source rs` — `is_self` and `domain` only exist on submissions. With `--source both`, every comment would fail both predicates and drop silently.
- Two `--json` predicates AND together: link-post AND `.gov`-suffix domain.
- `--mode month` writes TSV `YYYY-MM\tcount\n`, sorted by month.
- **PowerShell quoting**: prefer single quotes around the predicate (`'/domain~=\.gov$'`); double quotes will try to interpolate `$` as a variable.

### 3d. Find every post by a single user across the entire corpus (the "find this person" query)

```sh
retl export \
  --data-dir ./data --no-progress \
  --source both \
  --author somebody \
  --resume --allow-partial \
  --events ./events.ndjson --status-file ./status.json \
  --max-rss-mb 4096 \
  --format jsonl --out somebody.jsonl
```

Walk-through:

- `--author` goes through the `MinimalRecord` fast path (no full JSON parse), so this is one of the cheapest filters in the toolkit.
- `--resume` enables `_progress.json` / scan-checkpoint reuse so a killed run can pick up where it stopped.
- `--allow-partial` lets a corrupt monthly file be skipped instead of failing the entire scan; skipped paths are reported as JSON on stderr.
- `--events` / `--status-file` / `--max-rss-mb` — see `docs/monitoring.md`.

### 3e. Tabular CSV export with a fixed schema

```sh
retl export \
  --data-dir ./data --source rc --no-progress \
  --start 2020-01 --end 2020-12 \
  --keyword bitcoin \
  --format csv \
  --whitelist created_utc --whitelist author --whitelist subreddit --whitelist score --whitelist body \
  --human-timestamps \
  --out bitcoin_2020.csv
```

Walk-through:

- `--format csv` and `--format tsv` **require** `--whitelist` because the schema must be fixed before any record is read.
- `--whitelist` is repeatable; field order in the output matches the order of the flags.
- `--human-timestamps` rewrites Unix epochs in `created_utc` / `retrieved_on` to RFC3339.

### 3f. Per-author counts with a clever pre-filter (analytics)

```sh
retl count \
  --data-dir ./data --source both --no-progress \
  --start 2022-01 --end 2022-12 \
  --subreddit rust --subreddit golang \
  --exclude-common-bots \
  --mode author \
  --out top_rust_golang_2022.tsv
```

Walk-through:

- `--mode author` writes TSV `author\tcount\n`, sorted by author.
- Combine with `--top` post-processing (`Sort-Object` on PowerShell, `sort -k2 -nr | head` on bash) to get a leaderboard.

### 3g. JSON-pointer predicate gymnastics

```sh
# Non-empty controversiality field, score in [-5, 5]
retl export --source rc --no-progress \
  --json 'exists:/controversiality' \
  --json '/controversiality!=0' \
  --min-score -5 --max-score 5 \
  --format jsonl --out controversial_lite.jsonl
```

- `exists:` gates the more-specific predicate. Without it, missing-field records would be dropped silently. Using both is the safe pattern for fields that might not be present on every record.

### 3h. Subreddit-locked "find recent activity" with a stop-file watchdog

```sh
retl scan \
  --data-dir ./data --source both --no-progress \
  --subreddit askreddit \
  --after 2024-06-01 --before 2024-07-01 \
  --resume \
  --events ./events.ndjson --status-file ./status.json \
  --stop-file ./STOP --heartbeat-sec 5 \
  --out askreddit_users_jun24.txt
```

- `scan` emits unique usernames (vs. `dedupe` which can dedupe by any key). When you specifically want a username list, `scan` is the shortest path.
- `--after`/`--before` accept epoch seconds, `YYYY-MM-DD` (UTC midnight), or full RFC3339. **`--before` is exclusive**, `--after` is inclusive — pay attention when partitioning a year by month.
- `--stop-file STOP` — drop a file at that path to gracefully stop the run.

---

## 4. Performance hints relevant to query design

- Filters that read only `MinimalRecord` fields (`author`, `subreddit`, `created_utc`, `score`, `id`, `body`/`selftext`/`title`/`parent_id`/`domain`) take the fast path. `--json` predicates against any other field force a full `serde_json::Value` parse per record.
- Date predicates (`--start`/`--end`) prune at file-discovery time before any zstd frame is opened. Timestamp filters (`--after`/`--before`) prune at line-decode time. Both are essentially free.
- `--subreddit X` is *much* cheaper than `--text-regex '(?i)^.*subreddit.*$'`. Whenever a built-in flag covers what you want, prefer it over `--json` or `--text-regex`.
- `--source rc` / `--source rs` skips half the corpus at file-discovery time. Use it whenever the query is logically one-sided.

See `README.md`'s "Performance and tuning" section for the knobs
(`--inflight-bytes`, `--inflight-groups`, `--file-concurrency`,
`--parallelism`).

---

## 5. When to reach for the library API instead

The CLI exposes the common 95% of filters. For everything else
(`Vec<JsonPointerPredicate>` with arbitrary value-level predicates,
custom keyword tokenizers, per-record `KeyExtractor` closures), drop into
the `RedditETL` builder in Rust. See `examples/quickstart.rs` for the
shape, and `src/lib.rs` rustdoc for the full API.
