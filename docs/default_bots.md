# `--exclude-common-bots` — what's actually excluded

`--exclude-common-bots` applies the curated default list **plus** any names
supplied via `ETL_EXCLUDE_AUTHORS` and `ETL_EXCLUDE_AUTHORS_FILE`. Env-supplied
names are **additive** (merged with the defaults, not replacing them).

All names are normalized to lowercase before matching; the record's `author`
field is also lowercased at match time, so casing of the source data is
irrelevant.

## Curated default list

The 12-entry built-in list, current as of this file. Defined in
`src/util/exclusions.rs::default_bot_authors`.

| Author | Notes |
|---|---|
| `automoderator` | Reddit's per-subreddit AutoModerator |
| `imguralbumbot` | Imgur album expander |
| `autowikibot` | Wikipedia summary bot |
| `remindmebot` | Reminder service bot |
| `totesmessenger` | Cross-post notification bot |
| `tweet_poster` | Twitter mirror bot |
| `video_link_bot` | Video link expander |
| `gifvbot` | gifv-to-mp4 converter bot |
| `helper-bot` | Generic helper alias used by several subs |
| `github-actions[bot]` | GitHub Actions (rarely on Reddit, kept for safety) |
| `slackbot` | Slack mirror bot |
| `discordbot` | Discord mirror bot |

If you need a different baseline, **don't edit this file** — use
`ETL_EXCLUDE_AUTHORS_FILE` to extend it for a single run, or open a PR
adding the new name to `default_bot_authors` if it's a widely-deployed bot
worth shipping in the default set.

## `ETL_EXCLUDE_AUTHORS` — inline names

Comma, semicolon, or whitespace separated. Blank entries are discarded.

```powershell
$env:ETL_EXCLUDE_AUTHORS = "spam_bot_42, repostsleuthbot; another_bot"
retl scan --data-dir .\data --exclude-common-bots ...
```

```bash
ETL_EXCLUDE_AUTHORS="spam_bot_42, repostsleuthbot another_bot" \
  retl scan --data-dir ./data --exclude-common-bots ...
```

All separators are equivalent; you can mix them freely. Names are normalized
(lowercased, `r/` not applicable here) before merging.

## `ETL_EXCLUDE_AUTHORS_FILE` — file of names

Newline-separated. Blank lines are discarded. **Per-line reads are bounded by
`DEFAULT_MAX_LINE_BYTES`** (~1 MiB), so an adversarial input file cannot
OOM the process. **Failure to open or read the file is fatal** — the file
is treated as part of the query semantics, not a soft hint, so a missing or
unreadable path errors the run before any data is scanned.

```bash
cat > my_bots.txt <<EOF
# This file is read line-by-line; lines starting with '#' are
# NOT special — they will be treated as a literal author name
# (i.e., probably matching nothing). Don't put comments here.
spam_bot_42
repostsleuthbot
another_bot
EOF

ETL_EXCLUDE_AUTHORS_FILE=./my_bots.txt \
  retl scan --data-dir ./data --exclude-common-bots ...
```

Caveat: unlike `--ids-file`, the bot exclusions file does **not** strip
`#` comment lines. If you keep your bot list under version control,
maintain it as a plain newline-separated list with no commentary.

## Combining with explicit `--exclude-author`

`--exclude-common-bots`, `--exclude-author`, and the env vars all union
together. There's no precedence — final list is sort-dedup of all sources.

## Verifying the effective list

There's no `retl describe --bots` subcommand yet. If you need to audit the
effective denial list for a run, the simplest cross-check is:

```powershell
retl scan --data-dir .\data --start 2020-01 --end 2020-01 `
          --exclude-common-bots --out before.txt
$env:ETL_EXCLUDE_AUTHORS = "" ; Remove-Item Env:ETL_EXCLUDE_AUTHORS_FILE
# (or just don't set them)
retl scan --data-dir .\data --start 2020-01 --end 2020-01 `
          --exclude-common-bots --out after.txt
diff before.txt after.txt
```

A bot describe/audit subcommand is on the roadmap (see `plan.md` Tier 5
"Decision support"); until then, the source of truth is the table above.
