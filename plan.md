# Plan — LLM-friendly monitoring & observability for `retl`

Goal: make a long-running `retl` scan watchable and steerable by an external
process (an LLM, a human, or a Prometheus-style scraper) without invasive
changes to the library's hot loops. Optimize for: stable structured event
stream, single-file live status snapshot, hard resource caps that cannot be
overshot, and a clean schema documented well enough that an LLM watcher can
operate against it cold.

This is a **binary-side** feature wave (mostly), with one tiny library-side
extension (a `Cancelled` flag the binary can flip when a watchdog fires). The
library's atomic-write contract already guarantees that hard process exit
never corrupts final outputs — so killing the binary on a cap breach is safe.

## Out-of-scope for this wave (deferred)

- Per-stage in-library counters / threading a `MetricsSink` through hot loops
  (Tier 3 in the prior planning doc). Status numbers will come from sampled
  process info + parsing tracing events.
- Library-wide `ErrorClass` enum at every error site (Tier 6).
- Preflight / plan-estimate subcommands (Tier 5).
- Failure injection promotion (Tier 7).
- Profiling capture (Tier 8).

These remain valuable but are larger surface areas; the present wave is the
foundation everything else will plug into.

---

## What we're building

### Feature 1 — Structured NDJSON event stream (`--events <path>`)

An NDJSON file, flushed after every event line, that an external watcher
can `tail -f`. Each line is one JSON object with a stable schema. Source is
a `tracing_subscriber::Layer` installed alongside the existing fmt layer, so
anything the library logs through `tracing` flows out for free.

**Event envelope** (every line):

```json
{
  "ts":        "2026-05-17T14:32:11.482Z",
  "schema":    "retl.v1",
  "level":     "info",
  "kind":      "tracing|lifecycle|status|watchdog",
  "target":    "retl::pipeline_exec::scan",
  "span":      "extract",
  "msg":       "...",
  "fields":    { ... arbitrary key-values from tracing ... }
}
```

`kind` partitions the stream so an LLM can quickly filter:

| kind        | producer                                     | examples |
|-------------|----------------------------------------------|----------|
| `lifecycle` | binary, emitted explicitly                   | `run.start`, `run.knobs`, `run.summary` |
| `tracing`   | tracing layer, forwarding library log output | per-file completion, throttle warns |
| `status`    | status sampler thread (also written to file) | periodic snapshot |
| `watchdog`  | watchdog thread                              | cap breaches, stop-file detection |

Lifecycle events have a stable `event:` field (`run.start`, `run.knobs`,
`run.summary`). Tracing-forwarded events do not — they carry whatever the
library emits and an LLM is expected to read `target` + `msg`.

The file is opened with `File::create` (truncated fresh per run); each
event is serialized, written, and flushed synchronously on the `tracing`
call site under a `Mutex<BufWriter<File>>`. There is no background writer
thread and no channel — the `BufWriter` only coalesces the two `write_all`s
within one event. Each line is a complete JSON object: partial lines never
appear because the writer holds the lock around
`write_all(line) + write_all(b"\n")` and then flushes. A slow events disk
throttles the run rather than dropping events.

### Feature 2 — Live status snapshot (`--status-file <path>`)

A single JSON file atomically rewritten every ~1 s. An LLM watcher reads one
file rather than maintaining state across the event stream.

```json
{
  "schema":         "retl.status.v1",
  "pid":            12345,
  "started_at":     "2026-05-17T14:32:11Z",
  "elapsed_sec":    137.4,
  "current_rss_mb": 412,
  "peak_rss_mb":    489,
  "cpu_percent":    134.2,
  "available_memory_fraction": 0.61,
  "throttle_active": false,
  "events_emitted": 4821,
  "last_event_ts":  "2026-05-17T14:34:28.991Z",
  "last_event":     { "kind": "tracing", "msg": "..." },
  "watchdog": {
    "max_rss_mb":      1024,
    "max_runtime_sec": 3600,
    "stop_file":       "F:/scratch/stop"
  }
}
```

Rewrite path: stage to `<status-file>.retl-<pid>-<nonce>.inprogress` and
atomic-rename via `replace_file_atomic_backoff`. Watchers always see a
fully-valid prior or new snapshot, never a torn read.

### Feature 3 — Watchdog (`--max-rss-mb`, `--max-runtime-sec`, `--stop-file`)

Background thread polling at the same cadence as the status sampler (1 s):

- `--max-rss-mb N` — exceeded → emit `watchdog.rss_exceeded` event, flush
  events file, write final `run.summary` lifecycle event with
  `{ outcome: "killed_rss" }`, then `std::process::exit(2)`.
- `--max-runtime-sec N` — same flow, outcome `"killed_runtime"`.
- `--stop-file <path>` — file appears → outcome `"stop_file"`. Exit code 0
  (operator-requested stop is not an error).

The hard-exit approach is deliberately blunt. It works because:
- Library never writes final paths directly — staged-in-progress files are
  swept on next run.
- `_progress.json` resume manifest already covers re-entry.

A `Cancellation` token is set on breach as well; if the main thread happens
to be between work units it can return cleanly before the watchdog's exit
fires (small race window, but harmless either way).

### Feature 4 — JSON tracing log format (`--log-format json` / `RETL_LOG_FORMAT=json`)

Switch `init_tracing_for_binary` to compose:

- `tracing_subscriber::fmt::layer().json()` when JSON requested, else the
  existing pretty fmt.
- The new event-forwarding `EventLayer` (see Feature 1) when `--events` set.

`RUST_LOG` continues to govern levels. `RETL_LOG_FORMAT` takes precedence
over `--log-format` when set so an operator can always override.

### Feature 5 — Tracing JSON requires `tracing-subscriber` `json` feature

Cargo.toml: `tracing-subscriber = { features = ["env-filter", "json"] }`.

---

## File layout

```
src/
  obs/                  ← new module (library-internal, but pub-used by binary)
    mod.rs              ← re-exports
    events.rs           ← Event type, kind enum, JSON serialization
    sink.rs             ← EventSink trait + JsonlSink + NullSink
    status.rs           ← StatusSnapshot + StatusWriter (background thread)
    watchdog.rs         ← Watchdog thread
    tracing_layer.rs    ← Custom tracing Layer forwarding to EventSink
    handle.rs           ← MonitorHandle (owns the threads + shutdown)
src/bin_args/common.rs  ← extend CommonOpts with monitoring flags
src/bin_helpers/etl.rs  ← build MonitorHandle from CommonOpts
src/main.rs             ← initialize/drop MonitorHandle around handler call
src/util/tracing.rs     ← support JSON format selection
tests/
  monitoring_smoke.rs   ← spawn quickstart with --events, assert content
  obs_units.rs          ← in-process unit tests for events/status/watchdog
docs/
  monitoring.md         ← LLM-watcher quickstart, schema reference
CLAUDE.md               ← cross-link to docs/monitoring.md
```

`src/obs` lives in the library tree (so unit tests inside `src/` can reach
it via `crate::obs::*`), but the public re-exports are kept minimal —
`Event`, `EventKind`, `MonitorHandle`, `MonitorOptions`. End-users wiring
the library directly can opt in by constructing a `MonitorOptions` and
passing it to a small `retl::install_monitor` helper.

---

## Public API surface added to `src/lib.rs`

```rust
pub use crate::obs::{
    install_monitor, MonitorHandle, MonitorOptions, Event, EventKind,
    LifecycleEvent, StatusSnapshot,
};
```

`install_monitor(MonitorOptions) -> MonitorHandle` is the one entry point.
The handle owns all background threads and the tracing-layer attach state.
Dropping the handle flushes the event file and writes a final `run.summary`.

`MonitorOptions` is a small struct with `Option<PathBuf>` fields for each
of `events_file`, `status_file`, `stop_file`, plus `max_rss_mb` and
`max_runtime_sec` as `Option<u64>`, plus `log_format: LogFormat`.

All fields default to `None`. Calling `install_monitor` with everything
`None` returns a no-op handle and costs nothing — preserves the existing
library behavior for callers who don't opt in.

---

## Implementation steps (in order)

1. **`obs/events.rs`** — type definitions, `serde::Serialize`, kind enum,
   ISO-8601 timestamp helper that doesn't pull in `chrono` (`time` crate is
   already a dep).
2. **`obs/sink.rs`** — `EventSink` trait, `JsonlSink` (file-backed,
   mutex-guarded), `NullSink`. Round-trip unit test.
3. **`obs/status.rs`** — `Shared<StatusInner>` updated by both the
   tracing layer (last_event) and the sampler thread (rss, cpu); writer
   thread loops every 1 s, rewrites atomic.
4. **`obs/watchdog.rs`** — thread loop polling sysinfo + `Path::exists` for
   stop-file; calls a `WatchdogCallback` on breach.
5. **`obs/tracing_layer.rs`** — implement `Layer<S>` that on each event
   builds an `Event::tracing { ... }` and pushes to the sink. Capture the
   ordered visitor pattern for fields.
6. **`obs/handle.rs`** — `MonitorHandle` builds everything, owns
   `JoinHandle`s, has a `Drop` that signals shutdown and joins. Final
   `run.summary` lifecycle event written from `Drop`.
7. **`util/tracing.rs`** — extend `init_tracing_for_binary` to accept a
   format selector. JSON format requires `tracing-subscriber/json`.
8. **`Cargo.toml`** — enable `json` feature on `tracing-subscriber`.
9. **`bin_args/common.rs`** — add CLI flags. Defaults preserve current
   behavior.
10. **`bin_helpers/etl.rs`** — `build_monitor_options(&CommonOpts)` helper.
11. **`main.rs`** — initialize monitor before running subcommand handler,
    let `Drop` clean up.
12. **Tests**:
    - `tests/obs_units.rs` — direct in-process verification of
      `JsonlSink`, `StatusWriter` atomic rewrites, `Watchdog` triggering.
    - `tests/monitoring_smoke.rs` — `assert_cmd` invocation of
      `retl quickstart --events x.ndjson --status-file s.json`. Assert
      both files exist, NDJSON parses, status has expected keys.
13. **`docs/monitoring.md`** — full schema reference, LLM-watcher
    walkthrough, common patterns ("how to tell scan is wedged",
    "how to recognize OOM-imminent").
14. **`CLAUDE.md`** — one paragraph in the root pointing at
    `docs/monitoring.md` and at `src/obs/CLAUDE.md`.
15. **`src/obs/CLAUDE.md`** — invariants for future contributors.
16. **E2E smoke**: run an Agent that exercises the watcher loop against
    a real `retl quickstart` invocation. Update prompts/docs based on
    pain points.

---

## Schema stability contract

`schema:` field versioned `retl.v1` (events) and `retl.status.v1`
(status). Additive fields ok; breaking changes bump to `v2` and the
docs page calls out which fields moved/disappeared. An LLM watcher
prompted against `retl.v1` should always be able to read at least
{`ts`, `schema`, `kind`, `msg`} from every line.

---

## Risks / open questions

- **Tracing layer overhead** — `Layer::on_event` runs in hot paths. As shipped, the layer serializes the event and writes+flushes it synchronously under a `Mutex<BufWriter<File>>` (see Feature 1) — there is no bounded channel and no writer thread. The cost is one small `write`+`flush` syscall per event on the emitting thread; a slow events disk throttles the run rather than dropping events. `events_dropped` counts only hard serialization/write failures (disk full, I/O error), never backpressure. (The original design considered a bounded-channel-or-drop layer with a background writer thread; it was not implemented — the synchronous path is simpler and, given how sparse tracing events are on the hot path, fast enough.)
- **Sysinfo on Windows process RSS** — `sysinfo::Process::memory()` returns RSS in bytes on Windows. Confirmed working in this codebase (`src/mem.rs` already uses sysinfo for system memory). For process-specific RSS, sample the current PID's `Process` view.
- **Status file atomic rewrite cost** — once per second, ~1 KB write + rename. Negligible.
- **`tracing-subscriber/json` adds dependencies** — already pulled in transitively by env-filter; small marginal cost.

---

## Done criteria

1. `cargo build --release` clean. ✅
2. `cargo test` clean. ✅ (61 test binaries, all pass; full suite ≈ 5 min)
3. `cargo test --features test-utils` clean. ✅
4. `retl <subcmd> --events e.ndjson --status-file s.json --max-rss-mb N --max-runtime-sec N` produces a non-empty NDJSON, a final status snapshot, expected exit codes. ✅
5. `tail -f e.ndjson` while running shows live events with correct schema. ✅
6. A spawned Agent can read the event stream + status file and describe what the binary is doing without consulting source code. ✅ (one finding fed back into docs)
7. `docs/monitoring.md` exists and is sufficient for an LLM cold-read. ✅

---

## Outcome notes

Watcher Agent feedback round (`tests/monitoring.rs` regression added for each):

- **Bug:** `MonitorHandle::finalize` (and the watchdog's `emit_final_summary`)
  wrote the `run.summary` event directly via `sink.write` and skipped
  `StatusShared::record_event`. The status file's `last_event_*` then
  stuck on the last tracing line, so a watcher polling only the status
  file couldn't detect end-of-run. Fixed in `obs/handle.rs` /
  `obs/watchdog.rs`; new regression test
  `status_file_reflects_run_summary_after_finalize`.
- **Docs gaps surfaced and patched in `docs/monitoring.md`:**
  - "Heartbeats are the canonical liveness pulse" section + 3× threshold
    guidance.
  - "End-of-run semantics — which file is authoritative" — graceful
    finalize updates status; watchdog hard-exit may not.
  - "Detecting hard kill" — cross-check `pid` liveness if events stop
    without `run.summary`.
  - "Schema null fields" — explicitly show null watchdog slots.
  - "See also" link to `src/obs/CLAUDE.md`.
  - CLI vs. library default mismatch for `heartbeat_interval_sec` (CLI
    `5`, `MonitorOptions::default()` `0`).
- **End-to-end verified outcomes (release binary, full 32-month corpus):**
  - `completed` (normal exit, exit 0, run.summary present, status mirrored).
  - `killed_rss` (artificial `--max-rss-mb 1`, exit 2, `watchdog.rss_exceeded`
    event present, tracing mirror present, run.summary outcome present).
  - `stop_file` validated via unit test (`tests/monitoring.rs`) — E2E
    race-prone because the export finished naturally before the next 1 s
    watchdog tick.

## Deferred (still worthwhile)

These remain valuable but are larger surface areas; punted to a future
wave so the present change stays focused and low-risk:

- Per-stage in-library counters threaded through a `MetricsSink` (Tier 3
  in the prior planning doc).
- `enum ErrorClass` annotated at every error site (Tier 6).
- `retl preflight` / `retl plan --estimate` (Tier 5).
- `--inject-failure …` promotion (Tier 7).
- Profile capture, run.summary diff tool (Tier 8).
