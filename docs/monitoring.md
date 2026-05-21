# Monitoring `retl` runs (`--events`, `--status-file`, watchdog)

This guide is the cold-read reference for any human or LLM watching a running
`retl` invocation. It documents:

1. The CLI flags that opt the binary into machine-readable monitoring.
2. The on-disk event-stream and status-snapshot schemas.
3. Recommended watcher loops for an LLM-driven monitor.
4. The watchdog kill-switches.
5. How to extend monitoring from library callers.

The default behavior of `retl` is unchanged from prior releases: tracing
logs go to stderr (text), no extra files are written, no caps fire. All
monitoring features below are opt-in.

---

## Quick start — the LLM watcher loop

```powershell
# Window 1: run the scan with monitoring on.
retl scan `
  --data-dir .\data --author somebody `
  --events     F:\scratch\retl-events.ndjson `
  --status-file F:\scratch\retl-status.json `
  --max-rss-mb 4096 `
  --max-runtime-sec 7200 `
  --stop-file F:\scratch\STOP `
  --heartbeat-sec 10
```

```powershell
# Window 2: a watcher (human, script, or LLM tail) reads:
Get-Content F:\scratch\retl-events.ndjson -Wait -Tail 0
Get-Content F:\scratch\retl-status.json            # one JSON object, current state
```

To stop the scan gracefully: `New-Item F:\scratch\STOP -ItemType File`. The
watchdog's next tick (≤ 1 second) emits `watchdog.stop_file` and exits 0.
Hard kill (`Stop-Process`) is also safe — the library's atomic-write
contract guarantees no corrupt outputs even on abrupt termination.

---

## Flags

The monitoring flags below are accepted by every long-running subcommand.
`scan`, `dedupe`, `export`, `count`, `integrity`, `sample`, and `first-seen`
carry them inside the shared `CommonOpts`. `parents` and `aggregate` — among
the longest-running, most memory-hungry operations in the toolkit — flatten
the same flags directly (a `parents` run can balloon the parent cache to
several times the compressed input size, so it especially benefits from an
RSS cap). The remaining short-running analytics/manifest subcommands
(`corpus`, `describe`, `quickstart`, `convert`, `schema`) get default
monitoring (no event file, no caps).

| Flag | Purpose |
|---|---|
| `--events PATH` | Write an NDJSON event stream to this path. Truncated on open. |
| `--status-file PATH` | Atomically rewrite a single JSON snapshot to this path every ~1 s. |
| `--max-rss-mb N` | RSS cap in MiB. ≥ cap → exit code 2 after a `watchdog.rss_exceeded` event. |
| `--max-runtime-sec N` | Wall-clock cap. Exceed → exit code 2 after `watchdog.runtime_exceeded`. |
| `--stop-file PATH` | Watchdog polls this path; presence → exit code 0 after `watchdog.stop_file`. |
| `--log-format text\|json` | Stderr log formatter. `RETL_LOG_FORMAT` env var wins when set. |
| `--heartbeat-sec N` | `kind=status` mirror events on the event stream every N seconds. 0 disables. CLI default `5`; library `MonitorOptions::default()` is `0`. |

---

## Event stream — `--events <path>`

One JSON object per line, NDJSON. The file is opened with `File::create` so
each run starts fresh. Watchers should tail with `Get-Content -Wait -Tail 0`
(PowerShell) or `tail -f` (bash) and parse line-by-line.

Each event is serialized and flushed synchronously on the `tracing` call
site — there is no background writer thread and no bounded channel between
the producer and the file. A slow `--events` disk therefore *throttles the
run* rather than dropping events; point `--events` at fast local storage
for long scans. (See "Liveness" and the `events_dropped` notes below.)

### Envelope

Every line has at least these fields:

```json
{
  "ts":     "2026-05-17T19:08:33.195Z",
  "schema": "retl.v1",
  "kind":   "lifecycle | tracing | status | watchdog",
  "level":  "info | warn | error | debug | trace",
  "msg":    "..."
}
```

Optional fields (omitted when empty):

- `event` — stable identifier for `lifecycle` and `watchdog` events.
  Watchers should pattern-match this, not the free-form `msg`.
- `target` — tracing target (`module::path`). Present on `kind=tracing`.
- `span` — innermost active tracing span name (when known).
- `fields` — arbitrary structured key/values. For `tracing` lines this is
  whatever the library emitted via `tracing::info!(name=value, ...)`.

### Stable `event` names

| `kind`     | `event`                       | Meaning |
|------------|-------------------------------|---------|
| lifecycle  | `run.start`                   | Monitor installed; first line on the stream. Fields: `pid`, `started_at`, `monitor_options`. |
| lifecycle  | `run.knobs`                   | Emitted by binary code that wants to echo resolved config. |
| lifecycle  | `heartbeat`                   | Status mirror (also `kind=status`). |
| lifecycle  | `run.summary`                 | Final line on the stream. Fields: `outcome`, `elapsed_sec`, `peak_rss_mb`, `current_rss_mb`, `events_emitted`, `events_dropped`. |
| watchdog   | `watchdog.rss_exceeded`       | RSS cap fired. Fields: `rss_mb`, `max_rss_mb`. |
| watchdog   | `watchdog.runtime_exceeded`   | Runtime cap fired. Fields: `elapsed_sec`, `max_runtime_sec`. |
| watchdog   | `watchdog.stop_file`          | Stop-file detected. Fields: `stop_file`. |
| status     | (n/a — see `kind`)            | Periodic snapshot mirror. Fields mirror `--status-file` body. |

### Stable `outcome` values on `run.summary`

| `outcome`             | Exit code | Meaning |
|-----------------------|----------:|---------|
| `completed`           | 0         | Subcommand returned `Ok`. |
| `failed`              | 1         | Subcommand returned `Err`. |
| `corrupt_files_found` | 2         | `retl integrity` ran cleanly but detected at least one corrupt corpus file. This is a normal result, **not** a crash — the monitor finalizes (terminal `run.summary`, status file marked finished) *before* the process exits 2. |
| `killed_rss`          | 2         | Watchdog RSS cap fired. |
| `killed_runtime`      | 2         | Watchdog runtime cap fired. |
| `stop_file`           | 0         | Operator-requested graceful stop. |

Note that exit code 2 is shared by `corrupt_files_found`, `killed_rss`, and
`killed_runtime` — a watcher must read `fields.outcome` on `run.summary` (not
the exit code alone) to tell a corruption result from a watchdog kill.

### Tracing events (`kind=tracing`)

The library calls `tracing::info!`, `tracing::warn!`, etc. throughout the
pipeline. The monitor's tracing layer forwards every such event as one line
on the stream — no library changes required. Examples already on the stream
today:

- `target: retl::pipeline_exec`, `msg: Excluding pseudo-users …`
- `target: retl::pipeline_exec`, `msg: running an unfiltered, undated query …`, `fields: {files, compressed_bytes}`
- `target: retl::bucketing`, `msg: bucketing memory peak ≈ X MiB …` (when knobs are pathological)
- `target: retl::watchdog`, `msg: RSS cap exceeded: …`

An LLM watcher should look at:
- `level` to filter signal severity.
- `target` to identify the producing module.
- `msg` + `fields` for the details.

---

## Status snapshot — `--status-file <path>`

A single JSON object atomically rewritten every ~1 second. Watchers read it
with `Get-Content` / `cat` and parse — never tail. The atomic-rename
contract guarantees readers see either a complete prior snapshot or a
complete new one, never a torn read.

### Schema (`retl.status.v1`)

```json
{
  "schema": "retl.status.v1",
  "pid": 23548,
  "started_at": "2026-05-17T19:08:33.195Z",
  "last_updated": "2026-05-17T19:09:34.553Z",
  "elapsed_sec": 61.4,
  "current_rss_mb": 412,
  "peak_rss_mb": 489,
  "cpu_percent": 134.2,
  "available_memory_fraction": 0.61,
  "throttle_active": false,
  "events_emitted": 4821,
  "events_dropped": 0,
  "last_event_ts": "2026-05-17T19:09:34.499Z",
  "last_event_kind": "tracing",
  "last_event_msg": "...",
  "watchdog": {
    "max_rss_mb": 4096,
    "max_runtime_sec": 7200,
    "stop_file": "F:/scratch/STOP"
  }
}
```

Field guide for watchers:

- `current_rss_mb` vs. `peak_rss_mb` — climbing peak means the scan is
  acquiring more RAM than ever before. A flat peak with rising
  `current_rss_mb` suggests a transient.
- `cpu_percent` — across all cores. A 16-core box doing real work reads
  around `n_workers * 100`. Values near 0 with a wedged `last_event_ts`
  suggest the scan is stuck (deadlock or stalled in a syscall).
- `available_memory_fraction` — system-wide, sampled from `sysinfo`. When
  this drops below ~0.15 the in-library adaptive throttling kicks in
  and `throttle_active` flips true.
- `throttle_active` — when true, the library is intentionally slowing
  bucketing/dedupe producers. A watcher seeing this set for extended
  periods may want to lower `--inflight-bytes`.
- `events_dropped` — counts events lost to a **serialization or write
  error** (disk full, I/O failure), not to backpressure. The events file
  is written synchronously with no bounded channel, so a slow sink
  throttles the run instead of dropping events — `events_dropped` cannot
  rise from a watcher falling behind. Should normally be 0; a non-zero
  value means a hard write/serialize failure on the `--events` path.
- `last_event_kind` + `last_event_msg` — what's happening right now.

---

## Detecting common failure modes

| Signal in status | Likely cause | Recommended response |
|---|---|---|
| `current_rss_mb` climbing past `--inflight-bytes` × 4 | Pathological `inflight_groups` × `inflight_bytes` pair | Lower `--inflight-groups`, or use `RedditETL::inflight_budget()` to set both together |
| `throttle_active=true` for > 60 s | System under memory pressure | Reduce `--inflight-bytes` or close other apps |
| `last_event_ts` unchanging for > 30 s, `cpu_percent` near 0 | Wedged in zstd decode or stuck on a slow I/O path | Inspect the process; consider hard-kill + resume |
| `events_dropped > 0` | Serialization or write error on the events file (disk full, I/O failure) — *not* a slow watcher | Check disk space and permissions on the `--events` path |
| `throttle_active=false` but the run is slow and `--events` is on a slow disk | Synchronous event writes throttling the run | Move `--events` to fast local storage, or lower `--heartbeat-sec` |
| `outcome: killed_rss` in `run.summary` | RSS cap fired | Re-run with a higher cap or smaller `--inflight-bytes` |
| Stream stops mid-run with no `run.summary` | Hard kill (SIGKILL / Stop-Process / OOM-killer) | See "Detecting hard kill" below; re-run with `--resume` if applicable |

## Liveness — heartbeats are the canonical pulse

The `kind=status` heartbeat events on the event stream are the authoritative
liveness signal. Set `--heartbeat-sec N` to a non-zero value (default `5`
in the CLI, `0` in `MonitorOptions::default()` for library callers — set
it explicitly when you want heartbeats) and have your watcher expect a
new heartbeat at least every `3 * N` seconds. Longer than that without a
heartbeat *and* no `run.summary* means the binary is wedged or was
hard-killed.

Why heartbeats and not the status file timestamp? The status file is
written by a sampler thread; on a wedge the sampler may itself be stuck
or starved. The event-stream heartbeat goes through the same path as
every other event (`SharedSink` write), so its absence is informative.

## End-of-run semantics — which file is authoritative?

- **`run.summary` on the events stream is the only authoritative
  end-of-run marker.** Watchers should treat the appearance of a line
  with `kind: "lifecycle"` and `event: "run.summary"` as the
  end-of-run signal, and read its `fields.outcome` for the result.
- **Graceful exit (handler returned, `MonitorHandle::finalize` ran):**
  the status file is rewritten one final time so its `last_event_kind`
  is `"lifecycle"` and `last_event_msg` is `"run ended: outcome=…"`.
  This is a reliable secondary signal. This includes `retl integrity`
  exiting 2 on a 'corruption found' result (outcome `corrupt_files_found`):
  the monitor finalizes normally before the exit, so a watcher sees a
  proper `run.summary` and must not mistake it for a hard kill.
- **Watchdog hard-exit (cap fired):** `run.summary` *is* emitted on the
  event stream before `process::exit`, but the status writer thread may
  not get a final tick before termination — so the on-disk status file
  may show the last sampled state rather than the end state. Use the
  event stream as the source of truth.
- **External hard-kill (SIGKILL, Stop-Process, OOM-killer):** no
  `run.summary` is emitted. See "Detecting hard kill" below.

## Detecting hard kill (no `run.summary`)

If your watcher sees the event stream stop and no `run.summary` arrives,
the process was killed externally. Cross-check liveness:

- The status file's `pid` field is stable across the run. After the
  events stream stalls, check whether `pid` is still alive
  (`Get-Process -Id $pid` on PowerShell, `kill -0 $pid` on POSIX).
- If `pid` is gone and no `run.summary` exists, the run died abruptly.
  Final outputs are still safe (atomic-write contract); re-run with
  `--resume` if the operation supports it.

## Schema "null" fields

When a watchdog cap is unset, its slot in the status snapshot is
serialized as `null`:

```json
"watchdog": {
  "max_rss_mb":      null,
  "max_runtime_sec": null,
  "stop_file":       null
}
```

This is not a schema break — every `Option<T>` field follows the same
convention. An LLM watcher should treat absent caps as "no limit" rather
than as a missing or malformed field.

---

## Watchdog semantics

The watchdog runs on its own thread, polling once per second. Hard-cap
behavior is intentionally blunt: the watchdog emits its event, flushes the
sink, writes a final `run.summary`, and calls `std::process::exit`. The
library's atomic-write contract makes this safe — no final output path can
be left in a torn state.

If you need cooperative cancellation (drain current work unit, then exit),
use `--stop-file` and don't set `--max-rss-mb` / `--max-runtime-sec`. The
stop-file outcome is exit 0 specifically so it doesn't trip CI failure
detection.

---

## Library API

Library callers (not just the binary) can install monitoring directly:

```rust
use retl::{install_monitor, LogFormat, MonitorOptions};

let mut monitor = install_monitor(MonitorOptions {
    events_file: Some("events.ndjson".into()),
    status_file: Some("status.json".into()),
    max_rss_mb: Some(4096),
    max_runtime_sec: Some(7200),
    stop_file: Some("STOP".into()),
    log_format: LogFormat::Text,
    heartbeat_interval_sec: 5,
})?;

// ... drive RedditETL::scan(...) etc. ...

// Emit a lifecycle event from your own code:
use retl::LifecycleEvent;
use std::collections::BTreeMap;
let mut fields = BTreeMap::new();
fields.insert("target_user".into(), serde_json::Value::from("alice"));
monitor.emit_lifecycle(LifecycleEvent::RunKnobs, "starting per-user scan", fields);

// On Ok / Err:
monitor.finalize("completed");
```

Dropping a `MonitorHandle` without an explicit `finalize` emits
`run.summary { outcome: "completed" }`. The tracing subscriber is set once
per process — repeated `install_monitor` calls reuse the existing
subscriber but return a fresh handle.

---

## See also

- `src/obs/CLAUDE.md` — implementation invariants for contributors
  (atomic-write contract, schema-version policy, tracing-init ownership,
  how to add a new event).

## Schema stability

`schema: "retl.v1"` (events) and `schema: "retl.status.v1"` (status) are
both versioned. Field additions are backwards-compatible; renames or
removals bump the version (`retl.v2`). An LLM watcher prompted against
`retl.v1` should always be able to read at least `{ts, schema, kind, msg}`
from every event line.

When a watcher needs to detect schema upgrades, key off the `schema:`
field on the first line (`run.start`).
