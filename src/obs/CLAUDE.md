# `src/obs/` orientation

Opt-in observability surface: structured NDJSON event stream, atomic status
snapshot, watchdog. Used by the binary; library callers can also install
via `retl::install_monitor`. See `docs/monitoring.md` for the cold-read
schema reference.

## Files

- `mod.rs` — re-exports the public surface (`MonitorHandle`,
  `MonitorOptions`, `Event`, `EventKind`, `LifecycleEvent`,
  `StatusSnapshot`, `LogFormat`, `install_monitor`).
- `events.rs` — `Event`, `EventKind`, `LifecycleEvent`, `now_rfc3339`,
  schema string constants. Schema is versioned (`retl.v1` / `retl.status.v1`).
- `sink.rs` — `EventSink` trait, `JsonlSink` (file-backed, mutex-guarded,
  line-flushed), `NullSink`. Counters (`emitted`, `dropped`) are shared
  with the status writer so a watcher can see drops.
- `status.rs` — `StatusSnapshot`, `StatusShared`, writer thread that
  re-samples `sysinfo` and rewrites the file atomically every 1 s. Uses
  `crate::atomic_write::write_at_path_atomic` for torn-read safety.
- `tracing_layer.rs` — `EventLayer` implements `tracing_subscriber::Layer`
  and forwards every event the library logs to the sink as
  `kind: tracing`. No library changes needed for new tracing events to
  show up on the stream.
- `watchdog.rs` — `poll_once` returns `Option<Breach>`; the spawned
  thread calls it once per second. Pure function so it's easy to unit-test.
- `handle.rs` — `MonitorHandle` ties everything together. Owns thread
  joins, performs final-summary write on `Drop`. `install_monitor` is the
  one entry point.

## Invariants

1. **No library hot loops touched.** Forwarding is via `tracing` →
   `EventLayer` → `SharedSink`. Adding a new monitored fact in the library
   is just adding a `tracing::info!(field=value, ...)` somewhere.
2. **Atomic status file.** `_status.json` rewrites use
   `write_at_path_atomic` so readers always see a valid snapshot, never a
   torn one. Don't bypass.
3. **Watchdog is hard-exit.** On a cap breach the watchdog emits its
   event, flushes the sink, writes `run.summary`, and calls
   `std::process::exit`. Cooperative cancellation across the library is
   out of scope; the atomic-write contract on outputs makes hard-exit
   safe.
4. **Schema versions are stable.** `retl.v1` (events) and
   `retl.status.v1` (status). Additive changes ok; renames/removals bump
   to `v2`. The version string is a contract for downstream watchers.
5. **Tracing init is owned by `install_monitor`.** The legacy
   `init_tracing_for_binary` is still exported but the binary no longer
   calls it — `install_monitor` composes `EnvFilter + fmt + EventLayer`
   and calls `try_init` once. Repeat calls return a `tracing::debug!`
   and a no-op `MonitorHandle` (no panic).

## Test guidance

- `tests/monitoring.rs` exercises both in-process (`install_monitor`) and
  binary (`assert_cmd`) paths.
- The watchdog `poll_once` is intentionally pure; unit-test new breach
  classes by feeding fake `Instant`s rather than spawning the thread.

## Adding a new event

For a new tracing-forwarded fact: just add `tracing::info!(name=val, ...)`
where the fact is produced. It will appear on the stream as
`kind: tracing` with `target = <module path>` and `fields` populated.

For a new stable lifecycle marker: add a variant to `LifecycleEvent` with
a kebab/dot-separated name, then call
`MonitorHandle::emit_lifecycle(LifecycleEvent::Foo, msg, fields)` from
the producing site (binary handler, library entry point, etc.). Document
the new event in `docs/monitoring.md`.
