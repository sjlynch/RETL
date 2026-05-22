//! Status snapshot file. Atomically rewritten every ~1 s so an LLM watcher
//! can read a single file rather than reconstructing state from the event
//! stream.
//!
//! The snapshot is kept in a shared `Mutex<StatusSnapshot>`. Sampler thread
//! updates `current_rss_mb`, `cpu_percent`, etc.; the tracing layer updates
//! `last_event_*`. Writer thread serializes the snapshot and atomically
//! replaces the on-disk file via the crate's standard staging-and-rename.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde::Serialize;
use sysinfo::{Pid, ProcessesToUpdate, System};

use super::events::{now_rfc3339, Event, LifecycleEvent, STATUS_SCHEMA};
use super::sink::{SharedSink, SinkCounters};

/// Single-file snapshot of the running scan. Layout is versioned via the
/// `schema` field; field additions are backwards-compatible.
#[derive(Debug, Clone, Serialize)]
pub struct StatusSnapshot {
    pub schema: &'static str,
    pub pid: u32,
    pub started_at: String,
    pub last_updated: String,
    pub elapsed_sec: f64,
    pub current_rss_mb: u64,
    pub peak_rss_mb: u64,
    pub cpu_percent: f64,
    pub available_memory_fraction: f64,
    pub throttle_active: bool,
    pub events_emitted: u64,
    pub events_dropped: u64,
    pub last_event_ts: Option<String>,
    pub last_event_kind: Option<String>,
    pub last_event_msg: Option<String>,
    pub watchdog: WatchdogConfig,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct WatchdogConfig {
    pub max_rss_mb: Option<u64>,
    pub max_runtime_sec: Option<u64>,
    pub stop_file: Option<PathBuf>,
}

impl StatusSnapshot {
    fn new(pid: u32, started_at: String, wd: WatchdogConfig) -> Self {
        Self {
            schema: STATUS_SCHEMA,
            pid,
            started_at: started_at.clone(),
            last_updated: started_at,
            elapsed_sec: 0.0,
            current_rss_mb: 0,
            peak_rss_mb: 0,
            cpu_percent: 0.0,
            available_memory_fraction: 1.0,
            throttle_active: false,
            events_emitted: 0,
            events_dropped: 0,
            last_event_ts: None,
            last_event_kind: None,
            last_event_msg: None,
            watchdog: wd,
        }
    }
}

/// Shared state between sampler thread, tracing layer, and writer thread.
pub(crate) struct StatusShared {
    inner: Mutex<StatusSnapshot>,
    pub(crate) start_instant: Instant,
    pub(crate) peak_rss_bytes: AtomicU64,
    pub(crate) sink_counters: Arc<SinkCounters>,
    pub(crate) shutdown: AtomicBool,
    /// Check-and-set latch guarding the single `run.summary` event. Both
    /// `MonitorHandle::finalize` and the watchdog's breach path can reach
    /// end-of-run; whichever flips this from `false` first writes the
    /// summary, the other is a no-op. See [`emit_run_summary_once`].
    pub(crate) summary_emitted: AtomicBool,
}

impl StatusShared {
    pub(crate) fn new(
        wd: WatchdogConfig,
        sink_counters: Arc<SinkCounters>,
    ) -> Self {
        let snapshot = StatusSnapshot::new(std::process::id(), now_rfc3339(), wd);
        Self {
            inner: Mutex::new(snapshot),
            start_instant: Instant::now(),
            peak_rss_bytes: AtomicU64::new(0),
            sink_counters,
            shutdown: AtomicBool::new(false),
            summary_emitted: AtomicBool::new(false),
        }
    }

    pub(crate) fn snapshot(&self) -> StatusSnapshot {
        self.inner.lock().expect("status mutex poisoned").clone()
    }

    pub(crate) fn record_event(&self, kind: &str, msg: &str) {
        let mut s = self.inner.lock().expect("status mutex poisoned");
        s.last_event_ts = Some(now_rfc3339());
        s.last_event_kind = Some(kind.to_string());
        s.last_event_msg = Some(msg.to_string());
    }
}

/// Update the live `StatusSnapshot` with sampled OS metrics. Returns the
/// freshly-sampled RSS in MB so the watchdog can decide on caps without
/// touching `sysinfo` itself.
pub(crate) fn refresh_snapshot(
    shared: &Arc<StatusShared>,
    sys: &mut System,
    pid: Pid,
) -> u64 {
    sys.refresh_memory();
    sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    sys.refresh_cpu_all();

    let elapsed = shared.start_instant.elapsed().as_secs_f64();
    let avail_frac = crate::mem::available_memory_fraction();
    // Match the library's own low-memory predicate. The threshold echoes
    // `AdaptiveMemCfg::default().soft_low_frac` so the status snapshot's
    // `throttle_active` flag flips in step with the in-library throttling.
    let throttle_active =
        crate::mem::is_low_memory(crate::mem::AdaptiveMemCfg::default().soft_low_frac);

    let (rss_bytes, cpu_pct) = match sys.process(pid) {
        Some(p) => (p.memory(), p.cpu_usage() as f64),
        None => (0, 0.0),
    };
    let rss_mb = rss_bytes / (1024 * 1024);
    let peak = shared
        .peak_rss_bytes
        .fetch_max(rss_bytes, Ordering::Relaxed)
        .max(rss_bytes);
    let peak_mb = peak / (1024 * 1024);

    let counters = shared.sink_counters.clone();
    let emitted = counters.emitted();
    let dropped = counters.dropped();

    {
        let mut s = shared.inner.lock().expect("status mutex poisoned");
        s.last_updated = now_rfc3339();
        s.elapsed_sec = elapsed;
        s.current_rss_mb = rss_mb;
        s.peak_rss_mb = peak_mb;
        s.cpu_percent = cpu_pct;
        s.available_memory_fraction = avail_frac;
        s.throttle_active = throttle_active;
        s.events_emitted = emitted;
        s.events_dropped = dropped;
    }
    rss_mb
}

/// Writer thread context. Owns the on-disk path and rewrites the snapshot
/// every `interval` while `shared.shutdown` is false. Returns the
/// `JoinHandle` so callers can `join` on shutdown.
pub(crate) fn spawn_writer(
    shared: Arc<StatusShared>,
    path: PathBuf,
    interval: Duration,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name("retl-status-writer".to_string())
        .spawn(move || {
            let mut sys = System::new();
            let pid = Pid::from_u32(std::process::id());
            while !shared.shutdown.load(Ordering::Relaxed) {
                let _ = refresh_snapshot(&shared, &mut sys, pid);
                if let Err(err) = write_snapshot(&shared, &path) {
                    // Don't kill the writer thread on a single failure; the
                    // next tick will retry. Log via tracing so an operator
                    // sees a path issue without losing the loop.
                    tracing::warn!(
                        path = %path.display(),
                        error = %err,
                        "status snapshot write failed",
                    );
                }
                thread::sleep(interval);
            }
            // Final write on graceful shutdown.
            let _ = refresh_snapshot(&shared, &mut sys, pid);
            let _ = write_snapshot(&shared, &path);
        })
        .expect("spawn status writer thread")
}

fn write_snapshot(shared: &Arc<StatusShared>, path: &Path) -> Result<()> {
    let snap = shared.snapshot();
    let body = serde_json::to_vec_pretty(&snap).context("serializing status snapshot")?;
    crate::atomic_write::write_at_path_atomic(path, 64 * 1024, |w| {
        std::io::Write::write_all(w, &body)?;
        std::io::Write::write_all(w, b"\n")?;
        Ok(())
    })
    .with_context(|| format!("writing status snapshot to {}", path.display()))?;
    Ok(())
}

/// Read the current snapshot for inclusion in a `kind=status` event. Used by
/// the heartbeat ticker in [`MonitorHandle`].
pub(crate) fn snapshot_as_value(shared: &Arc<StatusShared>) -> serde_json::Value {
    let snap = shared.snapshot();
    // Drop schema / pid / started_at from the mirror — the lifecycle
    // RunStart already carried them. Keep the dynamic fields.
    let mut map = serde_json::Map::new();
    map.insert(
        "elapsed_sec".to_string(),
        serde_json::Value::from(snap.elapsed_sec),
    );
    map.insert(
        "current_rss_mb".to_string(),
        serde_json::Value::from(snap.current_rss_mb),
    );
    map.insert(
        "peak_rss_mb".to_string(),
        serde_json::Value::from(snap.peak_rss_mb),
    );
    map.insert(
        "cpu_percent".to_string(),
        serde_json::Value::from(snap.cpu_percent),
    );
    map.insert(
        "available_memory_fraction".to_string(),
        serde_json::Value::from(snap.available_memory_fraction),
    );
    map.insert(
        "throttle_active".to_string(),
        serde_json::Value::from(snap.throttle_active),
    );
    map.insert(
        "events_emitted".to_string(),
        serde_json::Value::from(snap.events_emitted),
    );
    map.insert(
        "events_dropped".to_string(),
        serde_json::Value::from(snap.events_dropped),
    );
    serde_json::Value::Object(map)
}

/// Compute extra context fields suitable for embedding in the final
/// `run.summary` lifecycle event. Pulls peak RSS + final counter totals
/// from the shared status struct.
pub(crate) fn final_summary_fields(
    shared: &Arc<StatusShared>,
    outcome: &str,
) -> HashMap<String, serde_json::Value> {
    let snap = shared.snapshot();
    let mut out = HashMap::new();
    out.insert("outcome".to_string(), serde_json::Value::from(outcome));
    out.insert(
        "elapsed_sec".to_string(),
        serde_json::Value::from(snap.elapsed_sec),
    );
    out.insert(
        "peak_rss_mb".to_string(),
        serde_json::Value::from(snap.peak_rss_mb),
    );
    out.insert(
        "current_rss_mb".to_string(),
        serde_json::Value::from(snap.current_rss_mb),
    );
    out.insert(
        "events_emitted".to_string(),
        serde_json::Value::from(snap.events_emitted),
    );
    out.insert(
        "events_dropped".to_string(),
        serde_json::Value::from(snap.events_dropped),
    );
    out
}

/// Emit the single authoritative `run.summary` lifecycle event, guarded by
/// the [`StatusShared::summary_emitted`] latch.
///
/// Both [`crate::obs::MonitorHandle::finalize`] and the watchdog's breach
/// path can reach end-of-run. If a cap fires while `main` is concurrently
/// inside `finalize`, both would otherwise write a `run.summary` — and
/// `docs/monitoring.md` declares `run.summary` the *only* authoritative
/// end-of-run marker. This check-and-set makes exactly one win: the caller
/// that flips the latch from `false` writes the event and returns `true`;
/// any later caller returns `false` and writes nothing.
pub(crate) fn emit_run_summary_once(
    shared: &Arc<StatusShared>,
    sink: &SharedSink,
    outcome: &str,
) -> bool {
    if shared.summary_emitted.swap(true, Ordering::SeqCst) {
        return false;
    }
    let mut fields = BTreeMap::new();
    for (k, v) in final_summary_fields(shared, outcome) {
        fields.insert(k, v);
    }
    let msg = format!("run ended: outcome={outcome}");
    // Mirror the marker into the status snapshot's `last_event_*` so a
    // watcher polling only the status file still sees the end-of-run state.
    shared.record_event("lifecycle", &msg);
    let event = Event::lifecycle(LifecycleEvent::RunSummary, msg, fields);
    sink.write(&event);
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::sink::JsonlSink;
    use tempfile::TempDir;

    #[test]
    fn run_summary_is_emitted_at_most_once() {
        // A normal `finalize` and a concurrent watchdog breach both route
        // through `emit_run_summary_once`; the latch must let exactly one
        // `run.summary` line reach the sink no matter the interleaving.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("events.ndjson");
        let sink: SharedSink = Arc::new(JsonlSink::create(&path).unwrap());
        let shared = Arc::new(StatusShared::new(
            WatchdogConfig::default(),
            sink.counters(),
        ));

        // First caller (e.g. main's finalize) wins the latch and writes it.
        assert!(emit_run_summary_once(&shared, &sink, "completed"));
        // Second caller (e.g. a watchdog breach racing finalize) is a no-op.
        assert!(!emit_run_summary_once(&shared, &sink, "killed_rss"));
        sink.flush();

        let body = std::fs::read_to_string(&path).unwrap();
        let summaries = body
            .lines()
            .filter(|l| l.contains("\"event\":\"run.summary\""))
            .count();
        assert_eq!(
            summaries, 1,
            "exactly one run.summary must ever be written; got:\n{body}"
        );
        // Only the winning caller's outcome is recorded.
        assert!(body.contains("\"outcome\":\"completed\""));
        assert!(!body.contains("\"outcome\":\"killed_rss\""));
    }
}
