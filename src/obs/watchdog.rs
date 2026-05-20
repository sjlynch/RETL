//! Hard-cap watchdog thread.
//!
//! Polls process RSS, runtime, and an optional stop-file path. On breach it
//! emits a watchdog event, writes a final `run.summary`, flushes the sink,
//! and exits the process. The hard-exit is safe by design: the library's
//! atomic-write contract guarantees never-corrupt outputs even on abrupt
//! termination, and `_progress.json` resume manifests cover re-entry.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use serde_json::Value;
use sysinfo::{Pid, ProcessesToUpdate, System};

use super::events::Event;
use super::sink::SharedSink;
use super::status::StatusShared;

#[derive(Debug, Clone)]
pub(crate) struct WatchdogConfig {
    pub max_rss_mb: Option<u64>,
    pub max_runtime_sec: Option<u64>,
    pub stop_file: Option<PathBuf>,
    pub poll_interval: Duration,
}

impl Default for WatchdogConfig {
    fn default() -> Self {
        Self {
            max_rss_mb: None,
            max_runtime_sec: None,
            stop_file: None,
            poll_interval: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Breach {
    pub event_name: &'static str,
    pub outcome: &'static str,
    pub exit_code: i32,
    pub level: &'static str,
    pub message: String,
    pub fields: BTreeMap<String, Value>,
}

/// Run a single poll tick. Returns `Some(Breach)` if a cap fired. Public to
/// the module for testing — the spawn loop is just a wrapper.
pub(crate) fn poll_once(
    cfg: &WatchdogConfig,
    sys: &mut System,
    pid: Pid,
    started: Instant,
) -> Option<Breach> {
    if let Some(stop) = &cfg.stop_file {
        if stop.exists() {
            let mut f = BTreeMap::new();
            f.insert("stop_file".to_string(), Value::from(stop.display().to_string()));
            return Some(Breach {
                event_name: "watchdog.stop_file",
                outcome: "stop_file",
                exit_code: 0,
                level: "info",
                message: format!("stop-file detected: {}", stop.display()),
                fields: f,
            });
        }
    }
    if let Some(max_secs) = cfg.max_runtime_sec {
        let elapsed = started.elapsed().as_secs();
        if elapsed >= max_secs {
            let mut f = BTreeMap::new();
            f.insert("elapsed_sec".to_string(), Value::from(elapsed));
            f.insert("max_runtime_sec".to_string(), Value::from(max_secs));
            return Some(Breach {
                event_name: "watchdog.runtime_exceeded",
                outcome: "killed_runtime",
                exit_code: 2,
                level: "error",
                message: format!("runtime cap exceeded: elapsed={elapsed}s, cap={max_secs}s"),
                fields: f,
            });
        }
    }
    if let Some(max_mb) = cfg.max_rss_mb {
        sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
        if let Some(p) = sys.process(pid) {
            let rss_mb = p.memory() / (1024 * 1024);
            if rss_mb >= max_mb {
                let mut f = BTreeMap::new();
                f.insert("rss_mb".to_string(), Value::from(rss_mb));
                f.insert("max_rss_mb".to_string(), Value::from(max_mb));
                return Some(Breach {
                    event_name: "watchdog.rss_exceeded",
                    outcome: "killed_rss",
                    exit_code: 2,
                    level: "error",
                    message: format!("RSS cap exceeded: rss={rss_mb} MiB, cap={max_mb} MiB"),
                    fields: f,
                });
            }
        }
    }
    None
}

/// Spawn the watchdog thread. The `on_breach` callback runs synchronously
/// inside the thread before it exits (whether by `process::exit` or by
/// returning).
pub(crate) fn spawn(
    cfg: WatchdogConfig,
    shared: Arc<StatusShared>,
    sink: SharedSink,
) -> JoinHandle<()> {
    let started = shared.start_instant;
    thread::Builder::new()
        .name("retl-watchdog".to_string())
        .spawn(move || {
            let mut sys = System::new();
            let pid = Pid::from_u32(std::process::id());
            while !shared.shutdown.load(Ordering::Relaxed) {
                if let Some(breach) = poll_once(&cfg, &mut sys, pid, started) {
                    emit_breach(&sink, &breach);
                    emit_final_summary(&sink, &shared, breach.outcome);
                    sink.flush();
                    // Signal shutdown so writer/heartbeat threads stop.
                    shared.shutdown.store(true, Ordering::Relaxed);
                    std::process::exit(breach.exit_code);
                }
                thread::sleep(cfg.poll_interval);
            }
        })
        .expect("spawn watchdog thread")
}

fn emit_breach(sink: &SharedSink, breach: &Breach) {
    let event = Event::watchdog(
        breach.event_name,
        breach.level,
        breach.message.clone(),
        breach.fields.clone(),
    );
    sink.write(&event);
    // Also surface on tracing so a human stderr operator sees it.
    match breach.level {
        "error" => tracing::error!(target: "retl::watchdog", "{}", breach.message),
        _ => tracing::warn!(target: "retl::watchdog", "{}", breach.message),
    }
}

fn emit_final_summary(sink: &SharedSink, shared: &Arc<StatusShared>, outcome: &str) {
    // Route through the shared `summary_emitted` latch: if a cap fires while
    // `MonitorHandle::finalize` is concurrently running, exactly one of the
    // two paths writes `run.summary`. `emit_run_summary_once` also mirrors
    // the marker into the status snapshot's `last_event_*` so an operator
    // polling the status file post-mortem can still identify the outcome.
    super::status::emit_run_summary_once(shared, sink, outcome);
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn poll_once_detects_stop_file() {
        let dir = TempDir::new().unwrap();
        let stop = dir.path().join("STOP");
        std::fs::write(&stop, b"go").unwrap();
        let cfg = WatchdogConfig {
            stop_file: Some(stop),
            ..Default::default()
        };
        let mut sys = System::new();
        let pid = Pid::from_u32(std::process::id());
        let breach = poll_once(&cfg, &mut sys, pid, Instant::now())
            .expect("stop-file should trigger a breach");
        assert_eq!(breach.outcome, "stop_file");
        assert_eq!(breach.exit_code, 0);
    }

    #[test]
    fn poll_once_detects_runtime_cap() {
        let cfg = WatchdogConfig {
            max_runtime_sec: Some(0),
            ..Default::default()
        };
        let mut sys = System::new();
        let pid = Pid::from_u32(std::process::id());
        // Started in the past so elapsed >= 0 immediately.
        let started = Instant::now()
            .checked_sub(Duration::from_secs(5))
            .unwrap_or_else(Instant::now);
        let breach = poll_once(&cfg, &mut sys, pid, started)
            .expect("runtime cap of 0 should trigger");
        assert_eq!(breach.outcome, "killed_runtime");
    }

    #[test]
    fn poll_once_returns_none_when_unconfigured() {
        let cfg = WatchdogConfig::default();
        let mut sys = System::new();
        let pid = Pid::from_u32(std::process::id());
        assert!(poll_once(&cfg, &mut sys, pid, Instant::now()).is_none());
    }
}
