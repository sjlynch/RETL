//! [`MonitorHandle`] — the public entry point. Owns the event sink, the
//! status snapshot writer, the watchdog, and the tracing subscriber init.
//!
//! Construction order matters: the tracing subscriber must be composed with
//! our [`EventLayer`] before background threads start logging. [`install_monitor`]
//! handles that for callers.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::Value;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use super::events::{Event, LifecycleEvent};
use super::options::{build_options_echo, LogFormat, MonitorOptions};
use super::sink::{JsonlSink, NullSink, SharedSink};
use super::status::{spawn_writer, StatusShared, WatchdogConfig as StatusWatchdog};
use super::tracing_layer::EventLayer;
use super::watchdog;

/// Active monitor. Dropping it signals shutdown, joins background threads,
/// and writes a final `run.summary` lifecycle event.
pub struct MonitorHandle {
    sink: SharedSink,
    status: Arc<StatusShared>,
    threads: Vec<JoinHandle<()>>,
    options_echo: serde_json::Value,
    finished: bool,
}

impl MonitorHandle {
    /// Stable JSON describing the resolved monitor configuration. Useful in
    /// `run.knobs` lifecycle events.
    pub fn options_echo(&self) -> &serde_json::Value {
        &self.options_echo
    }

    /// Emit a stable lifecycle event named via [`LifecycleEvent`]. Binary
    /// handlers use this to publish `run.knobs` and subcommand metadata so
    /// an LLM can verify intent without scraping logs.
    pub fn emit_lifecycle(
        &self,
        ev: LifecycleEvent,
        msg: impl Into<String>,
        fields: BTreeMap<String, Value>,
    ) {
        let event = Event::lifecycle(ev, msg, fields);
        self.status.record_event("lifecycle", &event.msg);
        self.sink.write(&event);
    }

    /// Finalize the monitor with a chosen outcome. Idempotent: subsequent
    /// calls (including the `Drop` impl) are no-ops.
    pub fn finalize(&mut self, outcome: &str) {
        if self.finished {
            return;
        }
        self.finished = true;

        // Refresh the snapshot once synchronously so the `run.summary`
        // carries an accurate peak/elapsed even for very fast runs whose
        // writer thread never got a tick.
        let mut sys = sysinfo::System::new();
        let pid = sysinfo::Pid::from_u32(std::process::id());
        let _ = super::status::refresh_snapshot(&self.status, &mut sys, pid);

        // Emit the terminal `run.summary` — but only if the watchdog thread
        // hasn't already done so on a concurrent cap breach. The shared
        // `summary_emitted` latch guarantees exactly one `run.summary` per
        // run. `emit_run_summary_once` also `record_event`s the marker so
        // the writer thread's final snapshot pickup of `last_event_*`
        // reflects the summary rather than the last tracing line.
        super::status::emit_run_summary_once(&self.status, &self.sink, outcome);

        // Stop background threads. Joining the writer thread guarantees
        // its final atomic write reflects the lifecycle.RunSummary
        // record_event above.
        self.status.shutdown.store(true, Ordering::Relaxed);
        for handle in self.threads.drain(..) {
            let _ = handle.join();
        }
        self.sink.flush();
    }
}

impl Drop for MonitorHandle {
    fn drop(&mut self) {
        if !self.finished {
            self.finalize("completed");
        }
    }
}

/// Install the global tracing subscriber and start monitoring threads.
///
/// Returns a [`MonitorHandle`] that the binary keeps alive for the duration
/// of the run. Idempotent for repeated calls with the same options — the
/// second call returns a fresh handle but the global subscriber is set only
/// once.
pub fn install_monitor(options: MonitorOptions) -> Result<MonitorHandle> {
    let sink: SharedSink = if let Some(path) = options.events_file.as_ref() {
        Arc::new(JsonlSink::create(path).context("creating events file")?)
    } else {
        Arc::new(NullSink::new())
    };

    let watchdog_cfg = StatusWatchdog {
        max_rss_mb: options.max_rss_mb,
        max_runtime_sec: options.max_runtime_sec,
        stop_file: options.stop_file.clone(),
    };
    let status = Arc::new(StatusShared::new(watchdog_cfg, sink.counters()));

    init_tracing(options.log_format, sink.clone(), status.clone())?;

    let options_echo = build_options_echo(&options);

    let mut threads: Vec<JoinHandle<()>> = Vec::new();
    if let Some(path) = options.status_file.clone() {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!("creating status-file parent dir {}", parent.display())
                })?;
            }
        }
        threads.push(spawn_writer(
            status.clone(),
            path,
            Duration::from_secs(1),
        ));
    }

    if options.max_rss_mb.is_some()
        || options.max_runtime_sec.is_some()
        || options.stop_file.is_some()
    {
        let wd_cfg = super::watchdog::WatchdogConfig {
            max_rss_mb: options.max_rss_mb,
            max_runtime_sec: options.max_runtime_sec,
            stop_file: options.stop_file.clone(),
            poll_interval: Duration::from_secs(1),
        };
        threads.push(watchdog::spawn(wd_cfg, status.clone(), sink.clone()));
    }

    if options.heartbeat_interval_sec > 0 {
        threads.push(spawn_heartbeat(
            status.clone(),
            sink.clone(),
            Duration::from_secs(options.heartbeat_interval_sec),
        ));
    }

    let handle = MonitorHandle {
        sink: sink.clone(),
        status: status.clone(),
        threads,
        options_echo: options_echo.clone(),
        finished: false,
    };

    // Lifecycle: run.start emitted up front so even a near-instant
    // failure leaves a trace.
    let mut fields = BTreeMap::new();
    fields.insert("pid".to_string(), Value::from(std::process::id()));
    fields.insert(
        "started_at".to_string(),
        Value::from(super::events::now_rfc3339()),
    );
    fields.insert("monitor_options".to_string(), options_echo);
    handle.emit_lifecycle(LifecycleEvent::RunStart, "retl monitor installed", fields);

    Ok(handle)
}

static TRACING_INIT: OnceLock<()> = OnceLock::new();

fn init_tracing(
    format: LogFormat,
    sink: SharedSink,
    status: Arc<StatusShared>,
) -> Result<()> {
    // Only one global subscriber per process; if a prior call won, the
    // second EventLayer wouldn't observe anything anyway.
    if TRACING_INIT.set(()).is_err() {
        return Ok(());
    }

    let env_layer = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let event_layer = EventLayer::new(sink, status);

    // Resolve format: env var wins.
    let effective_format = std::env::var("RETL_LOG_FORMAT")
        .ok()
        .and_then(|v| LogFormat::parse(&v))
        .unwrap_or(format);

    // Preserve the historical default of writing fmt output to stdout so
    // existing CLI tests (and operators piping stdout) continue to see
    // tracing lines where they always did. The new `--events` NDJSON file
    // is the structured destination; the stderr stream is unchanged.
    let registry = tracing_subscriber::registry().with(env_layer);
    let result = match effective_format {
        LogFormat::Json => registry
            .with(tracing_subscriber::fmt::layer().json())
            .with(event_layer)
            .try_init(),
        LogFormat::Text => registry
            .with(tracing_subscriber::fmt::layer())
            .with(event_layer)
            .try_init(),
    };
    if let Err(err) = result {
        // Another subscriber was already installed (e.g. a test harness).
        // The monitor still works for direct emit_lifecycle / watchdog
        // calls; just won't see tracing events.
        tracing::debug!(error = %err, "tracing subscriber already installed; EventLayer skipped");
    }
    Ok(())
}

fn spawn_heartbeat(
    status: Arc<StatusShared>,
    sink: SharedSink,
    interval: Duration,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("retl-monitor-heartbeat".to_string())
        .spawn(move || {
            while !status.shutdown.load(Ordering::Relaxed) {
                std::thread::sleep(interval);
                if status.shutdown.load(Ordering::Relaxed) {
                    break;
                }
                let snap = super::status::snapshot_as_value(&status);
                let event = Event::status_mirror(snap);
                sink.write(&event);
            }
        })
        .expect("spawn heartbeat thread")
}
