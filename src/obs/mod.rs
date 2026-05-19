//! Observability surface for binaries and library callers.
//!
//! `retl` defaults to human-oriented stderr (text logs + indicatif progress
//! bars). For machine watchers — Prometheus scrapers, LLM monitors, scripted
//! supervisors — install a [`MonitorHandle`] via [`install_monitor`]: it
//! optionally emits an NDJSON event stream, periodically rewrites a single
//! status snapshot file, and runs a watchdog thread that enforces hard caps
//! on RSS and runtime plus a stop-file kill switch.
//!
//! Every output is opt-in (`Option<PathBuf>` / `Option<u64>` in
//! [`MonitorOptions`]). With nothing set the handle is a near-no-op and
//! preserves the existing binary behavior. Dropping the handle flushes the
//! event file and writes a final `run.summary` lifecycle event.
//!
//! See `docs/monitoring.md` for the LLM-watcher quickstart and a full schema
//! reference. The on-disk schemas are versioned (`retl.v1`, `retl.status.v1`);
//! field additions are backwards-compatible, breaking changes bump the
//! version.

mod events;
mod handle;
mod options;
mod sink;
mod status;
mod tracing_layer;
mod watchdog;

pub use events::{Event, EventKind, LifecycleEvent};
pub use handle::{install_monitor, MonitorHandle};
pub use options::{LogFormat, MonitorOptions};
pub use status::StatusSnapshot;

#[allow(unused_imports)]
pub(crate) use sink::{EventSink, JsonlSink, NullSink, SharedSink};
#[allow(unused_imports)]
pub(crate) use tracing_layer::EventLayer;
