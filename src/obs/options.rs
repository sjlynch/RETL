//! [`MonitorOptions`] and the [`LogFormat`] selector — the configuration
//! surface for [`super::install_monitor`]. Kept separate from `handle.rs`
//! so readers extending the option set don't have to scroll past the
//! runtime wiring (and vice versa).

use std::path::{Path, PathBuf};

use serde_json::Value;

/// Log output format selector. Mirrored on the CLI as
/// `--log-format text|json` and on the environment as
/// `RETL_LOG_FORMAT=text|json` (env wins).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Text,
    Json,
}

impl Default for LogFormat {
    fn default() -> Self {
        LogFormat::Text
    }
}

impl LogFormat {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "text" | "pretty" | "fmt" => Some(LogFormat::Text),
            "json" | "ndjson" => Some(LogFormat::Json),
            _ => None,
        }
    }
}

/// Opt-in monitoring configuration. With all defaults, monitoring is a
/// near-no-op and the binary's stderr output is unchanged.
#[derive(Debug, Clone, Default)]
pub struct MonitorOptions {
    /// NDJSON file receiving all structured events. Truncated on open.
    pub events_file: Option<PathBuf>,
    /// JSON file atomically rewritten ~once per second with the live snapshot.
    pub status_file: Option<PathBuf>,
    /// Path the watchdog polls; appearance triggers a graceful stop.
    pub stop_file: Option<PathBuf>,
    /// RSS cap in mebibytes. Cross at or above → exit code 2.
    pub max_rss_mb: Option<u64>,
    /// Runtime cap in seconds. Exceed → exit code 2.
    pub max_runtime_sec: Option<u64>,
    /// Stderr log formatter selection. `RETL_LOG_FORMAT` env wins when set.
    pub log_format: LogFormat,
    /// Heartbeat interval for the status mirror events. Default 5 s; 0 disables.
    pub heartbeat_interval_sec: u64,
}

pub(super) fn build_options_echo(o: &MonitorOptions) -> Value {
    serde_json::json!({
        "events_file": o.events_file.as_ref().map(path_to_string),
        "status_file": o.status_file.as_ref().map(path_to_string),
        "stop_file":   o.stop_file.as_ref().map(path_to_string),
        "max_rss_mb":  o.max_rss_mb,
        "max_runtime_sec": o.max_runtime_sec,
        "log_format":  match o.log_format { LogFormat::Json => "json", LogFormat::Text => "text" },
        "heartbeat_interval_sec": o.heartbeat_interval_sec,
    })
}

fn path_to_string(p: &PathBuf) -> String {
    Path::display(p).to_string()
}
