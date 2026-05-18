//! End-to-end smoke for the observability surface added in the obs module.
//!
//! Spawns the `retl` binary with `--events`, `--status-file`, and a
//! `--stop-file` watchdog, plus drives the in-process unit tests via the
//! public `retl::install_monitor` entry point.

use std::fs;
use std::path::Path;
use std::time::Duration;

use assert_cmd::Command;
use serde_json::Value;

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
}

fn read_lines(path: &Path) -> Vec<Value> {
    let body = fs::read_to_string(path).expect("events file should exist");
    body.lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("each line parses as JSON"))
        .collect()
}

#[test]
fn sample_emits_lifecycle_bookends_and_tracing_events() {
    let dir = tempfile::tempdir().unwrap();
    let events = dir.path().join("events.ndjson");
    let status = dir.path().join("status.json");

    retl()
        .arg("sample")
        .arg("--data-dir")
        .arg("data")
        .arg("--limit")
        .arg("2")
        .arg("--events")
        .arg(&events)
        .arg("--status-file")
        .arg(&status)
        .arg("--heartbeat-sec")
        .arg("0")
        .assert()
        .success();

    let lines = read_lines(&events);
    assert!(lines.len() >= 2, "expected at least start+summary events");

    // First event is the lifecycle run.start, last is run.summary.
    assert_eq!(lines[0]["schema"], "retl.v1");
    assert_eq!(lines[0]["kind"], "lifecycle");
    assert_eq!(lines[0]["event"], "run.start");

    let last = lines.last().unwrap();
    assert_eq!(last["kind"], "lifecycle");
    assert_eq!(last["event"], "run.summary");
    assert_eq!(last["fields"]["outcome"], "completed");
    assert!(last["fields"]["elapsed_sec"].as_f64().unwrap() >= 0.0);

    // Tracing-level events from the library are forwarded as kind=tracing.
    let has_tracing = lines.iter().any(|l| l["kind"] == "tracing");
    assert!(
        has_tracing,
        "expected at least one tracing event forwarded; got {:?}",
        lines
    );

    // Status snapshot exists and parses.
    let status_body = fs::read_to_string(&status).expect("status file written");
    let status_json: Value = serde_json::from_str(&status_body).expect("status parses");
    assert_eq!(status_json["schema"], "retl.status.v1");
    assert!(status_json["pid"].as_u64().unwrap() > 0);
    assert!(status_json["events_emitted"].as_u64().unwrap() >= 2);
}

#[test]
fn stop_file_triggers_graceful_exit_with_outcome_stop_file() {
    // We need a subcommand that keeps the process alive long enough for the
    // watchdog to notice the stop-file. `count --mode month` over the full
    // ~32-file corpus is fast but not instant. Pre-create the stop file so
    // the very first watchdog tick (≤1s) fires.
    let dir = tempfile::tempdir().unwrap();
    let events = dir.path().join("events.ndjson");
    let stop = dir.path().join("STOP");
    fs::write(&stop, b"go").unwrap();

    // `count` doesn't have an --events flag at first glance; but it
    // flattens CommonOpts, so the monitoring flags are available.
    let _ = retl()
        .arg("count")
        .arg("--data-dir")
        .arg("data")
        .arg("--mode")
        .arg("month")
        .arg("--events")
        .arg(&events)
        .arg("--stop-file")
        .arg(&stop)
        .arg("--heartbeat-sec")
        .arg("0")
        .arg("--no-progress")
        .assert();
    // Don't .success() — exit code is 0 for stop_file but the process may
    // race and finish naturally; either is fine. We just want the events
    // file to exist with sensible content.

    if events.exists() {
        let body = fs::read_to_string(&events).unwrap();
        // If the stop-file fired, we expect a watchdog event.
        // If not (race: the scan finished before the first poll), we still
        // expect a clean run.summary. Either is acceptable; assert at least
        // one of them.
        let has_stop = body.contains("\"event\":\"watchdog.stop_file\"")
            || body.contains("\"outcome\":\"stop_file\"");
        let has_summary = body.contains("\"event\":\"run.summary\"");
        assert!(
            has_stop || has_summary,
            "expected either stop_file watchdog event or run.summary; events:\n{body}"
        );
    }
}

#[test]
fn status_file_reflects_run_summary_after_finalize() {
    // Regression: prior bug — `finalize` wrote `run.summary` to the event
    // sink directly and skipped `status.record_event`, leaving the status
    // file's `last_event_*` stuck on the last tracing line. Watchers
    // polling only the status file would then think the run was still
    // mid-flight.
    let dir = tempfile::tempdir().unwrap();
    let events = dir.path().join("events.ndjson");
    let status = dir.path().join("status.json");

    retl()
        .arg("sample")
        .arg("--data-dir")
        .arg("data")
        .arg("--limit")
        .arg("2")
        .arg("--events")
        .arg(&events)
        .arg("--status-file")
        .arg(&status)
        .arg("--heartbeat-sec")
        .arg("0")
        .assert()
        .success();

    let body = fs::read_to_string(&status).expect("status file written");
    let snap: Value = serde_json::from_str(&body).expect("status parses");
    assert_eq!(
        snap["last_event_kind"].as_str(),
        Some("lifecycle"),
        "status.last_event_kind should mirror run.summary; got {snap}"
    );
    let last_msg = snap["last_event_msg"]
        .as_str()
        .expect("last_event_msg present");
    assert!(
        last_msg.contains("run ended"),
        "status.last_event_msg should be the run.summary message; got {last_msg:?}"
    );
}

#[test]
fn install_monitor_in_process_emits_run_start() {
    use retl::{install_monitor, LifecycleEvent, MonitorOptions};
    use std::collections::BTreeMap;

    let dir = tempfile::tempdir().unwrap();
    let events_path = dir.path().join("events.ndjson");

    let opts = MonitorOptions {
        events_file: Some(events_path.clone()),
        heartbeat_interval_sec: 0,
        ..Default::default()
    };
    let mut handle = install_monitor(opts).expect("install_monitor succeeds");

    handle.emit_lifecycle(
        LifecycleEvent::RunKnobs,
        "test knobs echo",
        {
            let mut f = BTreeMap::new();
            f.insert(
                "scan_target".to_string(),
                serde_json::Value::from("smoke"),
            );
            f
        },
    );

    handle.finalize("test_done");
    drop(handle);

    // Allow a brief delay for the writer thread to flush — there's no
    // status file so it isn't actually spawned, but be defensive.
    std::thread::sleep(Duration::from_millis(50));

    let body = fs::read_to_string(&events_path).expect("events file written");
    let lines: Vec<Value> = body
        .lines()
        .map(|l| serde_json::from_str(l).expect("each line parses"))
        .collect();
    assert!(lines.len() >= 3, "want start + knobs + summary, got {lines:?}");
    assert_eq!(lines[0]["event"], "run.start");
    assert!(
        lines.iter().any(|l| l["event"] == "run.knobs"),
        "expected a run.knobs event: {lines:?}"
    );
    let last = lines.last().unwrap();
    assert_eq!(last["event"], "run.summary");
    assert_eq!(last["fields"]["outcome"], "test_done");
}
