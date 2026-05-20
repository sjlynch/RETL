//! `retl` — command-line interface to the RETL toolkit.
//!
//! Subcommands map onto existing builder methods on `retl::RedditETL` /
//! `retl::ScanPlan`. `retl quickstart` embeds the same tiny no-corpus demo
//! fixture as `examples/quickstart.rs` for installed-binary smoke tests.
//!
//! The binary is split across three sibling modules to keep this file thin:
//!
//! - [`bin_args`]      — `Cli`, `Command`, and per-subcommand `*Args` structs.
//! - [`bin_helpers`]   — `RecCount`, `build_etl`, `plan!`, etc.
//! - [`bin_handlers`]  — one `run_*` per subcommand variant.

mod bin_args;
mod bin_handlers;
mod bin_helpers;

use anyhow::Result;
use clap::Parser;

use bin_args::{Cli, Command};
use bin_handlers::HandlerOutcome;

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Install the monitor before dispatch so any tracing event the handler
    // emits is captured by the EventLayer. Subcommands without monitoring
    // flags get default monitoring (no events file, no caps) — they're the
    // short-running analytics/manifest commands.
    let monitor_options = match cli.command.monitor_opts() {
        Some(monitor) => bin_helpers::build_monitor_options(monitor),
        None => retl::MonitorOptions::default(),
    };
    let mut monitor = retl::install_monitor(monitor_options)?;

    let result: Result<HandlerOutcome> = match cli.command {
        Command::Describe(a) => bin_handlers::run_describe(a).map(|()| HandlerOutcome::Done),
        Command::Corpus(a) => bin_handlers::run_corpus(a).map(|()| HandlerOutcome::Done),
        Command::Schema(a) => bin_handlers::run_schema(a).map(|()| HandlerOutcome::Done),
        Command::Sample(a) => bin_handlers::run_sample(a).map(|()| HandlerOutcome::Done),
        Command::Quickstart(a) => bin_handlers::run_quickstart(a).map(|()| HandlerOutcome::Done),
        Command::Scan(a) => bin_handlers::run_scan(a).map(|()| HandlerOutcome::Done),
        Command::Dedupe(a) => bin_handlers::run_dedupe(a).map(|()| HandlerOutcome::Done),
        Command::Export(a) => bin_handlers::run_export(a).map(|()| HandlerOutcome::Done),
        Command::Convert(a) => bin_handlers::run_convert(a).map(|()| HandlerOutcome::Done),
        Command::Count(a) => bin_handlers::run_count(a).map(|()| HandlerOutcome::Done),
        Command::Integrity(a) => bin_handlers::run_integrity(a),
        Command::Aggregate(a) => bin_handlers::run_aggregate(a).map(|()| HandlerOutcome::Done),
        Command::Parents(a) => bin_handlers::run_parents(a).map(|()| HandlerOutcome::Done),
        Command::FirstSeen(a) => bin_handlers::run_first_seen(a).map(|()| HandlerOutcome::Done),
    };

    // Map the handler outcome to a monitor `outcome` string, then finalize.
    // `integrity`'s 'corruption found' result must exit 2 — but only *after*
    // `finalize`, so the run still emits a terminal `run.summary` and marks
    // the status file finished. A bare `std::process::exit` from the handler
    // would run no destructors and skip the `MonitorHandle` finalize entirely.
    let outcome = match &result {
        Ok(HandlerOutcome::Done) => "completed",
        Ok(HandlerOutcome::CorruptFilesFound) => "corrupt_files_found",
        Err(_) => "failed",
    };
    monitor.finalize(outcome);

    match result {
        Ok(HandlerOutcome::Done) => Ok(()),
        Ok(HandlerOutcome::CorruptFilesFound) => std::process::exit(2),
        Err(e) => Err(e),
    }
}
