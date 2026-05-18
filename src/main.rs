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

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Install the monitor before dispatch so any tracing event the handler
    // emits is captured by the EventLayer. Subcommands without CommonOpts
    // get default monitoring (no events file, no caps) — they're typically
    // short-running analytics/manifest commands.
    let monitor_options = match cli.command.common_opts() {
        Some(common) => bin_helpers::build_monitor_options(common),
        None => retl::MonitorOptions::default(),
    };
    let mut monitor = retl::install_monitor(monitor_options)?;

    let result = match cli.command {
        Command::Describe(a) => bin_handlers::run_describe(a),
        Command::Corpus(a) => bin_handlers::run_corpus(a),
        Command::Schema(a) => bin_handlers::run_schema(a),
        Command::Sample(a) => bin_handlers::run_sample(a),
        Command::Quickstart(a) => bin_handlers::run_quickstart(a),
        Command::Scan(a) => bin_handlers::run_scan(a),
        Command::Dedupe(a) => bin_handlers::run_dedupe(a),
        Command::Export(a) => bin_handlers::run_export(a),
        Command::Convert(a) => bin_handlers::run_convert(a),
        Command::Count(a) => bin_handlers::run_count(a),
        Command::Integrity(a) => bin_handlers::run_integrity(a),
        Command::Aggregate(a) => bin_handlers::run_aggregate(a),
        Command::Parents(a) => bin_handlers::run_parents(a),
        Command::FirstSeen(a) => bin_handlers::run_first_seen(a),
    };

    let outcome = match &result {
        Ok(()) => "completed",
        Err(_) => "failed",
    };
    monitor.finalize(outcome);
    result
}
