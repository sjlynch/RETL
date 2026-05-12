//! `retl` — command-line interface to the RETL toolkit.
//!
//! Subcommands map onto existing builder methods on `retl::RedditETL` /
//! `retl::ScanPlan`. The original demo binary lives at
//! `examples/quickstart.rs` (`cargo run --example quickstart`).
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
    retl::init_tracing_for_binary();
    let cli = Cli::parse();
    match cli.command {
        Command::Scan(a) => bin_handlers::run_scan(a),
        Command::Dedupe(a) => bin_handlers::run_dedupe(a),
        Command::Export(a) => bin_handlers::run_export(a),
        Command::Count(a) => bin_handlers::run_count(a),
        Command::Integrity(a) => bin_handlers::run_integrity(a),
        Command::Aggregate(a) => bin_handlers::run_aggregate(a),
        Command::Parents(a) => bin_handlers::run_parents(a),
        Command::FirstSeen(a) => bin_handlers::run_first_seen(a),
    }
}
