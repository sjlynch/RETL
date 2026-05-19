use super::Cli;
use clap::{CommandFactory, Parser};

mod help;
mod parse;

/// Render `<args> --help` for the configured `Cli` into a String. Avoids
/// spawning the binary just to grep for substrings in --help output.
fn render_help(args: &[&str]) -> String {
    let mut cmd = Cli::command();
    // Walk into nested subcommands so e.g. ["scan"] renders scan's help.
    let mut current = &mut cmd;
    for arg in args {
        current = current
            .find_subcommand_mut(arg)
            .unwrap_or_else(|| panic!("subcommand not found: {arg}"));
    }
    current.render_help().to_string()
}

/// Run `Cli::try_parse_from` with leading "retl" and assert that it fails.
/// Returns the rendered error string so callers can assert on its content.
fn parse_failure(argv: &[&str]) -> String {
    let mut v = vec!["retl"];
    v.extend_from_slice(argv);
    let err = Cli::try_parse_from(&v).expect_err("expected clap parse failure");
    err.to_string()
}
