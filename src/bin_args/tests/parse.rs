use super::super::{Cli, Command, CorpusArgs, CorpusCommand, IfExists, LoadMode};
use super::parse_failure;
use clap::Parser;
use retl::YearMonth;

#[test]
fn version_flag_works() {
    // clap returns DisplayVersion as an "error" that carries the version
    // string; that's still a non-spawn parse success in clap's model.
    let err = Cli::try_parse_from(["retl", "--version"]).expect_err("--version uses err path");
    assert!(matches!(err.kind(), clap::error::ErrorKind::DisplayVersion));
}

#[test]
fn unknown_subcommand_fails_clean() {
    let msg = parse_failure(&["not-a-real-subcommand"]);
    assert!(
        msg.to_lowercase().contains("error") || msg.contains("unrecognized"),
        "expected error wording, got: {msg}"
    );
}

#[test]
fn cli_accepts_huge_resource_flags_for_builder_clamp() {
    let huge = usize::MAX.to_string();
    let cli = Cli::try_parse_from([
        "retl",
        "scan",
        "--parallelism",
        huge.as_str(),
        "--file-concurrency",
        huge.as_str(),
    ])
    .expect("resource flag parsing should succeed; builders clamp later");

    match cli.command {
        Command::Scan(args) => {
            assert_eq!(args.common.parallelism, Some(usize::MAX));
            assert_eq!(args.common.file_concurrency, Some(usize::MAX));
        }
        other => panic!("expected scan command, got {other:?}"),
    }
}

#[test]
fn cli_accepts_corpus_plan_command() {
    let cli = Cli::try_parse_from([
        "retl", "corpus", "plan", "--source", "rc", "--start", "2006-01", "--end", "2006-02",
    ])
    .expect("corpus plan should parse");

    match cli.command {
        Command::Corpus(CorpusArgs {
            command: CorpusCommand::Plan(args),
        }) => {
            assert_eq!(args.source.label(), "rc");
            assert_eq!(args.start, YearMonth::new(2006, 1));
            assert_eq!(args.end, YearMonth::new(2006, 2));
        }
        other => panic!("expected corpus plan command, got {other:?}"),
    }
}

#[test]
fn load_command_parses_defaults() {
    let cli = Cli::try_parse_from([
        "retl",
        "load",
        "--from",
        "out/*.parquet",
        "--to",
        "duckdb://reddit.duckdb",
        "--table",
        "comments",
    ])
    .expect("load with defaults should parse");

    match cli.command {
        Command::Load(args) => {
            assert_eq!(args.from, "out/*.parquet");
            assert_eq!(args.to, std::path::PathBuf::from("reddit.duckdb"));
            assert_eq!(args.table, "comments");
            assert_eq!(args.mode, LoadMode::View);
            assert_eq!(args.if_exists, IfExists::Replace);
        }
        other => panic!("expected Load command, got {other:?}"),
    }
}

#[test]
fn load_command_accepts_table_mode_and_fail_policy() {
    let cli = Cli::try_parse_from([
        "retl",
        "load",
        "--from",
        "out/*.parquet",
        "--to",
        "duckdb://reddit.duckdb",
        "--table",
        "rc_2024",
        "--mode",
        "table",
        "--if-exists",
        "fail",
    ])
    .expect("load with explicit mode/if-exists should parse");

    match cli.command {
        Command::Load(args) => {
            assert_eq!(args.mode, LoadMode::Table);
            assert_eq!(args.if_exists, IfExists::Fail);
        }
        other => panic!("expected Load command, got {other:?}"),
    }
}

#[test]
fn load_command_rejects_missing_scheme_on_to() {
    let msg = parse_failure(&[
        "load",
        "--from",
        "out/*.parquet",
        "--to",
        "reddit.duckdb",
        "--table",
        "comments",
    ]);
    assert!(
        msg.contains("duckdb://"),
        "expected duckdb:// hint in error, got: {msg}"
    );
}

#[test]
fn timestamp_aliases_parse_into_query_opts() {
    let cli = Cli::try_parse_from([
        "retl",
        "scan",
        "--start-time",
        "2020-11-30T12:00Z",
        "--end-time",
        "2020-12-01T12:00Z",
    ])
    .expect("timestamp aliases should parse");

    match cli.command {
        Command::Scan(args) => {
            assert_eq!(args.query.after, Some(1_606_737_600));
            assert_eq!(args.query.before, Some(1_606_824_000));
        }
        other => panic!("expected scan command, got {other:?}"),
    }
}
