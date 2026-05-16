use clap::{Args, Subcommand, ValueEnum};
use retl::YearMonth;
use std::path::PathBuf;

use super::SourceArg;

#[derive(Args, Debug)]
pub(crate) struct CorpusArgs {
    #[command(subcommand)]
    pub(crate) command: CorpusCommand,
}

#[derive(Subcommand, Debug)]
pub(crate) enum CorpusCommand {
    /// Emit a desired-vs-local download checklist for RC/RS monthly dumps.
    Plan(CorpusPlanArgs),
    /// Print RETL's built-in corpus manifest JSON.
    Manifest(CorpusManifestArgs),
}

#[derive(Args, Debug)]
pub(crate) struct CorpusPlanArgs {
    /// Destination corpus base dir that will contain comments/ and submissions/.
    #[arg(long, default_value = "./data")]
    pub(crate) dest: PathBuf,

    /// Inclusive start month (YYYY-MM).
    #[arg(long, value_name = "YYYY-MM")]
    pub(crate) start: YearMonth,

    /// Inclusive end month (YYYY-MM).
    #[arg(long, value_name = "YYYY-MM")]
    pub(crate) end: YearMonth,

    /// Source selection: rc (comments), rs (submissions), or both.
    #[arg(long, value_enum, default_value_t = SourceArg::Both)]
    pub(crate) source: SourceArg,

    /// Custom corpus manifest JSON. Omit to use RETL's built-in manifest.
    #[arg(long)]
    pub(crate) manifest: Option<PathBuf>,

    /// Output format.
    #[arg(long, value_enum, default_value_t = CorpusPlanFmt::Json)]
    pub(crate) format: CorpusPlanFmt,

    /// Output destination (default stdout). Use '-' for stdout.
    #[arg(long, short, default_value = "-")]
    pub(crate) out: PathBuf,

    /// Only emit available source/month rows whose expected file is missing locally.
    #[arg(long)]
    pub(crate) only_missing: bool,

    /// Compute SHA-256 for present files that have manifest checksums. This may read multi-GB files.
    #[arg(long)]
    pub(crate) verify_checksums: bool,
}

#[derive(Args, Debug)]
pub(crate) struct CorpusManifestArgs {
    /// Output destination (default stdout). Use '-' for stdout.
    #[arg(long, short, default_value = "-")]
    pub(crate) out: PathBuf,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum CorpusPlanFmt {
    Json,
    Tsv,
}
