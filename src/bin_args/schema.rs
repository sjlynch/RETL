use clap::{Args, ValueEnum};
use retl::YearMonth;
use std::path::PathBuf;

use super::SourceArg;

#[derive(Args, Debug, Clone)]
pub(crate) struct SchemaArgs {
    /// Path to corpus base dir (containing `comments/` and `submissions/`).
    #[arg(long, default_value = "./data")]
    pub(crate) data_dir: PathBuf,

    /// Inclusive start month (YYYY-MM).
    #[arg(long, value_name = "YYYY-MM")]
    pub(crate) start: Option<YearMonth>,

    /// Inclusive end month (YYYY-MM).
    #[arg(long, value_name = "YYYY-MM")]
    pub(crate) end: Option<YearMonth>,

    /// Source selection: rc (comments), rs (submissions), or both.
    #[arg(long, value_enum, default_value_t = SourceArg::Both)]
    pub(crate) source: SourceArg,

    /// Records sampled per selected month.
    #[arg(long = "sample", default_value_t = 100)]
    pub(crate) sample_per_month: usize,

    /// Output format.
    #[arg(long, value_enum, default_value_t = SchemaFmt::Tsv)]
    pub(crate) format: SchemaFmt,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum SchemaFmt {
    Tsv,
    Json,
}
