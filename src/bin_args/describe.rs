use clap::Args;
use retl::YearMonth;
use std::path::PathBuf;

use super::{SchemaFmt, SourceArg};

#[derive(Args, Debug)]
pub(crate) struct DescribeArgs {
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

    /// Decode a small sample from each selected month and report field schema.
    #[arg(long)]
    pub(crate) schema: bool,

    /// Records sampled per selected month (default 100). Only valid with
    /// `--schema`; rejected otherwise because it would have no effect.
    #[arg(long = "schema-sample", value_name = "N")]
    pub(crate) schema_sample: Option<usize>,

    /// Schema output format (default tsv). Only valid with `--schema`;
    /// rejected otherwise because it would have no effect.
    #[arg(long = "schema-format", visible_alias = "format", value_enum)]
    pub(crate) schema_format: Option<SchemaFmt>,

    /// Compare the requested --start/--end/--source against a corpus manifest.
    #[arg(long)]
    pub(crate) expected: bool,

    /// Custom corpus manifest JSON. Implies --expected; omitted means RETL's built-in manifest.
    #[arg(long = "manifest")]
    pub(crate) manifest: Option<PathBuf>,
}
