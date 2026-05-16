use clap::{Args, ValueEnum};
use std::path::PathBuf;

#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum ConvertFmt {
    /// RFC4180-style CSV with quoted multiline cells.
    Csv,
    /// Tab-separated text; tabs and line breaks in values are rejected.
    Tsv,
}

#[derive(Args, Debug)]
pub(crate) struct ConvertArgs {
    /// Scratch directory used when streaming converted output to stdout.
    #[arg(long, default_value = "./etl_work")]
    pub(crate) work_dir: PathBuf,
    /// RETL spool/parent-enriched directory containing part_RC_*.jsonl / part_RS_*.jsonl files.
    #[arg(long)]
    pub(crate) spool: Option<PathBuf>,
    /// Output format.
    #[arg(long, value_enum, default_value_t = ConvertFmt::Csv)]
    pub(crate) format: ConvertFmt,
    /// Output destination (use `-` for stdout).
    #[arg(long, short)]
    pub(crate) out: PathBuf,
    /// Column field selector. Repeatable and comma-separated. Plain names select top-level keys;
    /// dotted paths (parent.author) traverse objects; JSON Pointers (/parent/body) handle unusual keys.
    #[arg(
        long = "field",
        alias = "fields",
        value_delimiter = ',',
        value_name = "FIELD"
    )]
    pub(crate) fields: Vec<String>,
    /// Omit the header row.
    #[arg(long)]
    pub(crate) no_header: bool,
    /// JSONL input files. Omit when using --spool, or combine with --spool to append extra files.
    #[arg(num_args = 0.., value_name = "INPUTS")]
    pub(crate) inputs: Vec<PathBuf>,
}
