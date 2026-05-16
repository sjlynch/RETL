use clap::{Args, ValueEnum};
use std::path::PathBuf;

use super::{parsers::parse_positive_sample_bytes, CommonOpts};

#[derive(Args, Debug)]
pub(crate) struct IntegrityArgs {
    #[command(flatten)]
    pub(crate) common: CommonOpts,
    /// Validation mode.
    #[arg(long, value_enum, default_value_t = IntegrityModeArg::Quick)]
    pub(crate) mode: IntegrityModeArg,
    /// Bytes (decompressed) to sample per file in quick mode. Must be positive;
    /// values below 4096 only validate a tiny prefix and emit a warning.
    #[arg(long, default_value_t = 64 * 1024, value_parser = parse_positive_sample_bytes)]
    pub(crate) sample_bytes: u64,
    /// Collect failures and print them only after all files finish.
    ///
    /// By default, integrity streams one `path<TAB>error` line to stdout as soon
    /// as each failure is discovered.
    #[arg(long)]
    pub(crate) collect: bool,

    /// Before zstd validation, compare the requested --start/--end/--source against a corpus manifest.
    #[arg(long)]
    pub(crate) expected: bool,

    /// Custom corpus manifest JSON. Implies --expected; omitted means RETL's built-in manifest.
    #[arg(long = "manifest")]
    pub(crate) manifest: Option<PathBuf>,

    /// With --expected/--manifest, compute SHA-256 for present files that have manifest checksums.
    /// This may read multi-GB files in addition to the zstd integrity pass.
    #[arg(long)]
    pub(crate) verify_checksums: bool,

    /// Hidden compatibility trap: integrity is intentionally not resumable.
    #[arg(long, hide = true)]
    pub(crate) resume: bool,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum IntegrityModeArg {
    Quick,
    Full,
}
