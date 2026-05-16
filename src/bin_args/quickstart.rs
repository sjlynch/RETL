use clap::Args;
use std::path::PathBuf;

#[derive(Args, Debug, Clone)]
pub(crate) struct QuickstartArgs {
    /// Directory where the generated tiny corpus and scratch files are written.
    #[arg(long = "out-dir", default_value = "./retl_quickstart_sample")]
    pub(crate) out_dir: PathBuf,
}
