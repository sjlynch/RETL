use crate::config::ETLOptions;
use crate::mem::AdaptiveMemCfg;

pub(crate) const BYTES_PER_MB: usize = 1024 * 1024;

/// Configuration for the generic dedupe engine.
#[derive(Clone, Debug)]
pub struct DedupeCfg {
    /// Shared adaptive-memory policy (soft_low_frac, high_frac, adapt_cooldown_ms).
    pub mem: AdaptiveMemCfg,
    pub min_buf_mb: usize,
    pub max_buf_mb: usize,
    pub read_buf_bytes: usize,
    pub write_buf_bytes: usize,
    /// Hard cap on bytes inflight between the line-reader producer and the
    /// run-writer consumer. Peak in-memory footprint of `build_runs_sorted`
    /// is bounded by this value (one map being filled + one map awaiting
    /// disk write). 0 disables the cap and falls back to `max_buf_mb` only.
    pub inflight_bytes: usize,
}
impl Default for DedupeCfg {
    fn default() -> Self {
        Self {
            mem: AdaptiveMemCfg::default(),
            min_buf_mb: 512,
            max_buf_mb: 8192,
            read_buf_bytes: 4 * BYTES_PER_MB,
            write_buf_bytes: 4 * BYTES_PER_MB,
            // Default backpressure budget: 256 MiB. With channel capacity of 1,
            // peak inflight = ~2 * (inflight_bytes / 2) = 256 MiB regardless of
            // available_memory_fraction sampling.
            inflight_bytes: 256 * BYTES_PER_MB,
        }
    }
}

impl From<&ETLOptions> for DedupeCfg {
    fn from(opts: &ETLOptions) -> Self {
        Self {
            mem: opts.adaptive_mem.clone(),
            read_buf_bytes: opts.read_buffer_bytes,
            write_buf_bytes: opts.write_buffer_bytes,
            inflight_bytes: opts.inflight_bytes,
            ..Self::default()
        }
    }
}
