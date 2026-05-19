use crate::config::ETLOptions;
use crate::dedupe::BYTES_PER_MB;
use crate::mem::AdaptiveMemCfg;

/// Adaptive streaming configuration used during micro-bucket processing.
#[derive(Clone, Debug)]
pub struct BucketingCfg {
    /// Shared adaptive-memory policy (soft_low_frac, high_frac, adapt_cooldown_ms).
    pub mem: AdaptiveMemCfg,
    pub hard_low_frac: f64,      // when below, yield briefly
    pub backoff_ms: u64,         // sleep when under hard threshold
    pub micro_min_buf_mb: usize, // min target buffering when RAM is tight
    pub micro_max_buf_mb: usize, // max target buffering when RAM is plentiful
    /// Hard cap on bytes inflight between the line-reader producer and the
    /// `on_group` consumer. Caps the per-bucket flush target so the bounded
    /// channel — not the RAM-fraction sampler — is the primary backpressure
    /// mechanism. 0 disables the cap.
    pub inflight_bytes: usize,
    /// Bounded channel capacity between producer and on_group consumer.
    /// Sized for ~8 in-flight per-key groups so producers stall when the
    /// consumer falls behind.
    pub inflight_groups: usize,
}

impl Default for BucketingCfg {
    fn default() -> Self {
        Self {
            mem: AdaptiveMemCfg::default(),
            hard_low_frac: 0.10,
            backoff_ms: 25,
            micro_min_buf_mb: 128,
            micro_max_buf_mb: 4096,
            // Default 256 MiB inflight budget; with channel cap 8 the per-flush
            // bucket target is capped at ~32 MiB.
            inflight_bytes: 256 * BYTES_PER_MB,
            inflight_groups: 8,
        }
    }
}

impl From<&ETLOptions> for BucketingCfg {
    fn from(opts: &ETLOptions) -> Self {
        crate::config::warn_if_inflight_pair_pathological(
            opts.inflight_bytes,
            opts.inflight_groups,
        );
        Self {
            mem: opts.adaptive_mem.clone(),
            inflight_bytes: opts.inflight_bytes,
            inflight_groups: opts.inflight_groups,
            ..Self::default()
        }
    }
}
