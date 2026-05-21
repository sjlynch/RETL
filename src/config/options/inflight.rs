impl ETLOptions {
    /// Set the inflight-bytes budget that bounds bucketing/dedupe producers.
    /// Lower values trade smaller memory peaks for more frequent flushes.
    /// 0 disables the explicit cap and falls back to memory-fraction sampling.
    ///
    /// Note: this does **not** also bound the bucketing-stage channel. See the
    /// docs on [`ETLOptions::inflight_bytes`] for the worst-case peak formula,
    /// or use [`Self::with_inflight_budget`] to set both knobs together.
    pub fn with_inflight_bytes(mut self, bytes: usize) -> Self {
        self.inflight_bytes = bytes;
        self
    }

    /// Set the bounded-channel depth used by bucketing producer/consumer
    /// pairs. Values below 1 are clamped to 1.
    ///
    /// Note: this is paired with `inflight_bytes`; raising it raises the
    /// bucketing memory peak. See [`ETLOptions::inflight_groups`] for the
    /// worst-case formula and [`Self::with_inflight_budget`] for a helper that
    /// derives both values together.
    pub fn with_inflight_groups(mut self, groups: usize) -> Self {
        self.inflight_groups = groups.max(1);
        self
    }

    /// Convenience setter that derives `inflight_bytes` and `inflight_groups`
    /// from a single budget so the bucketing worst-case peak matches the
    /// declared value.
    ///
    /// Sets `inflight_bytes = bytes` and `inflight_groups = 1`, which pins the
    /// producer→consumer channel to one in-flight group. With that pairing the
    /// worst-case peak is bounded by `inflight_bytes` (per the formula in
    /// [`ETLOptions::inflight_bytes`]). Use this when you want the value you
    /// pass to be the actual RAM ceiling; use the individual setters when you
    /// need throughput tuning (a deeper channel) and have measured headroom.
    ///
    /// **`bytes == 0` does not set a zero-byte ceiling.** `0` is the sentinel
    /// that *disables* the explicit `inflight_bytes` cap (see
    /// [`Self::with_inflight_bytes`]), falling back to memory-fraction
    /// sampling — the opposite of a hard ceiling. If you derive the budget
    /// from a computation that can yield `0`, guard against it before calling
    /// this so an empty input does not silently unbound the run.
    pub fn with_inflight_budget(mut self, bytes: usize) -> Self {
        self.inflight_bytes = bytes;
        self.inflight_groups = 1;
        self
    }

    /// Override the adaptive-memory policy used by bucketing/dedupe
    /// producers. This tunes cooperative throttling thresholds without
    /// changing the hard `inflight_bytes` backpressure cap.
    pub fn with_adaptive_mem(mut self, cfg: AdaptiveMemCfg) -> Self {
        self.adaptive_mem = cfg;
        self
    }
}

/// Worst-case bucketing peak for the configured `(inflight_bytes,
/// inflight_groups)` pair. Mirrors the formula documented on
/// [`ETLOptions::inflight_bytes`]:
///
/// ```text
/// peak ≈ (1 + inflight_groups) * (inflight_bytes / 2)
/// ```
///
/// Returns 0 when `inflight_bytes == 0` (the cap is disabled and the peak is
/// driven by `AdaptiveMemCfg` instead). The dedupe pipeline pins channel
/// capacity to 1 so its peak is bounded by `inflight_bytes`; this helper is
/// the bucketing-side bound.
pub(crate) fn inflight_worst_case_peak_bytes(
    inflight_bytes: usize,
    inflight_groups: usize,
) -> usize {
    if inflight_bytes == 0 {
        return 0;
    }
    let chan_cap = inflight_groups.max(1);
    chan_cap.saturating_add(1).saturating_mul(inflight_bytes / 2)
}

/// One-shot tracing warning when a configured `(inflight_bytes,
/// inflight_groups)` pair would produce a worst-case bucketing peak above
/// roughly 2× the declared `inflight_bytes`.
///
/// Fires **only** from `BucketingCfg::from(&ETLOptions)` — the peak it
/// describes is bucketing-specific. The dedupe pipeline pins its channel
/// capacity to 1 and never reads `inflight_groups`, so this warning must not
/// be raised from the dedupe-config builder (its peak is always bounded by
/// `inflight_bytes`, and the `Once` gate below would otherwise let a dedupe
/// run silence the bucketing path). Gated by a process-wide [`Once`] so the
/// warning is emitted at most once per process. Tests that don't initialize
/// tracing won't see it; binaries that call
/// [`crate::util::init_tracing_for_binary`] will.
pub(crate) fn warn_if_inflight_pair_pathological(inflight_bytes: usize, inflight_groups: usize) {
    use std::sync::Once;
    static WARNED: Once = Once::new();

    if inflight_bytes == 0 {
        return;
    }
    let peak = inflight_worst_case_peak_bytes(inflight_bytes, inflight_groups);
    if peak <= inflight_bytes.saturating_mul(2) {
        return;
    }
    WARNED.call_once(|| {
        let mib = |b: usize| b / (1024 * 1024);
        let ratio = peak as f64 / inflight_bytes as f64;
        let peak_mib = mib(peak);
        let declared_mib = mib(inflight_bytes);
        tracing::warn!(
            inflight_bytes,
            inflight_groups,
            worst_case_peak_bytes = peak,
            ratio,
            "bucketing memory peak ≈ {peak_mib} MiB ({ratio:.1}× declared inflight_bytes={declared_mib} MiB) — worst-case = (1 + inflight_groups) * inflight_bytes/2. \
             Lower --inflight-groups (or call ETLOptions::with_inflight_budget(bytes) to set both together) to bring the peak back under the declared budget.",
        );
    });
}
