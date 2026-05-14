use parking_lot::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use sysinfo::System;

/// Cached, low-overhead memory watcher.
/// - Refreshes at most every `REFRESH_EVERY`.
/// - Hot read path is two relaxed atomic loads — no mutex, no syscall.
/// - Refresh path holds a small `parking_lot::Mutex` only to serialize the
///   `sysinfo::System::refresh_memory()` syscall, which still happens at
///   most once per `REFRESH_EVERY`.

const REFRESH_EVERY: Duration = Duration::from_millis(500);
const REFRESH_EVERY_MS: u64 = 500;
/// Fixed-point scale for the cached fraction (parts-per-million).
const PPM: f64 = 1_000_000.0;

/// Env var that overrides the fallback fraction used when sysinfo is
/// unusable. Parsed once, clamped to `0.0..=1.0`.
pub const FALLBACK_FRAC_ENV: &str = "RETL_AVAILABLE_MEMORY_FRACTION";

/// Process-start-ish anchor; all timestamps are millis since this point.
/// Set ~1 second in the past so the very first call always crosses the
/// refresh threshold and pulls a real reading from sysinfo.
static EPOCH: OnceLock<Instant> = OnceLock::new();

/// Cached available-memory fraction in parts-per-million (0..=1_000_000).
/// Default 1_000_000 (== 1.0) means "treat as plenty available" until a
/// real refresh runs.
static FRAC_PPM: AtomicU64 = AtomicU64::new(1_000_000);

/// Millis-since-`EPOCH` at which `FRAC_PPM` was last updated.
static LAST_CHECK_MS: AtomicU64 = AtomicU64::new(0);

/// Mutex serializing the underlying `sysinfo` refresh. The hot read path
/// never touches this.
static REFRESH_LOCK: OnceLock<Mutex<System>> = OnceLock::new();

/// `true` until we observe a degenerate sysinfo reading
/// (`total_memory() == 0`). Stays `false` once tripped so the binary can
/// surface "adaptive throttling source is unreliable in this environment"
/// via [`sysinfo_ok`]. Also gates the one-time warn.
static SYSINFO_OK: AtomicBool = AtomicBool::new(true);

/// Parsed fallback fraction from `RETL_AVAILABLE_MEMORY_FRACTION`.
/// Resolved lazily on first sysinfo failure so unit tests can set the
/// env var before the first call.
static FALLBACK_FRAC: OnceLock<f64> = OnceLock::new();

fn fallback_fraction() -> f64 {
    *FALLBACK_FRAC.get_or_init(|| match std::env::var(FALLBACK_FRAC_ENV) {
        Ok(s) => s
            .trim()
            .parse::<f64>()
            .ok()
            .map(|f| f.clamp(0.0, 1.0))
            // Treat unparseable values as "default" rather than failing
            // the whole run — the env var is a soft knob, not a contract.
            .unwrap_or(1.0),
        Err(_) => 1.0,
    })
}

/// `false` once `sysinfo::System::refresh_memory()` has returned a degenerate
/// reading (`total_memory() == 0`) at least once in this process. When this
/// is `false`, [`available_memory_fraction`] is serving a static fallback
/// rather than tracking real memory pressure — `is_low_memory` and the
/// adaptive throttles in dedupe/bucketing depend on real readings, so a
/// `false` result means those cooperative throttles may never fire.
///
/// Set [`FALLBACK_FRAC_ENV`] (`RETL_AVAILABLE_MEMORY_FRACTION`) to a value in
/// `0.0..=1.0` to inject a non-1.0 fallback so the throttles still fire on
/// environments where sysinfo is unreliable.
pub fn sysinfo_ok() -> bool {
    SYSINFO_OK.load(Ordering::Relaxed)
}

#[inline]
fn epoch() -> Instant {
    *EPOCH.get_or_init(|| {
        Instant::now()
            .checked_sub(REFRESH_EVERY * 2)
            .unwrap_or_else(Instant::now)
    })
}

#[inline]
fn now_ms() -> u64 {
    epoch().elapsed().as_millis() as u64
}

fn refresh_lock() -> &'static Mutex<System> {
    REFRESH_LOCK.get_or_init(|| Mutex::new(System::new()))
}

/// Returns a recent estimate of available memory fraction (`0.0..=1.0`).
///
/// Backed by a cached `sysinfo::System::refresh_memory()` reading. The hot
/// path is two relaxed atomic loads — no mutex, no syscall — so this is
/// cheap enough to call inside tight loops.
///
/// # Fallback when sysinfo is unusable
///
/// On some platforms `sysinfo::total_memory()` returns `0` (sandboxed
/// containers, denied `/proc` access, broken Windows perf counters, certain
/// WSL configs). When that happens we cannot compute a real fraction; the
/// historic behavior was to silently return `1.0` ("pretend we have 100%
/// available"), which silently disabled every cooperative throttle
/// ([`is_low_memory`], [`maybe_throttle_low_memory`], and the
/// `AdaptiveMemCfg::soft_low_frac` checks in dedupe/bucketing).
///
/// The current behavior preserves `1.0` as the *default* fallback but makes
/// the failure observable and tunable:
/// - The first degraded reading emits a one-time `tracing::warn!` carrying
///   the raw `total_memory()` / `available_memory()` so operators can see
///   why memory throttling never kicks in.
/// - [`sysinfo_ok`] returns `false` from then on so the binary can probe
///   the state at startup or shutdown.
/// - The fallback fraction can be overridden via the
///   [`FALLBACK_FRAC_ENV`] env var (`RETL_AVAILABLE_MEMORY_FRACTION`),
///   parsed once and clamped to `0.0..=1.0`. Setting e.g. `0.10` keeps
///   `is_low_memory(0.18)` returning `true` so cooperative throttles still
///   fire on environments where sysinfo cannot be trusted.
///
/// If sysinfo later recovers and returns a non-zero total, real readings
/// resume; `sysinfo_ok()` stays `false` so the prior failure is still
/// visible.
pub fn available_memory_fraction() -> f64 {
    // Hot path: two relaxed loads. No mutex, no syscall.
    let now = now_ms();
    let last = LAST_CHECK_MS.load(Ordering::Relaxed);
    if now.saturating_sub(last) < REFRESH_EVERY_MS {
        return FRAC_PPM.load(Ordering::Relaxed) as f64 / PPM;
    }

    // Slow path: serialize the sysinfo refresh. Re-check under the lock so
    // racing threads don't double-refresh.
    let mut sys = refresh_lock().lock();
    let now = now_ms();
    let last = LAST_CHECK_MS.load(Ordering::Relaxed);
    if now.saturating_sub(last) < REFRESH_EVERY_MS {
        return FRAC_PPM.load(Ordering::Relaxed) as f64 / PPM;
    }
    sys.refresh_memory();
    let total_raw = sys.total_memory();
    let avail_raw = sys.available_memory();
    let frac = if total_raw > 0 {
        ((avail_raw as f64) / (total_raw as f64)).clamp(0.0, 1.0)
    } else {
        // swap returns the previous value; `true` means this is the first
        // observation, so emit the diagnostic exactly once.
        if SYSINFO_OK.swap(false, Ordering::Relaxed) {
            let fb = fallback_fraction();
            tracing::warn!(
                target: "retl::mem",
                total_memory = total_raw,
                available_memory = avail_raw,
                fallback_fraction = fb,
                fallback_env = FALLBACK_FRAC_ENV,
                "sysinfo::refresh_memory() returned total_memory=0; adaptive memory throttling will use a static fallback fraction. Set RETL_AVAILABLE_MEMORY_FRACTION (0.0..=1.0) to keep cooperative throttles active on this environment."
            );
        }
        fallback_fraction()
    };
    FRAC_PPM.store((frac * PPM) as u64, Ordering::Relaxed);
    LAST_CHECK_MS.store(now, Ordering::Relaxed);
    frac
}

/// Returns true if the cached available-memory fraction is below `threshold` (e.g., 0.10 for 10%).
pub fn is_low_memory(threshold: f64) -> bool {
    available_memory_fraction() < threshold
}

/// Cooperative backoff: yields briefly if under the threshold.
/// Safe to call frequently — uses cached memory values internally.
pub fn maybe_throttle_low_memory(threshold: f64) {
    if is_low_memory(threshold) {
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// Shared adaptive-memory tuning knobs embedded in both `DedupeCfg` and
/// `BucketingCfg`. Centralises the three fields so a policy change only
/// touches one place.
#[derive(Clone, Debug)]
pub struct AdaptiveMemCfg {
    /// Available-fraction below which the engine prefers smaller buffers.
    pub soft_low_frac: f64,
    /// Available-fraction above which the engine allows larger buffers.
    pub high_frac: f64,
    /// Minimum milliseconds between adaptive target recomputations.
    pub adapt_cooldown_ms: u64,
}

impl Default for AdaptiveMemCfg {
    fn default() -> Self {
        Self {
            soft_low_frac: 0.18,
            high_frac: 0.85,
            adapt_cooldown_ms: 400,
        }
    }
}

/// Test-only setter that overrides the cached available-memory fraction.
///
/// Stamps `LAST_CHECK_MS` to "now" so the next read uses the injected value
/// rather than tripping the cache window and clobbering it with a real
/// sysinfo refresh. Without that, the cooperative throttles in
/// `dedupe`/`bucketing`/`zstd_jsonl` cannot be exercised deterministically.
///
/// Gated to test/dev builds; production binaries (built without the
/// `test-utils` feature) carry zero overhead.
#[cfg(any(test, feature = "test-utils"))]
pub fn set_available_memory_fraction_for_tests(frac: f64) {
    let frac = frac.clamp(0.0, 1.0);
    FRAC_PPM.store((frac * PPM) as u64, Ordering::Relaxed);
    LAST_CHECK_MS.store(now_ms(), Ordering::Relaxed);
}
