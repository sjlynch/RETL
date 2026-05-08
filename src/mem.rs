use parking_lot::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use sysinfo::{System, SystemExt};

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

/// Returns a recent estimate of available memory fraction (0.0..1.0).
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
    let total = sys.total_memory() as f64;
    let avail = sys.available_memory() as f64;
    let frac = if total > 0.0 {
        (avail / total).clamp(0.0, 1.0)
    } else {
        1.0
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
