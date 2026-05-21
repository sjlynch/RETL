use crate::config::clamp_parallelism_threads;

/// Run `f` inside a scoped Rayon thread pool sized to `n` threads. If `n` is
/// `None` (or `Some(0)`), run on the global default pool. Positive values are
/// clamped through [`crate::config::max_parallelism_limit`]; if Rayon still
/// rejects the pool, RETL logs a warning and safely falls back to the global
/// pool instead of panicking.
///
/// The fallback does **not** honor the requested thread count: the global pool
/// is sized by whatever the first builder / `RAYON_NUM_THREADS` / CPU-count
/// default established, which is usually all cores. When a caller asked for a
/// specific `n` (e.g. to bound RAM on a tight host) the fallback warning
/// therefore reports the *effective* global-pool thread count alongside the
/// requested one, so an operator can see at a glance that `--parallelism` was
/// not applied and the run may over-subscribe — a real OOM risk on a
/// memory-tight box.
///
/// Prefer this over `rayon::ThreadPoolBuilder::build_global()`, which mutates
/// process-wide state, only succeeds for the first caller, and prevents
/// different stages from picking different thread counts.
pub fn with_thread_pool<R, F>(n: Option<usize>, f: F) -> R
where
    F: FnOnce() -> R + Send,
    R: Send,
{
    match n {
        Some(k) if k > 0 => {
            let clamped = clamp_parallelism_threads(k, "with_thread_pool");
            match rayon::ThreadPoolBuilder::new().num_threads(clamped).build() {
                Ok(pool) => pool.install(f),
                Err(e) => {
                    // The fallback runs on the global pool, which is *not*
                    // sized to `clamped`. Surface its actual thread count so
                    // the operator sees the requested limit was not honored
                    // rather than silently over-subscribing the machine.
                    let effective_threads = rayon::current_num_threads();
                    tracing::warn!(
                        requested = k,
                        clamped,
                        effective_threads,
                        error = %e,
                        "failed to build scoped Rayon thread pool; falling back to the \
                         global pool — the requested parallelism ({clamped}) was NOT \
                         applied and this run will use {effective_threads} threads, \
                         which may over-subscribe a memory-tight host"
                    );
                    f()
                }
            }
        }
        _ => f(),
    }
}
