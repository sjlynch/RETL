static INIT_ONCE: std::sync::Once = std::sync::Once::new();

/// Initialize tracing for the binary. Call this once at program startup from
/// `main`. Library code must NOT call this — the binary owns the tracing
/// subscriber.
pub fn init_tracing_for_binary() {
    INIT_ONCE.call_once(|| {
        // `RETL_LOG` is RETL's own filter env var (documented in the README);
        // honor it ahead of the generic `RUST_LOG` so a project-specific
        // setting wins when both are present.
        let env_filter = std::env::var("RETL_LOG")
            .or_else(|_| std::env::var("RUST_LOG"))
            .unwrap_or_else(|_| "info".to_string());
        let _ = tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .try_init();
    });
}
