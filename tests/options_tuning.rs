use retl::{AdaptiveMemCfg, BucketingCfg, DedupeCfg, ETLOptions};

#[test]
fn etl_options_forward_adaptive_mem_and_inflight_groups_to_runtime_cfgs() {
    let adaptive = AdaptiveMemCfg {
        soft_low_frac: 0.99,
        high_frac: 0.999,
        adapt_cooldown_ms: 50,
    };
    let opts = ETLOptions::default()
        .with_io_buffers(32 * 1024, 64 * 1024)
        .with_inflight_bytes(123 * 1024 * 1024)
        .with_inflight_groups(3)
        .with_adaptive_mem(adaptive.clone());

    let dedupe = DedupeCfg::from(&opts);
    assert_eq!(dedupe.mem.soft_low_frac, adaptive.soft_low_frac);
    assert_eq!(dedupe.mem.high_frac, adaptive.high_frac);
    assert_eq!(dedupe.mem.adapt_cooldown_ms, adaptive.adapt_cooldown_ms);
    assert_eq!(dedupe.read_buf_bytes, 32 * 1024);
    assert_eq!(dedupe.write_buf_bytes, 64 * 1024);
    assert_eq!(dedupe.inflight_bytes, 123 * 1024 * 1024);

    let bucketing = BucketingCfg::from(&opts);
    assert_eq!(bucketing.mem.soft_low_frac, adaptive.soft_low_frac);
    assert_eq!(bucketing.mem.high_frac, adaptive.high_frac);
    assert_eq!(bucketing.mem.adapt_cooldown_ms, adaptive.adapt_cooldown_ms);
    assert_eq!(bucketing.inflight_bytes, 123 * 1024 * 1024);
    assert_eq!(bucketing.inflight_groups, 3);
}

#[test]
fn inflight_groups_builder_clamps_zero_to_one() {
    let opts = ETLOptions::default().with_inflight_groups(0);
    assert_eq!(opts.inflight_groups, 1);
    assert_eq!(BucketingCfg::from(&opts).inflight_groups, 1);
}
