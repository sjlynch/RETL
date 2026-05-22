
#[cfg(test)]
mod tests {
    use super::*;
    use crate::paths::{plan_files_checked, Discovered};
    use crate::pipeline::RedditETL;
    use std::collections::BTreeMap;

    #[test]
    fn reddit_etl_new_planning_error_mentions_documented_default_data_dir() {
        let etl = RedditETL::new();
        assert_eq!(etl.opts.base_dir, PathBuf::from("./data"));
        assert_eq!(
            etl.opts.comments_dir,
            PathBuf::from("./data").join("comments")
        );
        assert_eq!(
            etl.opts.submissions_dir,
            PathBuf::from("./data").join("submissions")
        );

        let discovered = Discovered {
            comments: BTreeMap::new(),
            submissions: BTreeMap::new(),
        };
        let err = plan_files_checked(
            &discovered,
            &etl.opts.comments_dir,
            &etl.opts.submissions_dir,
            etl.opts.sources,
            etl.opts.start,
            etl.opts.end,
        )
        .expect_err("empty default corpus should produce a planning error");

        let msg = err.to_string();
        assert!(
            msg.contains(&etl.opts.comments_dir.display().to_string()),
            "{msg}"
        );
        assert!(
            msg.contains(&etl.opts.submissions_dir.display().to_string()),
            "{msg}"
        );
        assert!(!msg.contains("../reddit"), "{msg}");
    }

    #[test]
    fn worst_case_peak_matches_documented_formula() {
        // Disabled cap → 0 (no explicit ceiling; AdaptiveMemCfg drives it).
        assert_eq!(inflight_worst_case_peak_bytes(0, 8), 0);
        // Default knobs: (1 + 8) * 128 MiB = 1.125 GiB, not 256 MiB.
        let mib = 1024 * 1024;
        assert_eq!(
            inflight_worst_case_peak_bytes(256 * mib, 8),
            9 * 128 * mib
        );
        // inflight_groups = 1 (what with_inflight_budget sets) → peak == declared budget.
        assert_eq!(
            inflight_worst_case_peak_bytes(256 * mib, 1),
            2 * (256 * mib / 2)
        );
    }

    #[test]
    fn with_inflight_budget_pins_channel_depth_to_one() {
        let opts = ETLOptions::default().with_inflight_budget(64 * 1024 * 1024);
        assert_eq!(opts.inflight_bytes, 64 * 1024 * 1024);
        assert_eq!(opts.inflight_groups, 1);
        // Confirmed against the formula: worst-case peak == declared budget.
        let peak = inflight_worst_case_peak_bytes(opts.inflight_bytes, opts.inflight_groups);
        assert_eq!(peak, opts.inflight_bytes);
    }

    #[test]
    fn resource_knobs_clamp_huge_values() {
        let etl = RedditETL::new()
            .shard_count(usize::MAX)
            .parallelism(usize::MAX)
            .file_concurrency(usize::MAX);

        assert_eq!(etl.opts.shard_count, MAX_SHARDS);
        assert_eq!(etl.opts.parallelism, Some(max_parallelism_limit()));
        assert_eq!(etl.opts.file_concurrency, MAX_FILE_CONCURRENCY);
    }

    #[test]
    fn disable_inflight_cap_sets_zero_sentinel() {
        // The explicit, warning-free opt-out for the disable-the-cap sentinel.
        let opts = ETLOptions::default().disable_inflight_cap();
        assert_eq!(opts.inflight_bytes, 0);
        // 0 means "no explicit ceiling" — the worst-case peak helper reports 0.
        assert_eq!(
            inflight_worst_case_peak_bytes(opts.inflight_bytes, opts.inflight_groups),
            0
        );
    }

    #[test]
    fn with_inflight_budget_zero_keeps_disable_sentinel() {
        // Passing 0 still stores the disable sentinel (and emits a one-shot
        // warn the first time any inflight setter sees 0 this process); the
        // stored value/groups are unchanged.
        let opts = ETLOptions::default().with_inflight_budget(0);
        assert_eq!(opts.inflight_bytes, 0);
        assert_eq!(opts.inflight_groups, 1);
    }

    #[test]
    fn io_buffer_setters_clamp_floor_and_ceiling() {
        // Sub-floor request raised to MIN_IO_BUFFER (previously raised silently).
        let low = ETLOptions::default().with_io_buffers(1, 0);
        assert_eq!(low.read_buffer_bytes, MIN_IO_BUFFER);
        assert_eq!(low.write_buffer_bytes, MIN_IO_BUFFER);

        // Oversized request clamped to MAX_IO_BUFFER instead of reaching the
        // allocator with a usize::MAX-capacity BufReader/BufWriter.
        let high = ETLOptions::default()
            .with_io_read_buffer(usize::MAX)
            .with_io_write_buffer(usize::MAX);
        assert_eq!(high.read_buffer_bytes, MAX_IO_BUFFER);
        assert_eq!(high.write_buffer_bytes, MAX_IO_BUFFER);

        // In-range request passes through untouched.
        let ok = ETLOptions::default().with_io_read_buffer(1024 * 1024);
        assert_eq!(ok.read_buffer_bytes, 1024 * 1024);
    }

    #[test]
    fn resource_knobs_clamp_low_values_to_one() {
        let opts = ETLOptions::default()
            .with_shard_count(0)
            .with_parallelism(0)
            .with_file_concurrency(0);

        assert_eq!(opts.shard_count, 1);
        assert_eq!(opts.parallelism, Some(1));
        assert_eq!(opts.file_concurrency, 1);
    }

    #[test]
    fn with_subreddit_re_trims_after_stripping_r_prefix() {
        // "r/  foo" must not leave leading spaces — a space-padded subreddit
        // passes `is_empty()` validation and then silently matches nothing.
        #[allow(deprecated)]
        let opts = ETLOptions::default().with_subreddit("r/  foo");
        assert_eq!(opts.subreddit.as_deref(), Some("foo"));

        #[allow(deprecated)]
        let padded = ETLOptions::default().with_subreddit("  R/ Foo  ");
        assert_eq!(padded.subreddit.as_deref(), Some("foo"));

        #[allow(deprecated)]
        let blank = ETLOptions::default().with_subreddit("r/   ");
        assert_eq!(blank.subreddit.as_deref(), Some(""));
    }

    #[test]
    fn check_config_surfaces_deferred_invalid_date_range() {
        let ok = ETLOptions::default().with_date_range(
            Some(YearMonth::new(2006, 1)),
            Some(YearMonth::new(2006, 3)),
        );
        assert!(ok.check_config().is_ok());

        let backwards = ETLOptions::default().with_date_range(
            Some(YearMonth::new(2006, 3)),
            Some(YearMonth::new(2006, 1)),
        );
        let err = backwards
            .check_config()
            .expect_err("backwards date range should surface a ConfigBuildError");
        let cfg = err
            .downcast_ref::<ConfigBuildError>()
            .expect("error should be a ConfigBuildError");
        assert_eq!(
            cfg,
            &ConfigBuildError::InvalidDateRange {
                start: YearMonth::new(2006, 3),
                end: YearMonth::new(2006, 1),
            }
        );
    }
}
