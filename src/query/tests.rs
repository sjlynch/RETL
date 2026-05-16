
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_bounds_validate_order_and_stay_minimal() {
        let query = QuerySpec {
            timestamp_bounds: TimestampBounds::new(Some(1_606_737_600), Some(1_606_824_000)),
            ..Default::default()
        };
        query.validate().expect("valid timestamp range");
        assert!(query.has_selective_filters());
        assert!(
            !query.requires_full_parse(),
            "created_utc timestamp bounds are a MinimalRecord fast-path filter"
        );

        let invalid = QuerySpec {
            timestamp_bounds: TimestampBounds::new(Some(10), Some(10)),
            ..Default::default()
        };
        let err = invalid
            .validate()
            .expect_err("empty timestamp range should be rejected");
        assert!(err.to_string().contains("created_utc_gte"));
    }

    #[test]
    fn timestamp_bounds_derive_months_from_inclusive_exclusive_edges() {
        // 2020-11-30T12:00:00Z .. 2020-12-01T00:00:00Z should plan Nov only;
        // the exclusive upper endpoint is exactly at the start of December.
        let bounds = TimestampBounds::new(Some(1_606_737_600), Some(1_606_780_800));
        assert_eq!(bounds.derived_start_month(), Some(YearMonth::new(2020, 11)));
        assert_eq!(bounds.derived_end_month(), Some(YearMonth::new(2020, 11)));

        let spanning = TimestampBounds::new(Some(1_606_737_600), Some(1_606_824_000));
        assert_eq!(
            spanning.derived_start_month(),
            Some(YearMonth::new(2020, 11))
        );
        assert_eq!(spanning.derived_end_month(), Some(YearMonth::new(2020, 12)));
    }
}
