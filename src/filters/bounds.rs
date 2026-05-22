use crate::date::YearMonth;
use crate::zstd_jsonl::MinimalRecord;
use time::{Date, OffsetDateTime};

/// Independent record-level YYYY-MM bounds.
///
/// A missing endpoint leaves that side open, but the other endpoint still
/// applies. When either endpoint is present, records without `created_utc` are
/// rejected because they cannot be proven to fall inside the requested range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DateBounds {
    pub start: Option<YearMonth>,
    pub end: Option<YearMonth>,
}

impl DateBounds {
    #[inline]
    pub fn is_active(self) -> bool {
        self.start.is_some() || self.end.is_some()
    }
}

/// Bounds helper (record-level YYYY-MM gate).
///
/// Kept under its historical name for compatibility with existing internal
/// callers/tests; unlike the old tuple shape, one-sided ranges now return
/// `Some(DateBounds)` and enforce the present endpoint.
pub fn bounds_tuple(start: Option<YearMonth>, end: Option<YearMonth>) -> Option<DateBounds> {
    let bounds = DateBounds { start, end };
    bounds.is_active().then_some(bounds)
}

pub fn within_bounds(min: &MinimalRecord, bounds: Option<DateBounds>) -> bool {
    let Some(bounds) = bounds else {
        return true;
    };

    let Some(ts) = min.created_utc else {
        return false;
    };

    // A `created_utc` outside the representable `OffsetDateTime` range (e.g. an
    // absurd magnitude that still coerces to a valid i64) cannot be proven to
    // fall inside the requested window. Reject it as a non-match rather than
    // folding it to 1970, mirroring the TimestampBounds fast path.
    let Some(ym) = ym_from_epoch_checked(ts) else {
        return false;
    };
    if let Some(lo) = bounds.start {
        if ym < lo {
            return false;
        }
    }
    if let Some(hi) = bounds.end {
        if ym > hi {
            return false;
        }
    }
    true
}

pub fn ym_from_epoch(ts: i64) -> YearMonth {
    let dt = OffsetDateTime::from_unix_timestamp(ts).unwrap_or_else(|_| OffsetDateTime::UNIX_EPOCH);
    let date: Date = dt.date();
    let year = date.year().clamp(0, u16::MAX as i32) as u16;
    let month = date.month() as u8;
    YearMonth { year, month }
}

/// `ym_from_epoch` variant that returns `None` for a timestamp outside the
/// representable `OffsetDateTime` range (or a year outside `u16`) instead of
/// folding it to 1970. Mirrors `unix_timestamp_to_year_month` in
/// `src/query/timestamps.rs`.
pub fn ym_from_epoch_checked(ts: i64) -> Option<YearMonth> {
    let dt = OffsetDateTime::from_unix_timestamp(ts).ok()?;
    let date: Date = dt.date();
    let year = date.year();
    if !(0..=u16::MAX as i32).contains(&year) {
        return None;
    }
    Some(YearMonth {
        year: year as u16,
        month: date.month() as u8,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zstd_jsonl::parse_minimal;

    fn rec(ts: i64) -> MinimalRecord {
        parse_minimal(&format!(r#"{{"created_utc":{ts}}}"#)).expect("valid minimal record")
    }

    fn ym(year: u16, month: u8) -> YearMonth {
        YearMonth { year, month }
    }

    #[test]
    fn out_of_range_created_utc_is_rejected_not_folded_to_1970() {
        // 99999999999999 coerces to a valid i64 but is far outside the
        // representable OffsetDateTime range. The old code folded it to 1970:
        // wrongly dropped under a 2015 start bound, wrongly kept under an
        // end-only bound. It must now be a non-match either way.
        let absurd = rec(99_999_999_999_999);
        assert!(!within_bounds(&absurd, bounds_tuple(Some(ym(2015, 1)), None)));
        assert!(!within_bounds(
            &absurd,
            bounds_tuple(None, Some(ym(2020, 12)))
        ));
    }

    #[test]
    fn in_range_created_utc_still_passes() {
        // 1529020800 == 2018-06-15T00:00:00Z.
        let r = rec(1_529_020_800);
        assert!(within_bounds(
            &r,
            bounds_tuple(Some(ym(2015, 1)), Some(ym(2020, 12)))
        ));
    }

    #[test]
    fn ym_from_epoch_checked_rejects_absurd_timestamp() {
        assert!(ym_from_epoch_checked(99_999_999_999_999).is_none());
        assert_eq!(ym_from_epoch_checked(1_529_020_800), Some(ym(2018, 6)));
    }
}
