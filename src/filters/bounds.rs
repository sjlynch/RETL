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

    let ym = ym_from_epoch(ts);
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
