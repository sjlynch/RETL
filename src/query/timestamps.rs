
/// Inclusive/exclusive fast-path bounds for a record's top-level
/// `created_utc` Unix timestamp.
///
/// `created_utc_gte` is inclusive (`created_utc >= value`) and
/// `created_utc_lt` is exclusive (`created_utc < value`). When either side is
/// present, records whose `created_utc` is missing or not an integer timestamp
/// are rejected on the [`MinimalRecord`](crate::MinimalRecord) fast path.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TimestampBounds {
    pub created_utc_gte: Option<i64>,
    pub created_utc_lt: Option<i64>,
}

impl TimestampBounds {
    pub const fn new(created_utc_gte: Option<i64>, created_utc_lt: Option<i64>) -> Self {
        Self {
            created_utc_gte,
            created_utc_lt,
        }
    }

    #[inline]
    pub fn is_active(self) -> bool {
        self.created_utc_gte.is_some() || self.created_utc_lt.is_some()
    }

    #[inline]
    pub fn contains(self, created_utc: i64) -> bool {
        if let Some(lo) = self.created_utc_gte {
            if created_utc < lo {
                return false;
            }
        }
        if let Some(hi) = self.created_utc_lt {
            if created_utc >= hi {
                return false;
            }
        }
        true
    }

    pub(crate) fn validate(self) -> Result<(), QueryBuildError> {
        if let (Some(lo), Some(hi)) = (self.created_utc_gte, self.created_utc_lt) {
            if lo >= hi {
                return Err(QueryBuildError::new(format!(
                    "created_utc_gte ({lo}) must be less than created_utc_lt ({hi})"
                )));
            }
        }

        for (field, value) in [
            ("created_utc_gte", self.created_utc_gte),
            ("created_utc_lt", self.created_utc_lt),
        ] {
            if let Some(ts) = value {
                unix_timestamp_to_year_month(ts).ok_or_else(|| {
                    QueryBuildError::new(format!(
                        "{field} ({ts}) is outside RETL's supported timestamp range"
                    ))
                })?;
            }
        }

        Ok(())
    }

    pub(crate) fn derived_start_month(self) -> Option<YearMonth> {
        self.created_utc_gte.and_then(unix_timestamp_to_year_month)
    }

    pub(crate) fn derived_end_month(self) -> Option<YearMonth> {
        self.created_utc_lt
            .and_then(|ts| ts.checked_sub(1))
            .and_then(unix_timestamp_to_year_month)
    }
}

fn unix_timestamp_to_year_month(ts: i64) -> Option<YearMonth> {
    let dt = OffsetDateTime::from_unix_timestamp(ts).ok()?;
    let year = dt.year();
    if !(0..=u16::MAX as i32).contains(&year) {
        return None;
    }
    Some(YearMonth {
        year: year as u16,
        month: dt.month() as u8,
    })
}
