use std::fmt;
use std::str::FromStr;

/// Simple "YYYY-MM" utility with safe arithmetic and ordering.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct YearMonth {
    pub year: u16,
    pub month: u8, // 1..=12
}

impl YearMonth {
    pub fn new(year: u16, month: u8) -> Self {
        assert!((1..=12).contains(&month), "Month must be 1..=12");
        Self { year, month }
    }
    pub fn next(self) -> Option<Self> {
        if self.month < 12 {
            Some(Self { year: self.year, month: self.month + 1 })
        } else if self.year < u16::MAX {
            Some(Self { year: self.year + 1, month: 1 })
        } else {
            None
        }
    }
    pub fn prev(self) -> Option<Self> {
        if self.month > 1 {
            Some(Self { year: self.year, month: self.month - 1 })
        } else if self.year > 0 {
            Some(Self { year: self.year - 1, month: 12 })
        } else {
            None
        }
    }
}

impl fmt::Display for YearMonth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}-{:02}", self.year, self.month)
    }
}

impl FromStr for YearMonth {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<_> = s.split('-').collect();
        if parts.len() != 2 {
            return Err("expected YYYY-MM".into());
        }
        let year: u16 = parts[0].parse().map_err(|_| "invalid year")?;
        let month: u8 = parts[1].parse().map_err(|_| "invalid month")?;
        if !(1..=12).contains(&month) {
            return Err("month must be 01..12".into());
        }
        Ok(Self { year, month })
    }
}

/// Inclusive iteration from `start` to `end` (if `start` <= `end`), else empty.
pub fn iter_year_months(start: YearMonth, end: YearMonth) -> impl Iterator<Item = YearMonth> {
    let mut curr = if start <= end { Some(start) } else { None };
    std::iter::from_fn(move || {
        let ret = curr?;
        curr = ret.next().filter(|n| *n <= end);
        Some(ret)
    })
}
