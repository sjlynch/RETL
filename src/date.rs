use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

/// Simple "YYYY-MM" utility with safe arithmetic and ordering.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct YearMonth {
    pub year: u16,
    pub month: u8, // 1..=12
}

impl YearMonth {
    /// Construct a `YearMonth` from a year and a `1..=12` month.
    ///
    /// # Panics
    ///
    /// Panics if `month` is not in `1..=12`. Use [`YearMonth::try_new`] for a
    /// non-panicking constructor when `month` is computed at runtime (e.g.
    /// from arithmetic) and may fall outside the valid range.
    pub fn new(year: u16, month: u8) -> Self {
        Self::try_new(year, month).expect("Month must be 1..=12")
    }
    /// Construct a `YearMonth`, returning `None` when `month` is outside
    /// `1..=12`.
    ///
    /// Prefer this over [`YearMonth::new`] for runtime-computed months — `new`
    /// panics on an out-of-range month, which a library caller deriving a
    /// month from arithmetic generally does not want.
    pub fn try_new(year: u16, month: u8) -> Option<Self> {
        if (1..=12).contains(&month) {
            Some(Self { year, month })
        } else {
            None
        }
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

impl Serialize for YearMonth {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for YearMonth {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl FromStr for YearMonth {
    type Err = String;
    /// Parses a strict `YYYY-MM` (exactly 7 ASCII bytes, four-digit year, `-`,
    /// two-digit month). Loose forms like `2024-1`, `2-1`, `24-01`, or
    /// `02024-01` are rejected so `parse → Display` is bit-for-bit stable.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = s.as_bytes();
        if bytes.len() != 7 || bytes[4] != b'-' {
            return Err("expected YYYY-MM".into());
        }
        if !bytes[..4].iter().all(u8::is_ascii_digit) {
            return Err("invalid year".into());
        }
        if !bytes[5..].iter().all(u8::is_ascii_digit) {
            return Err("invalid month".into());
        }
        let year: u16 = s[..4].parse().map_err(|_| "invalid year")?;
        let month: u8 = s[5..].parse().map_err(|_| "invalid month")?;
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
