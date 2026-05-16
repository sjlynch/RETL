use time::format_description::well_known::Rfc3339;
use time::{Date, Month, OffsetDateTime, Time, UtcOffset};

const SAMPLE_BYTES_ZERO_ERROR: &str =
    "--sample-bytes must be > 0; use --mode full for complete validation";

pub(crate) fn parse_positive_sample_bytes(raw: &str) -> Result<u64, String> {
    let bytes = raw
        .parse::<u64>()
        .map_err(|e| format!("invalid byte count: {e}"))?;
    if bytes == 0 {
        Err(SAMPLE_BYTES_ZERO_ERROR.to_string())
    } else {
        Ok(bytes)
    }
}

pub(crate) fn parse_timestamp_bound(raw: &str) -> Result<i64, String> {
    let s = raw.trim();
    if s.is_empty() {
        return Err("timestamp bound cannot be empty".to_string());
    }

    if looks_like_integer(s) {
        return s
            .parse::<i64>()
            .map_err(|e| format!("invalid Unix epoch seconds {s:?}: {e}"));
    }

    if let Ok(dt) = OffsetDateTime::parse(s, &Rfc3339) {
        return Ok(dt.unix_timestamp());
    }
    if let Some(with_seconds) = add_missing_rfc3339_seconds(s) {
        if let Ok(dt) = OffsetDateTime::parse(&with_seconds, &Rfc3339) {
            return Ok(dt.unix_timestamp());
        }
    }

    if let Some(ts) = parse_yyyy_mm_dd_utc(s)? {
        return Ok(ts);
    }

    Err(format!(
        "invalid timestamp {s:?}; expected Unix epoch seconds, RFC3339 timestamp (e.g. 2020-11-03T00:00:00Z), or YYYY-MM-DD date (UTC midnight)"
    ))
}

pub(crate) fn looks_like_integer(s: &str) -> bool {
    let digits = s
        .strip_prefix('+')
        .or_else(|| s.strip_prefix('-'))
        .filter(|rest| !rest.is_empty())
        .unwrap_or(s);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}

pub(crate) fn add_missing_rfc3339_seconds(s: &str) -> Option<String> {
    let t_idx = s.find('T').or_else(|| s.find('t'))?;
    let rest = &s[t_idx + 1..];
    let offset_rel = rest.find(|ch| matches!(ch, 'Z' | 'z' | '+' | '-'))?;
    let time_part = &rest[..offset_rel];
    let bytes = time_part.as_bytes();
    if !(bytes.len() == 5
        && bytes[2] == b':'
        && bytes[..2].iter().all(u8::is_ascii_digit)
        && bytes[3..].iter().all(u8::is_ascii_digit))
    {
        return None;
    }

    let offset_idx = t_idx + 1 + offset_rel;
    let mut out = String::with_capacity(s.len() + 3);
    out.push_str(&s[..t_idx]);
    out.push('T');
    out.push_str(&s[t_idx + 1..offset_idx]);
    out.push_str(":00");
    out.push_str(&s[offset_idx..]);
    if out.ends_with('z') {
        out.pop();
        out.push('Z');
    }
    Some(out)
}

pub(crate) fn parse_yyyy_mm_dd_utc(s: &str) -> Result<Option<i64>, String> {
    let bytes = s.as_bytes();
    if !(bytes.len() == 10 && bytes[4] == b'-' && bytes[7] == b'-') {
        return Ok(None);
    }
    if !(bytes[..4].iter().all(u8::is_ascii_digit)
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[8..].iter().all(u8::is_ascii_digit))
    {
        return Ok(None);
    }

    let year = s[..4]
        .parse::<i32>()
        .map_err(|e| format!("invalid date year in {s:?}: {e}"))?;
    let month_num = s[5..7]
        .parse::<u8>()
        .map_err(|e| format!("invalid date month in {s:?}: {e}"))?;
    let day = s[8..]
        .parse::<u8>()
        .map_err(|e| format!("invalid date day in {s:?}: {e}"))?;
    let month = Month::try_from(month_num)
        .map_err(|_| format!("invalid date {s:?}: month must be 01..12"))?;
    let date = Date::from_calendar_date(year, month, day)
        .map_err(|e| format!("invalid date {s:?}: {e}"))?;
    Ok(Some(
        OffsetDateTime::new_in_offset(date, Time::MIDNIGHT, UtcOffset::UTC).unix_timestamp(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_flags_accept_epoch_rfc3339_minute_and_date_forms() {
        assert_eq!(parse_timestamp_bound("1606737600").unwrap(), 1_606_737_600);
        assert_eq!(
            parse_timestamp_bound("2020-11-30T12:00:00Z").unwrap(),
            1_606_737_600
        );
        assert_eq!(
            parse_timestamp_bound("2020-11-30T12:00Z").unwrap(),
            1_606_737_600
        );
        assert_eq!(parse_timestamp_bound("2020-12-01").unwrap(), 1_606_780_800);
    }
}
