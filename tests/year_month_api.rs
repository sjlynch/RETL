//! Direct tests for `YearMonth` arithmetic / parsing / display. The public API
//! ships `YearMonth::new/next/prev`, `Display`, `FromStr`, and `Ord`, but until
//! now nothing exercised these directly. The pipeline only ever observes them
//! through the file planner where typical inputs (2005..) never hit the
//! year-boundary or u16::MAX edges.

use retl::YearMonth;
use std::str::FromStr;

#[test]
fn next_advances_within_year() {
    let ym = YearMonth::new(2020, 5);
    assert_eq!(ym.next(), Some(YearMonth::new(2020, 6)));
}

#[test]
fn next_wraps_december_to_next_january() {
    let ym = YearMonth::new(2020, 12);
    assert_eq!(ym.next(), Some(YearMonth::new(2021, 1)));
}

#[test]
fn next_at_u16_max_december_returns_none() {
    let ym = YearMonth::new(u16::MAX, 12);
    assert!(ym.next().is_none(), "next() at u16::MAX/12 must saturate to None");
}

#[test]
fn next_at_u16_max_november_advances_into_december() {
    let ym = YearMonth::new(u16::MAX, 11);
    assert_eq!(ym.next(), Some(YearMonth::new(u16::MAX, 12)));
}

#[test]
fn prev_steps_within_year() {
    let ym = YearMonth::new(2020, 5);
    assert_eq!(ym.prev(), Some(YearMonth::new(2020, 4)));
}

#[test]
fn prev_wraps_january_to_previous_december() {
    let ym = YearMonth::new(2020, 1);
    assert_eq!(ym.prev(), Some(YearMonth::new(2019, 12)));
}

#[test]
fn prev_at_year_zero_january_returns_none() {
    let ym = YearMonth::new(0, 1);
    assert!(ym.prev().is_none(), "prev() at 0/1 must saturate to None");
}

#[test]
fn display_pads_single_digit_month_and_year_with_zeros() {
    assert_eq!(format!("{}", YearMonth::new(2006, 1)), "2006-01");
    assert_eq!(format!("{}", YearMonth::new(2006, 12)), "2006-12");
    assert_eq!(format!("{}", YearMonth::new(7, 3)), "0007-03");
    assert_eq!(format!("{}", YearMonth::new(0, 1)), "0000-01");
}

#[test]
fn fromstr_parses_canonical_and_round_trips_display() {
    let ym: YearMonth = "2006-01".parse().expect("canonical form parses");
    assert_eq!(ym, YearMonth::new(2006, 1));
    assert_eq!(format!("{}", ym), "2006-01");

    // Round-trip through Display + FromStr for several years.
    for (y, m) in [(2005u16, 12u8), (2006, 2), (2020, 9), (1999, 3)] {
        let s = format!("{}", YearMonth::new(y, m));
        assert_eq!(YearMonth::from_str(&s).unwrap(), YearMonth::new(y, m));
    }
}

#[test]
fn fromstr_rejects_malformed_inputs() {
    assert!(YearMonth::from_str("").is_err());
    assert!(YearMonth::from_str("2006").is_err());
    assert!(YearMonth::from_str("2006-").is_err());
    assert!(YearMonth::from_str("-01").is_err());
    assert!(YearMonth::from_str("2006-01-15").is_err());
    assert!(YearMonth::from_str("2006/01").is_err());
}

#[test]
fn fromstr_rejects_out_of_range_month() {
    assert!(YearMonth::from_str("2006-00").is_err());
    assert!(YearMonth::from_str("2006-13").is_err());
    assert!(YearMonth::from_str("2006-99").is_err());
}

#[test]
fn fromstr_rejects_non_numeric_components() {
    assert!(YearMonth::from_str("abcd-01").is_err());
    assert!(YearMonth::from_str("2006-jan").is_err());
}

#[test]
fn ord_compares_year_then_month() {
    let a = YearMonth::new(2005, 12);
    let b = YearMonth::new(2006, 1);
    let c = YearMonth::new(2006, 2);
    assert!(a < b);
    assert!(b < c);
    assert!(a < c);
    assert_eq!(b, YearMonth::new(2006, 1));

    // Same year, different month
    assert!(YearMonth::new(2010, 3) < YearMonth::new(2010, 4));
    // Same month, different year
    assert!(YearMonth::new(2009, 6) < YearMonth::new(2010, 6));
}

#[test]
#[should_panic(expected = "Month must be 1..=12")]
fn new_panics_on_zero_month() {
    let _ = YearMonth::new(2020, 0);
}

#[test]
#[should_panic(expected = "Month must be 1..=12")]
fn new_panics_on_thirteen_month() {
    let _ = YearMonth::new(2020, 13);
}
