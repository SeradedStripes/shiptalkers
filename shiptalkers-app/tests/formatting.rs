use ship_talkers::auth::civil_from_days;
use ship_talkers::website::{fmt_duration, fmt_hour, fmt_minutes, fmt_thousands, parse_ts};

#[test]
fn fmt_thousands_groups_with_commas() {
    assert_eq!(fmt_thousands(0), "0");
    assert_eq!(fmt_thousands(1), "1");
    assert_eq!(fmt_thousands(999), "999");
    assert_eq!(fmt_thousands(1000), "1,000");
    assert_eq!(fmt_thousands(1234567), "1,234,567");
    assert_eq!(fmt_thousands(1000000000), "1,000,000,000");
}

#[test]
fn fmt_duration_hours_and_minutes() {
    assert_eq!(fmt_duration(0), "0m");
    assert_eq!(fmt_duration(150), "2m");
    assert_eq!(fmt_duration(3661), "1h 1m");
    assert_eq!(fmt_duration(7200), "2h 0m");
    assert_eq!(fmt_duration(86400), "24h 0m");
}

#[test]
fn fmt_minutes_hrs_min() {
    assert_eq!(fmt_minutes(0), "0hrs 0min");
    assert_eq!(fmt_minutes(59), "0hrs 59min");
    assert_eq!(fmt_minutes(90), "1hrs 30min");
    assert_eq!(fmt_minutes(600), "10hrs 0min");
}

#[test]
fn fmt_hour_is_12_hour_clock() {
    assert_eq!(fmt_hour(0), "12 AM");
    assert_eq!(fmt_hour(7), "7 AM");
    assert_eq!(fmt_hour(11), "11 AM");
    assert_eq!(fmt_hour(12), "12 PM");
    assert_eq!(fmt_hour(13), "1 PM");
    assert_eq!(fmt_hour(23), "11 PM");
}

#[test]
fn parse_ts_breaks_micros_into_utc_parts() {
    // 1700000000 = 2023-11-14 22:13:20 UTC
    let (y, m, d, h, min) = parse_ts(1_700_000_000_000_000).expect("parses");
    assert_eq!((y, m, d, h, min), (2023, 11, 14, 22, 13));
    assert_eq!(parse_ts(0), None);
    assert_eq!(parse_ts(999), None);
}

#[test]
fn civil_from_days_matches_known_dates() {
    assert_eq!(civil_from_days(0), (1970, 1, 1));
    assert_eq!(civil_from_days(11_000), (2000, 2, 13));
    assert_eq!(civil_from_days(18_262), (2020, 1, 1));
    assert_eq!(civil_from_days(20_454), (2026, 1, 1));
    assert_eq!(civil_from_days(20_680), (2026, 8, 15));
}
