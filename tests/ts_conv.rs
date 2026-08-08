use ship_talkers::db::clickhouse_db::{micros_to_slack_ts, parse_date, slack_ts_to_micros};

#[test]
fn slack_ts_to_micros_handles_secs_and_frac() {
    assert_eq!(
        slack_ts_to_micros("1700000000.123456"),
        1_700_000_000_123_456
    );
    assert_eq!(slack_ts_to_micros("1700000000"), 1_700_000_000_000_000);
    assert_eq!(slack_ts_to_micros("0.000001"), 1);
}

#[test]
fn micros_to_slack_ts_formats_padded_frac() {
    assert_eq!(
        micros_to_slack_ts(1_700_000_000_123_456),
        "1700000000.123456"
    );
    assert_eq!(micros_to_slack_ts(1), "0.000001");
    assert_eq!(
        micros_to_slack_ts(1_700_000_000_000_000),
        "1700000000.000000"
    );
}

#[test]
fn ts_conversion_round_trips() {
    for ts in [
        "1672531200.000000",
        "1700000000.123456",
        "0.000001",
        "9999999999.999999",
    ] {
        assert_eq!(micros_to_slack_ts(slack_ts_to_micros(ts)), ts);
    }
}

#[test]
fn parse_date_accepts_iso_and_rejects_bad() {
    assert_eq!(
        parse_date("2026-08-15").unwrap(),
        time::Date::from_calendar_date(2026, time::Month::August, 15).unwrap()
    );
    assert_eq!(
        parse_date("2024-02-29").unwrap(),
        time::Date::from_calendar_date(2024, time::Month::February, 29).unwrap()
    );
    assert!(parse_date("2023-02-29").is_none());
    assert!(parse_date("not a date").is_none());
    assert!(parse_date("2026-13-01").is_none());
}
