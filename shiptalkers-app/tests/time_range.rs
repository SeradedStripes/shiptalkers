use ship_talkers::slack::{TimeRange, parse_time_range_at};

// 2026-08-15 00:00:00 UTC, a Saturday.
const NOW: i64 = 1_786_752_000;

const MONTH_DAYS: [i64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

fn is_leap(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn ts(year: i64, month: i64, day: i64) -> i64 {
    let mut days = 0;
    for yy in 1970..year {
        days += if is_leap(yy) { 366 } else { 365 };
    }
    for mm in 0..(month - 1) {
        days += MONTH_DAYS[mm as usize];
        if mm == 1 && is_leap(year) {
            days += 1;
        }
    }
    (days + day - 1) * 86400
}

fn range(text: &str) -> (Option<i64>, Option<i64>) {
    let r = parse_time_range_at(text, NOW).expect("keyword should parse");
    (r.start_ts(), r.end_ts())
}

#[test]
fn calendar_ranges_are_exact() {
    assert_eq!(range("today"), (Some(ts(2026, 8, 15)), None));
    assert_eq!(
        range("yesterday"),
        (Some(ts(2026, 8, 14)), Some(ts(2026, 8, 15)))
    );
    assert_eq!(range("this week"), (Some(ts(2026, 8, 10)), None));
    assert_eq!(
        range("last week"),
        (Some(ts(2026, 8, 3)), Some(ts(2026, 8, 10)))
    );
    assert_eq!(range("this month"), (Some(ts(2026, 8, 1)), None));
    assert_eq!(
        range("last month"),
        (Some(ts(2026, 7, 1)), Some(ts(2026, 8, 1)))
    );
    assert_eq!(range("this year"), (Some(ts(2026, 1, 1)), None));
    assert_eq!(
        range("last year"),
        (Some(ts(2025, 1, 1)), Some(ts(2026, 1, 1)))
    );
}

#[test]
fn rolling_ranges() {
    assert_eq!(range("one second"), (Some(NOW - 1), None));
    assert_eq!(range("oneday"), (Some(NOW - 86400), None));
    assert_eq!(range("one day"), (Some(NOW - 86400), None));
    assert_eq!(range("this hour"), (Some(NOW), None));
    assert_eq!(range("last hour"), (Some(NOW - 3600), None));
    assert_eq!(range("all time"), (None, None));
    assert_eq!(range("alltime"), (None, None));
}

#[test]
fn keywords_match_inside_sentences() {
    assert!(parse_time_range_at("show me last month", NOW).is_some());
    assert!(parse_time_range_at("stats for yesterday", NOW).is_some());
    assert!(parse_time_range_at("one second of stats", NOW).is_some());
    assert!(parse_time_range_at("no keyword here", NOW).is_none());
}

#[test]
fn time_range_has_exclusive_end_dates() {
    let yesterday = parse_time_range_at("yesterday", NOW).unwrap();
    assert_eq!(yesterday.start_date().as_deref(), Some("2026-08-14"));
    assert_eq!(yesterday.end_date().as_deref(), Some("2026-08-15"));
    let last_month = parse_time_range_at("last month", NOW).unwrap();
    assert_eq!(last_month.start_date().as_deref(), Some("2026-07-01"));
    assert_eq!(last_month.end_date().as_deref(), Some("2026-08-01"));
    let all_time = TimeRange::AllTime;
    assert_eq!(all_time.start_date(), None);
}
