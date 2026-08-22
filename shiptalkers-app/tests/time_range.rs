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
fn digit_count_ranges() {
    assert_eq!(range("2 days"), (Some(NOW - 2 * 86400), None));
    assert_eq!(range("12 hours"), (Some(NOW - 12 * 3600), None));
    assert_eq!(range("12 minutes"), (Some(NOW - 720), None));
    assert_eq!(range("90 seconds"), (Some(NOW - 90), None));
    assert_eq!(range("5 weeks"), (Some(NOW - 35 * 86400), None));
    assert_eq!(range("3 months"), (Some(NOW - 90 * 86400), None));
    assert_eq!(range("6 months"), (Some(NOW - 180 * 86400), None));
    assert_eq!(range("1 year"), (Some(NOW - 365 * 86400), None));
    assert_eq!(range("2 hours"), (Some(NOW - 7200), None));
}

#[test]
fn word_count_ranges() {
    assert_eq!(range("two hours"), (Some(NOW - 7200), None));
    assert_eq!(range("three months"), (Some(NOW - 90 * 86400), None));
    assert_eq!(range("seven days"), (Some(NOW - 7 * 86400), None));
    assert_eq!(range("twenty four hours"), (Some(NOW - 86400), None));
    assert_eq!(range("twenty-four days"), (Some(NOW - 24 * 86400), None));
    assert_eq!(range("forty five minutes"), (Some(NOW - 2700), None));
    assert_eq!(range("ninety seconds"), (Some(NOW - 90), None));
}

#[test]
fn large_word_numbers_with_scales_and_and() {
    assert_eq!(
        range("two hundred million and ninety nine seconds"),
        (Some(NOW - 200_000_099), None)
    );
    assert_eq!(
        range("three thousand five hundred hours"),
        (Some(NOW - 3500 * 3600), None)
    );
    assert_eq!(
        range("one hundred and one days"),
        (Some(NOW - 101 * 86400), None)
    );
    assert_eq!(range("hundred days"), (Some(NOW - 100 * 86400), None));
    assert_eq!(range("2 million seconds"), (Some(NOW - 2_000_000), None));
    assert_eq!(range("five hundred days"), (Some(NOW - 500 * 86400), None));
}

#[test]
fn sub_second_units_floor_to_one_second() {
    assert_eq!(range("one thousand milliseconds"), (Some(NOW - 1), None));
    assert_eq!(range("500 milliseconds"), (Some(NOW - 1), None));
    assert_eq!(range("250 ms"), (Some(NOW - 1), None));
    assert_eq!(range("one microsecond"), (Some(NOW - 1), None));
    assert_eq!(range("2 nanoseconds"), (Some(NOW - 1), None));
    assert_eq!(range("100 ns"), (Some(NOW - 1), None));
    assert_eq!(range("one nano second"), (Some(NOW - 1), None));
    assert_eq!(range("1500 milliseconds"), (Some(NOW - 1), None));
    assert_eq!(range("90000 milliseconds"), (Some(NOW - 90), None));
}

#[test]
fn keywords_match_inside_sentences() {
    assert!(parse_time_range_at("show me last month", NOW).is_some());
    assert!(parse_time_range_at("stats for yesterday", NOW).is_some());
    assert!(parse_time_range_at("one second of stats", NOW).is_some());
    assert!(parse_time_range_at("show me 30 days of stats", NOW).is_some());
    assert!(parse_time_range_at("how about twenty minutes", NOW).is_some());
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
