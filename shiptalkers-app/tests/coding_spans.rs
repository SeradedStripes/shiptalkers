use ship_talkers::auth::{date_plus_days, span_overlap_seconds};
use ship_talkers::slack::{TimeRange, build_coding_query, parse_time_range_at};

// --- span_overlap_seconds (pure math, mirrors the SQL) ---

#[test]
fn span_within_range_counts_in_full() {
    // 2026-08-10 12:00 -> 12:30 UTC, range 00:00 -> midnight
    assert_eq!(
        span_overlap_seconds(
            1_786_363_200,
            1800,
            Some(1_786_320_000),
            Some(1_786_406_400)
        ),
        1800
    );
}

#[test]
fn range_inside_span_counts_in_full() {
    // span 2026-08-10 11:00 -> 13:00 UTC, range 12:00 -> 12:30
    assert_eq!(
        span_overlap_seconds(
            1_786_359_600,
            7200,
            Some(1_786_363_200),
            Some(1_786_365_000)
        ),
        1800
    );
}

#[test]
fn span_crossing_range_start_counts_only_overlap() {
    // span 11:00 -> 13:00 UTC, range 12:00 -> 14:00 = 1h
    assert_eq!(
        span_overlap_seconds(
            1_786_359_600,
            7200,
            Some(1_786_363_200),
            Some(1_786_370_400)
        ),
        3600
    );
}

#[test]
fn span_crossing_range_end_counts_only_overlap() {
    // span 12:00 -> 14:00 UTC, range 11:00 -> 13:00 = 1h
    assert_eq!(
        span_overlap_seconds(
            1_786_363_200,
            7200,
            Some(1_786_320_000),
            Some(1_786_366_800)
        ),
        3600
    );
}

#[test]
fn span_entirely_before_range_counts_zero() {
    assert_eq!(
        span_overlap_seconds(1_786_363_200, 600, Some(1_786_366_800), Some(1_786_449_600)),
        0
    );
}

#[test]
fn span_entirely_after_range_counts_zero() {
    assert_eq!(
        span_overlap_seconds(1_786_449_600, 600, Some(1_786_320_000), Some(1_786_366_800)),
        0
    );
}

#[test]
fn unbounded_range_counts_full_duration() {
    assert_eq!(span_overlap_seconds(1_786_363_200, 5400, None, None), 5400);
}

#[test]
fn since_only_range_clips_the_start() {
    // span 11:00 -> 13:00 UTC, range starting 12:00 = 1h
    assert_eq!(
        span_overlap_seconds(1_786_359_600, 7200, Some(1_786_363_200), None),
        3600
    );
}

// --- build_coding_query (SQL construction and bind order) ---

fn max_placeholder(sql: &str) -> usize {
    sql.split('$')
        .skip(1)
        .filter_map(|s| {
            s.chars()
                .take_while(|c| c.is_ascii_digit())
                .collect::<String>()
                .parse::<usize>()
                .ok()
        })
        .max()
        .unwrap_or(0)
}

#[test]
fn all_time_query_has_no_range_binds() {
    let range = TimeRange::AllTime;
    let (sql, binds) = build_coding_query(&range);
    assert_eq!(binds, Vec::<i64>::new());
    assert_eq!(
        sql,
        "SELECT sum(duration)::bigint FROM hackatime_spans WHERE slack_id = $1"
    );
}

#[test]
fn since_query_reuses_start_placeholder() {
    let range = TimeRange::Since(1_786_363_200);
    let (sql, binds) = build_coding_query(&range);
    assert_eq!(binds, vec![1_786_363_200]);
    assert!(sql.ends_with("WHERE slack_id = $2"));
    assert_eq!(sql.matches('$').count(), 3);
}

#[test]
fn between_query_binds_start_end() {
    let start: i64 = 1_751_328_000;
    let end: i64 = 1_753_920_000;
    let range = TimeRange::Between(start, end);
    let (sql, binds) = build_coding_query(&range);
    assert_eq!(binds, vec![start, end]);
    assert!(sql.ends_with("WHERE slack_id = $3"));
    assert_eq!(sql.matches('$').count(), 5);
}

#[test]
fn between_query_with_negative_timestamps() {
    let range = TimeRange::Between(-100, 200);
    let (sql, binds) = build_coding_query(&range);
    assert_eq!(binds[0], -100);
    assert_eq!(binds[1], 200);
    assert!(sql.ends_with("WHERE slack_id = $3"));
}

#[test]
fn sql_uses_start_ts_in_seconds_not_microseconds() {
    // The original bug divided start_ts by 1_000_000 in the SQL.
    // start_ts is stored as unix seconds in the DB.
    for range in [TimeRange::Since(100), TimeRange::Between(100, 200)] {
        let (sql, _) = build_coding_query(&range);
        assert!(
            !sql.contains("/ 1000000"),
            "SQL must not divide start_ts by 1000000: {sql}"
        );
        assert!(
            !sql.contains("/1000000"),
            "SQL must not divide start_ts by 1000000: {sql}"
        );
        assert!(
            !sql.contains("* 1000000"),
            "SQL must not multiply by 1000000: {sql}"
        );
        assert!(
            sql.contains("start_ts"),
            "SQL must reference start_ts column: {sql}"
        );
    }
}

#[test]
fn range_queries_use_case_not_clickhouse_if() {
    // ClickHouse's if() types the 0 literal as Int64 while the subtraction is
    // UInt64, which sum() rejects; the Postgres port uses CASE ... ELSE 0 END.
    for range in [TimeRange::Since(100), TimeRange::Between(100, 200)] {
        let (sql, _) = build_coding_query(&range);
        assert!(
            sql.starts_with("SELECT sum(CASE WHEN "),
            "SQL must use CASE WHEN for range-scoped overlap: {sql}"
        );
    }
}

#[test]
fn where_slack_id_is_always_last_placeholder() {
    // The original bug bound user before the range values.
    // The SQL must end with WHERE slack_id = $N, making it the final
    // placeholder so the caller can bind range values first, user last.
    for (i, range) in [
        TimeRange::AllTime,
        TimeRange::Since(100),
        TimeRange::Between(100, 200),
    ]
    .into_iter()
    .enumerate()
    {
        let (sql, _) = build_coding_query(&range);
        let last = format!("WHERE slack_id = ${}", i + 1);
        assert!(sql.ends_with(&last), "SQL must end with '{last}': {sql}");
        let where_clause = &sql[sql.find("WHERE").unwrap()..];
        assert_eq!(
            where_clause.matches('$').count(),
            1,
            "WHERE clause must have exactly one placeholder: {where_clause}"
        );
    }
}

#[test]
fn bind_count_matches_expression_placeholders() {
    // Range binds from build_coding_query cover every placeholder except the
    // final WHERE slack_id = $N (which the caller binds separately as the user).
    for range in [
        TimeRange::AllTime,
        TimeRange::Since(100),
        TimeRange::Between(100, 200),
    ] {
        let (sql, binds) = build_coding_query(&range);
        // The highest placeholder number is reserved for slack_id; every
        // number below it belongs to the range expression.
        let user_placeholder = max_placeholder(&sql);
        assert_eq!(
            binds.len() + 1,
            user_placeholder,
            "range bind count ({}) != range placeholder count ({}) in: {sql}",
            binds.len(),
            user_placeholder - 1,
        );
    }
}

// --- End-to-end: parse_time_range -> build_coding_query -> span_overlap_seconds ---
// For each keyword, parse the range, build the SQL, then verify that
// span_overlap_seconds (which mirrors the SQL logic) produces the expected
// result. This tests that parse_time_range returns sane boundaries and that
// the Rust overlap math matches the SQL semantics.

const NOW: i64 = 1_787_040_000; // 2026-08-18 00:00 UTC

#[test]
fn e2e_last_month_span_inside_range() {
    let range = parse_time_range_at("last month", NOW).unwrap();
    let span_start = range.start_ts().unwrap() as u64 + 3600;
    assert_eq!(
        span_overlap_seconds(span_start, 3600, range.start_ts(), range.end_ts()),
        3600
    );
    let (sql, binds) = build_coding_query(&range);
    assert_eq!(max_placeholder(&sql), binds.len() + 1);
    assert!(sql.ends_with(&format!("slack_id = ${}", binds.len() + 1)));
}

#[test]
fn e2e_last_month_span_outside_range() {
    let range = parse_time_range_at("last month", NOW).unwrap();
    let span_start = range.end_ts().unwrap() as u64 + 1000;
    assert_eq!(
        span_overlap_seconds(span_start, 3600, range.start_ts(), range.end_ts()),
        0
    );
}

#[test]
fn e2e_last_week_span_inside_range() {
    let range = parse_time_range_at("last week", NOW).unwrap();
    let span_start = range.start_ts().unwrap() as u64 + 3600;
    assert_eq!(
        span_overlap_seconds(span_start, 7200, range.start_ts(), range.end_ts()),
        7200
    );
    let (sql, binds) = build_coding_query(&range);
    assert_eq!(max_placeholder(&sql), binds.len() + 1);
}

#[test]
fn e2e_today_span_inside_range() {
    let range = parse_time_range_at("today", NOW).unwrap();
    let span_start = range.start_ts().unwrap() as u64 + 3600;
    assert_eq!(
        span_overlap_seconds(span_start, 1800, range.start_ts(), range.end_ts()),
        1800
    );
    let (sql, binds) = build_coding_query(&range);
    assert_eq!(max_placeholder(&sql), binds.len() + 1);
}

#[test]
fn e2e_all_time_always_returns_full_duration() {
    let range = parse_time_range_at("all time", NOW).unwrap();
    let (sql, binds) = build_coding_query(&range);
    assert_eq!(binds, Vec::<i64>::new());
    assert_eq!(max_placeholder(&sql), binds.len() + 1);
    assert_eq!(
        span_overlap_seconds(1_000_000_000, 5400, range.start_ts(), range.end_ts()),
        5400
    );
}

// --- date helpers ---

#[test]
fn date_plus_days_crosses_boundaries() {
    assert_eq!(date_plus_days("2026-08-10", 0).unwrap(), "2026-08-10");
    assert_eq!(date_plus_days("2026-08-10", 1).unwrap(), "2026-08-11");
    assert_eq!(date_plus_days("2026-08-10", -1).unwrap(), "2026-08-09");
    assert_eq!(date_plus_days("2026-08-31", 1).unwrap(), "2026-09-01");
    assert_eq!(date_plus_days("2025-12-31", 1).unwrap(), "2026-01-01");
    assert_eq!(date_plus_days("2026-03-01", -1).unwrap(), "2026-02-28");
    assert_eq!(date_plus_days("2024-02-29", 1).unwrap(), "2024-03-01");
    assert_eq!(date_plus_days("2024-02-29", 366).unwrap(), "2025-03-01");
}

#[test]
fn date_plus_days_rejects_bad_input() {
    assert!(date_plus_days("2026-13-01", 0).is_none());
    assert!(date_plus_days("2026-00-01", 0).is_none());
    assert!(date_plus_days("2026-08-40", 0).is_none());
    assert!(date_plus_days("2026-08-10-extra", 0).is_none());
    assert!(date_plus_days("garbage", 0).is_none());
    assert!(date_plus_days("", 0).is_none());
}
