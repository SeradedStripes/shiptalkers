use ship_talkers::auth::{date_plus_days, span_overlap_seconds};

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

#[test]
fn start_time_fraction_is_truncated_at_insert() {
    // The API returns a fractional f64 which is truncated to whole seconds
    // by `as u64` in the insert; span_overlap_seconds receives already-truncated
    // seconds, so 1786363200.157 becomes 1786363200.
    assert_eq!(
        span_overlap_seconds(1_786_363_200, 300, Some(1_786_320_000), Some(1_786_406_400)),
        300
    );
}

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
