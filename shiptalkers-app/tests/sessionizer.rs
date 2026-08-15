use ship_talkers::sessionize::sessionize;

#[test]
fn empty_input_gives_zeros() {
    let s = sessionize(&[]);
    assert_eq!(s.total_seconds, 0);
    assert_eq!(s.longest_seconds, 0);
    assert_eq!(s.session_count, 0);
    assert_eq!(s.day_count, 0);
}

#[test]
fn lone_message_earns_typing_time() {
    // 50 chars at 5 chars/s plus 10s reading overhead = 20s.
    let s = sessionize(&[(1000, 50)]);
    assert_eq!(s.session_count, 1);
    assert_eq!(s.total_seconds, 20);
    assert_eq!(s.longest_seconds, 20);
    assert_eq!(s.day_count, 1);
}

#[test]
fn lone_long_message_earns_more() {
    // 1000 chars at 5 chars/s plus 10s overhead = 210s.
    let s = sessionize(&[(1000, 1000)]);
    assert_eq!(s.session_count, 1);
    assert_eq!(s.total_seconds, 210);
    assert_eq!(s.longest_seconds, 210);
}

#[test]
fn close_messages_share_one_session() {
    let s = sessionize(&[(1000, 50), (1100, 50)]);
    assert_eq!(s.session_count, 1);
    assert_eq!(s.total_seconds, 120);
    assert_eq!(s.longest_seconds, 120);
    assert_eq!(s.day_count, 1);
}

#[test]
fn boundary_gap_keeps_one_session() {
    let s = sessionize(&[(1000, 50), (3100, 50)]);
    assert_eq!(s.session_count, 1);
    assert_eq!(s.total_seconds, 2120);
}

#[test]
fn gap_over_boundary_splits_sessions() {
    let s = sessionize(&[(1000, 50), (6000, 50)]);
    assert_eq!(s.session_count, 2);
    assert_eq!(s.total_seconds, 40);
    assert_eq!(s.longest_seconds, 20);
}

#[test]
fn long_session_caps_at_twelve_hours() {
    // 23 events 2000s apart stay under the boundary but span well past 12h.
    let events: Vec<(u64, u64)> = (0..23).map(|i| (1000 + 2000 * i, 50)).collect();
    let s = sessionize(&events);
    assert_eq!(s.session_count, 1);
    assert_eq!(s.total_seconds, 43200);
    assert_eq!(s.longest_seconds, 43200);
}

#[test]
fn same_second_messages_merge() {
    let s = sessionize(&[(1000, 20), (1000, 30), (1000, 10)]);
    assert_eq!(s.session_count, 1);
    // Merged into (1000, 60 chars, 3 messages): 60/5 + 3*10 = 42.
    assert_eq!(s.total_seconds, 42);
}

#[test]
fn unsorted_input_is_sorted() {
    let s = sessionize(&[(1100, 50), (1000, 50)]);
    assert_eq!(s.session_count, 1);
    assert_eq!(s.total_seconds, 120);
}

#[test]
fn days_count_spans_session_starts() {
    let day = 86400u64;
    let s = sessionize(&[
        (day, 50),
        (day + 100, 50),
        (3 * day, 50),
        (3 * day + 100, 50),
    ]);
    assert_eq!(s.session_count, 2);
    assert_eq!(s.day_count, 3);
}

#[test]
fn mixed_timeline_accumulates() {
    let s = sessionize(&[(0, 50), (60, 50), (120, 50), (5000, 50), (5100, 50)]);
    assert_eq!(s.session_count, 2);
    // Session 1: 0..120 + 20 = 140. Session 2: 5000..5100 + 20 = 120.
    assert_eq!(s.total_seconds, 260);
    assert_eq!(s.longest_seconds, 140);
    assert_eq!(s.day_count, 1);
}

#[test]
fn demo_sessionize_outputs() {
    let cases: [(&str, Vec<(u64, u64)>); 5] = [
        ("lone message (50 chars)", vec![(1000, 50)]),
        ("close pair (100s apart)", vec![(1000, 50), (1100, 50)]),
        ("gap over boundary (5000s)", vec![(1000, 50), (6000, 50)]),
        (
            "all-day burst capped at 12h",
            (0..23).map(|i| (1000 + 2000 * i, 50)).collect(),
        ),
        (
            "two sessions across 3 days",
            vec![
                (86400, 50),
                (86400 + 100, 50),
                (3 * 86400, 50),
                (3 * 86400 + 100, 50),
            ],
        ),
    ];
    println!();
    for (name, events) in cases {
        let s = sessionize(&events);
        println!(
            "{name: <28} events={events:?}\n  -> total={}s ({}h), sessions={}, longest={}s, days={}",
            s.total_seconds,
            s.total_seconds / 3600,
            s.session_count,
            s.longest_seconds,
            s.day_count
        );
    }
}

#[test]
fn deployed_sessionizer_matches_expected_slack_time() {
    // A realistic day: morning burst, afternoon burst, then a lone evening message.
    // The expected total pins the exact deployed semantics, so changing the
    // sessionizer fails here until the expected value is re-derived on purpose.
    let events: Vec<(u64, u64)> = vec![
        (8 * 3600, 120),
        (8 * 3600 + 60, 40),
        (8 * 3600 + 200, 300),
        (14 * 3600, 80),
        (14 * 3600 + 120, 200),
        (14 * 3600 + 300, 60),
        (22 * 3600, 500),
    ];
    let s = sessionize(&events);
    // Morning: 0..200 + 120/5+10 = 234. Afternoon: 0..300 + 80/5+10 = 326.
    // Evening lone: 500/5 + 10 = 110. Total 670.
    assert_eq!(s.total_seconds, 670);
    assert_eq!(s.longest_seconds, 326);
    assert_eq!(s.session_count, 3);
    assert_eq!(s.day_count, 1);
    println!(
        "\ndeployed sessionizer over a realistic day: {events:?}\n  -> total={}s ({}h), sessions={}, longest={}s, days={}",
        s.total_seconds,
        s.total_seconds / 3600,
        s.session_count,
        s.longest_seconds,
        s.day_count
    );
}
