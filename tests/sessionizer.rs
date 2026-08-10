use ship_talkers::formula::sessionize;

#[test]
fn empty_input_gives_zeros() {
    let s = sessionize(&[]);
    assert_eq!(s.total_seconds, 0);
    assert_eq!(s.longest_seconds, 0);
    assert_eq!(s.session_count, 0);
    assert_eq!(s.day_count, 0);
}

#[test]
fn lone_message_earns_grace_only() {
    let s = sessionize(&[1000]);
    assert_eq!(s.session_count, 1);
    assert_eq!(s.total_seconds, 300);
    assert_eq!(s.longest_seconds, 300);
    assert_eq!(s.day_count, 1);
}

#[test]
fn close_messages_share_one_session() {
    let s = sessionize(&[1000, 1100]);
    assert_eq!(s.session_count, 1);
    assert_eq!(s.total_seconds, 400);
    assert_eq!(s.longest_seconds, 400);
    assert_eq!(s.day_count, 1);
}

#[test]
fn boundary_gap_keeps_one_session() {
    let s = sessionize(&[1000, 3100]);
    assert_eq!(s.session_count, 1);
    assert_eq!(s.total_seconds, 2400);
}

#[test]
fn gap_over_boundary_splits_sessions() {
    let s = sessionize(&[1000, 6000]);
    assert_eq!(s.session_count, 2);
    assert_eq!(s.total_seconds, 600);
    assert_eq!(s.longest_seconds, 300);
}

#[test]
fn long_session_caps_at_four_hours() {
    // 8 gaps of 2000s each stay under the boundary but span well past 4h.
    let s = sessionize(&[1000, 3000, 5000, 7000, 9000, 11000, 13000, 15000, 17000]);
    assert_eq!(s.session_count, 1);
    assert_eq!(s.total_seconds, 14400);
    assert_eq!(s.longest_seconds, 14400);
}

#[test]
fn same_second_messages_dedup() {
    let s = sessionize(&[1000, 1000, 1000]);
    assert_eq!(s.session_count, 1);
    assert_eq!(s.total_seconds, 300);
}

#[test]
fn unsorted_input_is_sorted() {
    let s = sessionize(&[1100, 1000]);
    assert_eq!(s.session_count, 1);
    assert_eq!(s.total_seconds, 400);
}

#[test]
fn days_count_spans_session_starts() {
    let day = 86400u64;
    let s = sessionize(&[day, day + 100, 3 * day, 3 * day + 100]);
    assert_eq!(s.session_count, 2);
    assert_eq!(s.day_count, 3);
}

#[test]
fn mixed_timeline_accumulates() {
    let s = sessionize(&[0, 60, 120, 5000, 5100]);
    assert_eq!(s.session_count, 2);
    assert_eq!(s.total_seconds, 820);
    assert_eq!(s.longest_seconds, 420);
    assert_eq!(s.day_count, 1);
}
