//! Slack Time sessionizer for Top Talkers ranking, the per-user Slack Time report,
//! and the Slack stats bot card. Slack Time is the sessionized active time
//! (`user_scores.total_time`): the sessionizer credits typing time (each session's
//! first message: `chars / 5` plus a 10s reading overhead) and every in-session
//! gap, so edit the constants below to change how Slack Time is calculated
//! everywhere.

/// Sessionizer parameters shared by the ClickHouse queries and the Rust reference
/// in `sessionize`. Each SQL site injects these via named format args, so changing
/// a value here updates production queries and the reference at once. The tests in
/// `tests/sessionizer.rs` pin the exact semantics.
///
/// `SESSION_GAP_BOUNDARY_SECS` is the longest gap between consecutive messages that
/// still belongs to the same session; `MESSAGE_TYPING_CHARS_PER_SEC` and
/// `MESSAGE_READ_OVERHEAD_SECS` estimate how long the first message of a session
/// took to produce; `SESSION_MAX_SECS` caps a single session's credit.
pub const SESSION_GAP_BOUNDARY_SECS: u64 = 2100;
pub const MESSAGE_TYPING_CHARS_PER_SEC: u64 = 5;
pub const MESSAGE_READ_OVERHEAD_SECS: u64 = 10;
pub const SESSION_MAX_SECS: u64 = 43200;

/// Per-user metrics produced by `sessionize`, mirroring the sessionizer columns
/// that `user_scores` stores.
pub struct SessionStats {
    pub total_seconds: u64,
    pub longest_seconds: u64,
    pub session_count: u64,
    pub day_count: u64,
}

/// Reference implementation of the Slack Time sessionizer. It matches the `WITH`
/// sessionizer blocks in `recompute_user_scores_chunk`, `recompute_channel_scores_chunk`,
/// and `query_slack_seconds` exactly, so the tests here double as the spec for the SQL.
///
/// Input is `(timestamp_seconds, chars)` per message. Messages in the same second
/// merge into one event (the SQL `GROUP BY` does the same). A gap longer than
/// `SESSION_GAP_BOUNDARY_SECS` between consecutive events starts a new session.
/// A session spans from its first event's timestamp to its last, and its first
/// message's estimated production time (`chars / MESSAGE_TYPING_CHARS_PER_SEC` plus
/// `MESSAGE_READ_OVERHEAD_SECS`) is added on top, since the user was typing before
/// that first send. The credit is `min(end - start + first_duration, SESSION_MAX_SECS)`.
/// `day_count` is the number of calendar days from the first session's start to the
/// last session's start.
pub fn sessionize(events: &[(u64, u64)]) -> SessionStats {
    let mut events = events.to_vec();
    events.sort_unstable();

    let mut merged: Vec<(u64, u64, u64)> = Vec::new();
    for &(t, chars) in &events {
        match merged.last_mut() {
            Some((last_ts, last_chars, last_msgs)) if *last_ts == t => {
                *last_chars += chars;
                *last_msgs += 1;
            }
            _ => merged.push((t, chars, 1)),
        }
    }

    let mut sessions: Vec<(u64, u64, u64, u64)> = Vec::new();
    let mut start = None;
    let mut prev = 0;
    for &(t, chars, msgs) in &merged {
        match start {
            None => start = Some((t, chars, msgs)),
            Some((s, first_chars, first_msgs)) => {
                if t - prev > SESSION_GAP_BOUNDARY_SECS {
                    sessions.push((s, prev, first_chars, first_msgs));
                    start = Some((t, chars, msgs));
                }
            }
        }
        prev = t;
    }
    if let Some((s, first_chars, first_msgs)) = start {
        sessions.push((s, prev, first_chars, first_msgs));
    }

    let mut total_seconds = 0u64;
    let mut longest_seconds = 0u64;
    let mut min_start = u64::MAX;
    let mut max_start = 0u64;
    for &(s, e, first_chars, first_msgs) in &sessions {
        let first_duration = first_chars.div_ceil(MESSAGE_TYPING_CHARS_PER_SEC)
            + first_msgs * MESSAGE_READ_OVERHEAD_SECS;
        let secs = (e - s + first_duration).min(SESSION_MAX_SECS);
        total_seconds += secs;
        longest_seconds = longest_seconds.max(secs);
        min_start = min_start.min(s);
        max_start = max_start.max(s);
    }

    let day_count = if sessions.is_empty() {
        0
    } else {
        (max_start / 86400).saturating_sub(min_start / 86400) + 1
    };

    SessionStats {
        total_seconds,
        longest_seconds,
        session_count: sessions.len() as u64,
        day_count,
    }
}
