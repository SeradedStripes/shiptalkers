use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub async fn sessionizer_changed(client: &Client) -> Result<bool, Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct ScoreMetaRead {
        formula: String,
    }

    let stored: Option<ScoreMetaRead> = client
        .query("SELECT formula FROM score_meta FINAL WHERE id = 1")
        .fetch_optional()
        .await?;
    Ok(stored.as_ref().map(|m| m.formula.clone()) != Some(sessionizer_fingerprint()))
}

fn sessionizer_fingerprint() -> String {
    format!(
        "sessionize:{}:{}:{}:{}",
        crate::sessionize::SESSION_GAP_BOUNDARY_SECS,
        crate::sessionize::MESSAGE_TYPING_CHARS_PER_SEC,
        crate::sessionize::MESSAGE_READ_OVERHEAD_SECS,
        crate::sessionize::SESSION_MAX_SECS,
    )
}

pub async fn backfill_stale_user_scores(
    client: &Client,
    force_full: bool,
) -> Result<usize, Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct UserRow {
        user_id: String,
    }

    #[derive(Debug, Row, Serialize)]
    struct ScoreMetaRow {
        id: u8,
        formula: String,
    }

    if force_full {
        tracing::info!("Sessionizer changed, recomputing scores for all users");
        let ids: Vec<String> = client
            .query("SELECT DISTINCT user_id FROM slack_messages_by_user")
            .fetch_all::<UserRow>()
            .await?
            .into_iter()
            .map(|r| r.user_id)
            .collect();
        recompute_user_scores(client, &ids).await?;
        let mut insert = client.insert::<ScoreMetaRow>("score_meta").await?;
        insert
            .write(&ScoreMetaRow {
                id: 1,
                formula: sessionizer_fingerprint(),
            })
            .await?;
        insert.end().await?;
        return Ok(ids.len());
    }

    let ids: Vec<String> = client
        .query(
            "SELECT msg.user_id FROM (
                 SELECT user_id, max(message_ts) AS last_ts
                 FROM slack_messages_by_user
                 GROUP BY user_id
             ) msg
             LEFT JOIN (SELECT user_id, updated, longest FROM user_scores FINAL) sc
               ON msg.user_id = sc.user_id
             WHERE sc.user_id IS NULL OR toUInt64(msg.last_ts / 1000000) > sc.updated OR sc.longest = 0",
        )
        .fetch_all::<UserRow>()
        .await?
        .into_iter()
        .map(|r| r.user_id)
        .collect();

    if ids.is_empty() {
        tracing::info!("All users already have fresh Slack Time scores, skipping backfill");
        return Ok(0);
    }
    tracing::info!(
        "Backfilling Slack Time scores for {} stale/missing users",
        ids.len()
    );
    recompute_user_scores(client, &ids).await?;
    Ok(ids.len())
}

pub async fn backfill_stale_channel_scores(
    client: &Client,
    force_full: bool,
) -> Result<usize, Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct ChannelRow {
        channel_id: String,
    }

    if force_full {
        tracing::info!("Sessionizer changed, recomputing channel scores for all channels");
        return recompute_channel_scores(client, &[]).await;
    }

    let ids: Vec<String> = client
        .query(
            "SELECT msg.channel_id FROM (
                 SELECT channel_id, max(message_ts) AS last_ts
                 FROM slack_messages_by_user
                 GROUP BY channel_id
             ) msg
             LEFT JOIN (SELECT channel_id, updated FROM channel_scores FINAL) sc
               ON msg.channel_id = sc.channel_id
             WHERE sc.channel_id IS NULL OR toUInt64(msg.last_ts / 1000000) > sc.updated",
        )
        .fetch_all::<ChannelRow>()
        .await?
        .into_iter()
        .map(|r| r.channel_id)
        .collect();

    if ids.is_empty() {
        return Ok(0);
    }
    tracing::info!(
        "Backfilling Slack Time scores for {} stale/missing channels",
        ids.len()
    );
    recompute_channel_scores(client, &ids).await
}

const SCORE_RECOMPUTE_CHUNK: usize = 50;

pub async fn recompute_user_scores(
    client: &Client,
    user_ids: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let full = user_ids.is_empty();
    let ids: Vec<String> = if full {
        #[derive(Debug, Row, Deserialize)]
        struct UserRow {
            user_id: String,
        }

        let ids = client
            .query("SELECT DISTINCT user_id FROM slack_messages_by_user")
            .fetch_all::<UserRow>()
            .await?
            .into_iter()
            .map(|r| r.user_id)
            .collect::<Vec<String>>();
        tracing::info!("Backfilling Slack Time scores for all {} users", ids.len());
        ids
    } else {
        let mut ids: Vec<String> = user_ids.to_vec();
        ids.sort();
        ids.dedup();
        ids
    };

    let start = std::time::Instant::now();
    let mut done = 0usize;
    for chunk in ids.chunks(SCORE_RECOMPUTE_CHUNK) {
        done += recompute_user_scores_chunk(client, chunk).await?;
        if full {
            tracing::debug!("Backfill progress: {}/{} users recomputed", done, ids.len());
        }
    }
    if done > 0 {
        tracing::info!(
            "Recomputed Slack Time scores for {} users in {:.1}s",
            done,
            start.elapsed().as_secs_f64()
        );
    }
    Ok(())
}

async fn recompute_user_scores_chunk(
    client: &Client,
    ids: &[String],
) -> Result<usize, Box<dyn std::error::Error>> {
    let where_clause = format!("WHERE user_id IN ('{}')", ids.join("', '"));
    let boundary = crate::sessionize::SESSION_GAP_BOUNDARY_SECS;
    let rate = crate::sessionize::MESSAGE_TYPING_CHARS_PER_SEC;
    let overhead = crate::sessionize::MESSAGE_READ_OVERHEAD_SECS;
    let max_secs = crate::sessionize::SESSION_MAX_SECS;

    #[derive(Debug, Row, Deserialize)]
    struct MetricsRow {
        user_id: String,
        total_time: u64,
        longest: u64,
        sessions: u64,
        days: u64,
    }

    let metrics: Vec<MetricsRow> = client
        .query(&format!(
            "WITH
             msg AS (
                 SELECT user_id, toInt64(message_ts / 1000000) AS ts,
                        sum(char_length(text)) AS chars,
                        count() AS msgs
                 FROM slack_messages_by_user
                 {}
                 GROUP BY user_id, ts
             ),
             flagged AS (
                 SELECT user_id, ts, chars, msgs,
                     if(ts - lag(ts) OVER (PARTITION BY user_id ORDER BY ts) > {boundary}, 1, 0) AS boundary
                 FROM msg
             ),
             sess AS (
                 SELECT user_id, ts, chars, msgs,
                     sum(boundary) OVER (PARTITION BY user_id ORDER BY ts) AS sid
                 FROM flagged
             ),
             sessions AS (
                 SELECT user_id, sid, min(ts) AS start_ts, max(ts) AS end_ts,
                        argMin(chars, ts) AS first_chars,
                        argMin(msgs, ts) AS first_msgs
                 FROM sess
                 GROUP BY user_id, sid
             )
             SELECT user_id,
                    sum(toUInt64(least(end_ts - start_ts + (first_chars + {rate} - 1) / {rate} + first_msgs * {overhead}, {max_secs}))) AS total_time,
                    max(toUInt64(least(end_ts - start_ts + (first_chars + {rate} - 1) / {rate} + first_msgs * {overhead}, {max_secs}))) AS longest,
                    count() AS sessions,
                    greatest(toUInt64(dateDiff('day', toDateTime(min(start_ts)), toDateTime(max(start_ts))) + 1), 1) AS days
             FROM sessions
             GROUP BY user_id
             SETTINGS max_bytes_before_external_sort = 268435456",
            where_clause
        ))
        .fetch_all()
        .await?;

    #[derive(Debug, Clone, Row, Deserialize)]
    struct CountRow {
        user_id: String,
        messages: u64,
        channels: u64,
        first_ts: u64,
        last_ts: u64,
    }

    let counts: Vec<CountRow> = client
        .query(&format!(
            "SELECT user_id, count() AS messages,
                    uniqExact(channel_id) AS channels,
                    min(message_ts) AS first_ts,
                    max(message_ts) AS last_ts
             FROM slack_messages_by_user
             {}
             GROUP BY user_id",
            where_clause
        ))
        .fetch_all()
        .await?;

    let count_map: HashMap<String, CountRow> =
        counts.into_iter().map(|r| (r.user_id.clone(), r)).collect();

    #[derive(Debug, Row, Deserialize)]
    struct HourRow {
        user_id: String,
        active_hour: u8,
    }

    let hours: Vec<HourRow> = client
        .query(&format!(
            "SELECT user_id, argMax(hour, cnt) AS active_hour
             FROM (
                 SELECT user_id, toHour(toDateTime(message_ts / 1000000)) AS hour,
                        count() AS cnt
                 FROM slack_messages_by_user
                 {}
                 GROUP BY user_id, hour
             )
             GROUP BY user_id",
            where_clause
        ))
        .fetch_all()
        .await?;

    let hour_map: HashMap<String, u8> = hours
        .into_iter()
        .map(|r| (r.user_id, r.active_hour))
        .collect();

    let updated = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    #[derive(Debug, Row, Serialize)]
    struct ScoreRow {
        user_id: String,
        score: i64,
        total_time: u64,
        messages: u64,
        sessions: u64,
        longest: u64,
        days: u64,
        channels: u64,
        first_ts: u64,
        last_ts: u64,
        active_hour: u8,
        updated: u64,
    }

    let rows: Vec<ScoreRow> = metrics
        .into_iter()
        .map(|m| {
            let count = count_map.get(&m.user_id).cloned().unwrap_or(CountRow {
                user_id: String::new(),
                messages: 0,
                channels: 0,
                first_ts: 0,
                last_ts: 0,
            });
            let active_hour = hour_map.get(&m.user_id).copied().unwrap_or(0);
            ScoreRow {
                user_id: m.user_id,
                score: m.total_time as i64,
                total_time: m.total_time,
                messages: count.messages,
                sessions: m.sessions,
                longest: m.longest,
                days: m.days,
                channels: count.channels,
                first_ts: count.first_ts,
                last_ts: count.last_ts,
                active_hour,
                updated,
            }
        })
        .collect();

    if rows.is_empty() {
        return Ok(0);
    }

    let mut insert = client.insert::<ScoreRow>("user_scores").await?;
    for r in &rows {
        insert.write(r).await?;
    }
    insert.end().await?;
    Ok(rows.len())
}

pub async fn recompute_channel_scores(
    client: &Client,
    channel_ids: &[String],
) -> Result<usize, Box<dyn std::error::Error>> {
    let full = channel_ids.is_empty();
    let ids: Vec<String> = if full {
        #[derive(Debug, Row, Deserialize)]
        struct ChannelRow {
            channel_id: String,
        }

        let ids = client
            .query("SELECT DISTINCT channel_id FROM slack_messages_by_user")
            .fetch_all::<ChannelRow>()
            .await?
            .into_iter()
            .map(|r| r.channel_id)
            .collect::<Vec<String>>();
        tracing::info!(
            "Backfilling Slack Time scores for all {} channels",
            ids.len()
        );
        ids
    } else {
        let mut ids: Vec<String> = channel_ids.to_vec();
        ids.sort();
        ids.dedup();
        ids
    };

    let start = std::time::Instant::now();
    let mut done = 0usize;
    for chunk in ids.chunks(SCORE_RECOMPUTE_CHUNK) {
        done += recompute_channel_scores_chunk(client, chunk).await?;
        if full {
            tracing::debug!(
                "Channel score backfill progress: {}/{} channels",
                done,
                ids.len()
            );
        }
    }
    if done > 0 {
        tracing::info!(
            "Recomputed Slack Time scores for {} channels in {:.1}s",
            done,
            start.elapsed().as_secs_f64()
        );
    }
    Ok(done)
}

async fn recompute_channel_scores_chunk(
    client: &Client,
    ids: &[String],
) -> Result<usize, Box<dyn std::error::Error>> {
    if ids.is_empty() {
        return Ok(0);
    }
    let in_list = format!("'{}'", ids.join("', '"));
    let scope = format!("channel_id IN ({})", in_list);
    let exclude_bots_deleted =
        "user_id NOT IN (SELECT user_id FROM users FINAL WHERE is_bot = 1 OR is_deleted = 1)";
    let boundary = crate::sessionize::SESSION_GAP_BOUNDARY_SECS;
    let rate = crate::sessionize::MESSAGE_TYPING_CHARS_PER_SEC;
    let overhead = crate::sessionize::MESSAGE_READ_OVERHEAD_SECS;
    let max_secs = crate::sessionize::SESSION_MAX_SECS;

    #[derive(Debug, Row, Deserialize)]
    struct SessionRow {
        channel_id: String,
        total_time: u64,
    }

    #[derive(Debug, Row, Deserialize)]
    struct CountRow {
        channel_id: String,
        messages: u64,
    }

    let sessions: Vec<SessionRow> = client
        .query(&format!(
            "WITH
             msg AS (
                 SELECT channel_id, toInt64(message_ts / 1000000) AS ts,
                        sum(char_length(text)) AS chars,
                        count() AS msgs
                 FROM slack_messages_by_user
                 WHERE {scope} AND {exclude_bots_deleted}
                 GROUP BY channel_id, ts
             ),
             flagged AS (
                 SELECT channel_id, ts, chars, msgs,
                        if(ts - lag(ts) OVER (PARTITION BY channel_id ORDER BY ts) > {boundary}, 1, 0) AS boundary
                 FROM msg
             ),
             sess AS (
                 SELECT channel_id, ts, chars, msgs,
                        sum(boundary) OVER (PARTITION BY channel_id ORDER BY ts) AS sid
                 FROM flagged
             ),
             sessions AS (
                 SELECT channel_id, sid, min(ts) AS start_ts, max(ts) AS end_ts,
                        argMin(chars, ts) AS first_chars,
                        argMin(msgs, ts) AS first_msgs
                 FROM sess
                 GROUP BY channel_id, sid
             )
             SELECT channel_id,
                    sum(toUInt64(least(end_ts - start_ts + (first_chars + {rate} - 1) / {rate} + first_msgs * {overhead}, {max_secs}))) AS total_time
             FROM sessions
             GROUP BY channel_id
             SETTINGS max_bytes_before_external_sort = 268435456"
        ))
        .fetch_all()
        .await?;

    let counts: Vec<CountRow> = client
        .query(&format!(
            "SELECT channel_id, count() AS messages
             FROM slack_messages_by_user
             WHERE {scope} AND {exclude_bots_deleted}
             GROUP BY channel_id"
        ))
        .fetch_all()
        .await?;
    let count_map: std::collections::HashMap<String, u64> = counts
        .into_iter()
        .map(|r| (r.channel_id, r.messages))
        .collect();

    let updated = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    #[derive(Debug, Row, Serialize)]
    struct ChannelScoreRow {
        channel_id: String,
        total_time: u64,
        messages: u64,
        updated: u64,
    }

    let rows: Vec<ChannelScoreRow> = sessions
        .into_iter()
        .map(|r| {
            let channel_id = r.channel_id;
            ChannelScoreRow {
                channel_id: channel_id.clone(),
                total_time: r.total_time,
                messages: count_map.get(&channel_id).copied().unwrap_or(0),
                updated,
            }
        })
        .collect();

    if rows.is_empty() {
        return Ok(0);
    }

    let mut insert = client.insert::<ChannelScoreRow>("channel_scores").await?;
    for r in &rows {
        insert.write(r).await?;
    }
    insert.end().await?;
    Ok(rows.len())
}
