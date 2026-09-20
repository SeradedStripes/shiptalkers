use crate::sqlx;
use crate::sqlx::PgPool;
use std::collections::HashMap;

pub async fn sessionizer_changed(pool: &PgPool) -> Result<bool, Box<dyn std::error::Error>> {
    let stored: Option<String> = sqlx::query_scalar("SELECT formula FROM score_meta WHERE id = 1")
        .fetch_optional(pool)
        .await?;
    Ok(stored != Some(sessionizer_fingerprint()))
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
    pool: &PgPool,
    force_full: bool,
) -> Result<usize, Box<dyn std::error::Error>> {
    if force_full {
        tracing::info!("Sessionizer changed, recomputing scores for all users");
        let ids: Vec<String> = distinct_user_ids(pool).await?;
        recompute_user_scores(pool, &ids).await?;
        mark_sessionizer_current(pool).await?;
        return Ok(ids.len());
    }

    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT u.user_id
         FROM users u
         LEFT JOIN user_scores sc ON sc.user_id = u.user_id
         WHERE EXISTS (
             SELECT 1
             FROM slack_messages m
             JOIN slack_identities i ON i.internal_id = m.identity_id
             WHERE i.ship_talkers_id = u.ship_talkers_id
         )
           AND (sc.user_id IS NULL OR sc.longest = 0 OR EXISTS (
               SELECT 1
               FROM slack_messages m
               JOIN slack_identities i ON i.internal_id = m.identity_id
               WHERE i.ship_talkers_id = u.ship_talkers_id
                 AND m.message_ts > sc.updated * 1000000
           ))",
    )
    .fetch_all(pool)
    .await?;

    if ids.is_empty() {
        tracing::info!("All users already have fresh Slack Time scores, skipping backfill");
        return Ok(0);
    }
    tracing::info!(
        "Backfilling Slack Time scores for {} stale/missing users",
        ids.len()
    );
    recompute_user_scores(pool, &ids).await?;
    Ok(ids.len())
}

pub async fn backfill_stale_channel_scores(
    pool: &PgPool,
    force_full: bool,
) -> Result<usize, Box<dyn std::error::Error>> {
    if force_full {
        tracing::info!("Sessionizer changed, recomputing channel scores for all channels");
        return recompute_channel_scores(pool, &[]).await;
    }

    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT c.channel_id
         FROM slack_channels c
         LEFT JOIN channel_scores sc ON sc.channel_id = c.channel_id
         WHERE EXISTS (
             SELECT 1 FROM slack_messages m
             WHERE m.channel_id = c.internal_id
         )
           AND (sc.channel_id IS NULL OR EXISTS (
               SELECT 1 FROM slack_messages m
               WHERE m.channel_id = c.internal_id
                 AND m.message_ts > sc.updated * 1000000
           ))",
    )
    .fetch_all(pool)
    .await?;

    if ids.is_empty() {
        return Ok(0);
    }
    tracing::info!(
        "Backfilling Slack Time scores for {} stale/missing channels",
        ids.len()
    );
    recompute_channel_scores(pool, &ids).await
}

async fn mark_sessionizer_current(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query(
        "INSERT INTO score_meta (id, formula) VALUES (1, $1)
         ON CONFLICT (id) DO UPDATE SET formula = EXCLUDED.formula",
    )
    .bind(sessionizer_fingerprint())
    .execute(pool)
    .await?;
    Ok(())
}

async fn distinct_user_ids(pool: &PgPool) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let rows: Vec<String> = sqlx::query_scalar("SELECT DISTINCT u.user_id FROM slack_messages m JOIN slack_identities i ON i.internal_id = m.identity_id JOIN users u ON u.ship_talkers_id = i.ship_talkers_id")
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

async fn distinct_channel_ids(pool: &PgPool) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let rows: Vec<String> = sqlx::query_scalar("SELECT DISTINCT c.channel_id FROM slack_messages m JOIN slack_channels c ON c.internal_id = m.channel_id")
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

const SCORE_RECOMPUTE_CHUNK: usize = 50;

pub async fn recompute_user_scores(
    pool: &PgPool,
    user_ids: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let full = user_ids.is_empty();
    let ids: Vec<String> = if full {
        let ids = distinct_user_ids(pool).await?;
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
        done += recompute_user_scores_chunk(pool, chunk).await?;
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
    pool: &PgPool,
    ids: &[String],
) -> Result<usize, Box<dyn std::error::Error>> {
    let boundary = crate::sessionize::SESSION_GAP_BOUNDARY_SECS;
    let rate = crate::sessionize::MESSAGE_TYPING_CHARS_PER_SEC;
    let overhead = crate::sessionize::MESSAGE_READ_OVERHEAD_SECS;
    let max_secs = crate::sessionize::SESSION_MAX_SECS;

    let metrics: Vec<(String, i64, i64, i64, i64)> =
        sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "WITH
         msg AS (
             SELECT u.user_id, m.message_ts / 1000000 AS ts,
                    sum(m.char_count) AS chars,
                    count(*) AS msgs
             FROM slack_messages m
             JOIN slack_identities i ON i.internal_id = m.identity_id
             JOIN users u ON u.ship_talkers_id = i.ship_talkers_id
             WHERE u.user_id = ANY($1)
             GROUP BY u.user_id, ts
         ),
         flagged AS (
             SELECT user_id, ts, chars, msgs,
                 CASE WHEN ts - lag(ts) OVER (PARTITION BY user_id ORDER BY ts) > {boundary} THEN 1 ELSE 0 END AS boundary
             FROM msg
         ),
         sess AS (
             SELECT user_id, ts, chars, msgs,
                 sum(boundary) OVER (PARTITION BY user_id ORDER BY ts) AS sid
             FROM flagged
         ),
         sessions AS (
             SELECT user_id, sid, min(ts) AS start_ts, max(ts) AS end_ts,
                    (array_agg(chars ORDER BY ts))[1] AS first_chars,
                    (array_agg(msgs ORDER BY ts))[1] AS first_msgs
             FROM sess
             GROUP BY user_id, sid
         )
         SELECT user_id,
                sum(least(end_ts - start_ts + (first_chars + {rate} - 1) / {rate} + first_msgs * {overhead}, {max_secs}))::bigint AS total_time,
                max(least(end_ts - start_ts + (first_chars + {rate} - 1) / {rate} + first_msgs * {overhead}, {max_secs}))::bigint AS longest,
                count(*) AS sessions,
                greatest(max(start_ts) / 86400 - min(start_ts) / 86400 + 1, 1) AS days
         FROM sessions
         GROUP BY user_id"
    )))
    .bind(ids)
    .fetch_all(pool)
    .await?;

    let counts: Vec<(String, i64, i64, i64, i64)> = sqlx::query_as(
        "SELECT u.user_id, count(*) AS messages,
                 count(DISTINCT m.channel_id) AS channels,
                 min(m.message_ts) AS first_ts,
                 max(m.message_ts) AS last_ts
          FROM slack_messages m
          JOIN slack_identities i ON i.internal_id = m.identity_id
             JOIN users u ON u.ship_talkers_id = i.ship_talkers_id
          WHERE u.user_id = ANY($1)
          GROUP BY u.user_id",
    )
    .bind(ids)
    .fetch_all(pool)
    .await?;

    struct CountRow {
        messages: u64,
        channels: u64,
        first_ts: u64,
        last_ts: u64,
    }
    let count_map: HashMap<String, CountRow> = counts
        .into_iter()
        .map(|(user_id, messages, channels, first_ts, last_ts)| {
            (
                user_id,
                CountRow {
                    messages: messages.max(0) as u64,
                    channels: channels.max(0) as u64,
                    first_ts: first_ts.max(0) as u64,
                    last_ts: last_ts.max(0) as u64,
                },
            )
        })
        .collect();

    let hours: Vec<(String, i64)> = sqlx::query_as(
        "SELECT user_id, (array_agg(hour ORDER BY cnt DESC))[1] AS active_hour
         FROM (
              SELECT u.user_id, (m.message_ts / 1000000 % 86400) / 3600 AS hour,
                     count(*) AS cnt
              FROM slack_messages m
              JOIN slack_identities i ON i.internal_id = m.identity_id
             JOIN users u ON u.ship_talkers_id = i.ship_talkers_id
              WHERE u.user_id = ANY($1)
              GROUP BY u.user_id, hour
         ) h
         GROUP BY user_id",
    )
    .bind(ids)
    .fetch_all(pool)
    .await?;

    let hour_map: HashMap<String, i16> = hours
        .into_iter()
        .map(|(user_id, hour)| (user_id, hour.clamp(0, 23) as i16))
        .collect();

    let updated = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    #[derive(Debug, Clone)]
    struct ScoreRow {
        user_id: String,
        score: i64,
        total_time: u64,
        sessions: u64,
        longest: u64,
        days: u64,
    }

    let rows: Vec<ScoreRow> = metrics
        .into_iter()
        .map(|(user_id, total_time, longest, sessions, days)| ScoreRow {
            score: total_time,
            total_time: total_time.max(0) as u64,
            sessions: sessions.max(0) as u64,
            longest: longest.max(0) as u64,
            days: days.max(0) as u64,
            user_id,
        })
        .collect();

    if rows.is_empty() {
        return Ok(0);
    }

    let mut sql = String::from(
        "INSERT INTO user_scores (user_id, score, total_time, messages, sessions, longest, days, channels, first_ts, last_ts, active_hour, updated) VALUES ",
    );
    sql.push_str(&crate::db::postgres_db::placeholders(rows.len(), 12));
    sql.push_str(
        " ON CONFLICT (user_id) DO UPDATE SET score = EXCLUDED.score, total_time = EXCLUDED.total_time,
          messages = EXCLUDED.messages, sessions = EXCLUDED.sessions, longest = EXCLUDED.longest,
          days = EXCLUDED.days, channels = EXCLUDED.channels, first_ts = EXCLUDED.first_ts,
          last_ts = EXCLUDED.last_ts, active_hour = EXCLUDED.active_hour, updated = EXCLUDED.updated",
    );
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
    for row in &rows {
        let count = count_map.get(&row.user_id);
        let active_hour = hour_map.get(&row.user_id).copied().unwrap_or(0);
        query = query
            .bind(&row.user_id)
            .bind(row.score)
            .bind(row.total_time as i64)
            .bind(count.map(|c| c.messages as i64).unwrap_or(0))
            .bind(row.sessions as i64)
            .bind(row.longest as i64)
            .bind(row.days as i64)
            .bind(count.map(|c| c.channels as i64).unwrap_or(0))
            .bind(count.map(|c| c.first_ts as i64).unwrap_or(0))
            .bind(count.map(|c| c.last_ts as i64).unwrap_or(0))
            .bind(active_hour)
            .bind(updated as i64);
    }
    query.execute(pool).await?;
    Ok(rows.len())
}

pub async fn recompute_channel_scores(
    pool: &PgPool,
    channel_ids: &[String],
) -> Result<usize, Box<dyn std::error::Error>> {
    let full = channel_ids.is_empty();
    let ids: Vec<String> = if full {
        let ids = distinct_channel_ids(pool).await?;
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
        done += recompute_channel_scores_chunk(pool, chunk).await?;
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
    pool: &PgPool,
    ids: &[String],
) -> Result<usize, Box<dyn std::error::Error>> {
    if ids.is_empty() {
        return Ok(0);
    }
    let exclude_bots_deleted = "NOT EXISTS (SELECT 1 FROM slack_identities bi JOIN users bu ON bu.ship_talkers_id = bi.ship_talkers_id WHERE bi.internal_id = m.identity_id AND (bu.is_bot = 1 OR bu.is_deleted = 1))";
    let boundary = crate::sessionize::SESSION_GAP_BOUNDARY_SECS;
    let rate = crate::sessionize::MESSAGE_TYPING_CHARS_PER_SEC;
    let overhead = crate::sessionize::MESSAGE_READ_OVERHEAD_SECS;
    let max_secs = crate::sessionize::SESSION_MAX_SECS;

    let sessions: Vec<(String, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "WITH
         msg AS (
             SELECT m.channel_id, m.message_ts / 1000000 AS ts,
                    sum(m.char_count) AS chars,
                    count(*) AS msgs
             FROM slack_messages m
             JOIN slack_channels c ON c.internal_id = m.channel_id
             WHERE c.channel_id = ANY($1) AND {exclude_bots_deleted}
             GROUP BY m.channel_id, ts
         ),
         flagged AS (
             SELECT channel_id, ts, chars, msgs,
                    CASE WHEN ts - lag(ts) OVER (PARTITION BY channel_id ORDER BY ts) > {boundary} THEN 1 ELSE 0 END AS boundary
             FROM msg
         ),
         sess AS (
             SELECT channel_id, ts, chars, msgs,
                    sum(boundary) OVER (PARTITION BY channel_id ORDER BY ts) AS sid
             FROM flagged
         ),
         sessions AS (
             SELECT channel_id, sid, min(ts) AS start_ts, max(ts) AS end_ts,
                    (array_agg(chars ORDER BY ts))[1] AS first_chars,
                    (array_agg(msgs ORDER BY ts))[1] AS first_msgs
             FROM sess
             GROUP BY channel_id, sid
         )
         SELECT channel_id,
                sum(least(end_ts - start_ts + (first_chars + {rate} - 1) / {rate} + first_msgs * {overhead}, {max_secs}))::bigint AS total_time
         FROM sessions
         GROUP BY channel_id"
    )))
    .bind(ids)
    .fetch_all(pool)
    .await?;

    let counts: Vec<(String, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT c.channel_id, count(*) AS messages
          FROM slack_messages m
          JOIN slack_channels c ON c.internal_id = m.channel_id
          WHERE c.channel_id = ANY($1) AND {exclude_bots_deleted}
          GROUP BY c.channel_id"
    )))
    .bind(ids)
    .fetch_all(pool)
    .await?;
    let count_map: HashMap<String, u64> = counts
        .into_iter()
        .map(|(channel_id, messages)| (channel_id, messages.max(0) as u64))
        .collect();

    let updated = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let recomputed = sessions.len();
    let mut sql = String::from(
        "INSERT INTO channel_scores (channel_id, total_time, messages, updated) VALUES ",
    );
    sql.push_str(&crate::db::postgres_db::placeholders(sessions.len(), 4));
    sql.push_str(
        " ON CONFLICT (channel_id) DO UPDATE SET total_time = EXCLUDED.total_time,
          messages = EXCLUDED.messages, updated = EXCLUDED.updated",
    );
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
    for (channel_id, total_time) in &sessions {
        let messages = count_map.get(channel_id).copied().unwrap_or(0);
        query = query
            .bind(channel_id)
            .bind(*total_time)
            .bind(messages as i64)
            .bind(updated as i64);
    }
    if recomputed > 0 {
        query.execute(pool).await?;
    }
    Ok(recomputed)
}
