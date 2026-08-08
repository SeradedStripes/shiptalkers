use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct SlackMessageRow {
    pub user_id: String,
    pub channel_id: String,
    pub message_ts: u64,
    pub text: String,
    pub thread_ts: Option<String>,
}

/// Converts a Slack timestamp string ("seconds.microseconds") to microseconds.
pub fn slack_ts_to_micros(ts: &str) -> u64 {
    let (secs, frac) = match ts.split_once('.') {
        Some((s, f)) => (s, f),
        None => (ts, "0"),
    };
    let secs: u64 = secs.parse().unwrap_or(0);
    let micros: u64 = frac.parse().unwrap_or(0);
    secs.saturating_mul(1_000_000).saturating_add(micros)
}

/// Formats microseconds as a Slack timestamp string ("seconds.microseconds").
pub fn micros_to_slack_ts(micros: u64) -> String {
    format!("{}.{:06}", micros / 1_000_000, micros % 1_000_000)
}

/// Parses an ISO "YYYY-MM-DD" date string into a `time::Date`.
pub fn parse_date(s: &str) -> Option<time::Date> {
    let mut parts = s.split('-');
    let year: i32 = parts.next()?.parse().ok()?;
    let month: u8 = parts.next()?.parse().ok()?;
    let day: u8 = parts.next()?.parse().ok()?;
    time::Date::from_calendar_date(year, time::Month::try_from(month).ok()?, day).ok()
}

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct SlackChannelRow {
    pub channel_id: String,
    pub name: String,
}

#[derive(Debug, Row, Deserialize)]
pub struct ChannelIdRow {
    pub channel_id: String,
}

pub async fn init_tables(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    let admin_client = client.clone().with_database("");

    admin_client
        .query("CREATE DATABASE IF NOT EXISTS ship_talkers")
        .execute()
        .await?;

    // Migrate existing MergeTree to ReplacingMergeTree if needed
    migrate_slack_messages(client).await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS slack_messages (
                user_id String,
                channel_id String,
                message_ts UInt64,
                text String,
                thread_ts Nullable(String)
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (channel_id, message_ts)",
        )
        .execute()
        .await?;

    // Add thread_ts column if it doesn't exist (migration for existing tables)
    client
        .query("ALTER TABLE slack_messages ADD COLUMN IF NOT EXISTS thread_ts Nullable(String)")
        .execute()
        .await
        .ok();

    // Migrate message_ts from String ("seconds.microseconds") to UInt64 microseconds
    migrate_slack_messages_ts(client).await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS slack_channels (
                channel_id String,
                name String
            ) ENGINE = ReplacingMergeTree()
            ORDER BY channel_id",
        )
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS users (
                user_id String,
                display_name String,
                pfp String DEFAULT '',
                updated UInt64 DEFAULT 0,
                is_bot UInt8 DEFAULT 0,
                is_deleted UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree()
            ORDER BY user_id",
        )
        .execute()
        .await?;

    // Add columns if they don't exist (migrations for existing tables)
    client
        .query("ALTER TABLE users ADD COLUMN IF NOT EXISTS updated UInt64 DEFAULT 0")
        .execute()
        .await
        .ok();
    client
        .query("ALTER TABLE users ADD COLUMN IF NOT EXISTS pfp String DEFAULT ''")
        .execute()
        .await
        .ok();
    client
        .query("ALTER TABLE users ADD COLUMN IF NOT EXISTS is_bot UInt8 DEFAULT 0")
        .execute()
        .await
        .ok();
    client
        .query("ALTER TABLE users ADD COLUMN IF NOT EXISTS is_deleted UInt8 DEFAULT 0")
        .execute()
        .await
        .ok();

    client
        .query(
            "CREATE TABLE IF NOT EXISTS coding_activity (
                user_id String,
                date Date,
                minutes Int64,
                language Nullable(String)
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (user_id, date)",
        )
        .execute()
        .await?;

    // Migrate date from String ("YYYY-MM-DD") to Date
    migrate_coding_activity_date(client).await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS scrape_checkpoints (
                channel_id String,
                fully_scraped UInt8
            ) ENGINE = ReplacingMergeTree()
            ORDER BY channel_id",
        )
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS thread_checkpoints (
                channel_id String,
                thread_ts String,
                fully_scraped UInt8
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (channel_id, thread_ts)",
        )
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS scraped_channels (
                channel_id String,
                scraped_at DateTime('UTC') DEFAULT now()
            ) ENGINE = ReplacingMergeTree()
            ORDER BY channel_id",
        )
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS user_scores (
                user_id String,
                score Int64,
                total_time UInt64,
                messages UInt64,
                sessions UInt64,
                total_chars UInt64,
                longest UInt64,
                days UInt64,
                channels UInt64,
                first_ts UInt64,
                last_ts UInt64,
                active_hour UInt8,
                updated UInt64
            ) ENGINE = ReplacingMergeTree(updated)
            ORDER BY user_id",
        )
        .execute()
        .await?;

    // Migration for existing deployments: the columns below were added later, so
    // they arrive as zeros; backfill_stale_user_scores recomputes any user with
    // longest = 0 (real users always have a session of at least 300s).
    for column in [
        "longest UInt64",
        "days UInt64",
        "channels UInt64",
        "first_ts UInt64",
        "last_ts UInt64",
        "active_hour UInt8",
    ] {
        client
            .query(&format!(
                "ALTER TABLE user_scores ADD COLUMN IF NOT EXISTS {column}"
            ))
            .execute()
            .await
            .ok();
    }

    // Fix column types on tables created before the DDL used unsigned types
    migrate_user_scores_types(client).await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS hackatime_connections (
                slack_id String,
                access_token String,
                last_synced_date Nullable(String),
                connected_at DateTime('UTC') DEFAULT now()
            ) ENGINE = ReplacingMergeTree()
            ORDER BY slack_id",
        )
        .execute()
        .await?;

    // Add last_synced_date column if it doesn't exist (migration for existing tables)
    client
        .query(
            "ALTER TABLE hackatime_connections ADD COLUMN IF NOT EXISTS last_synced_date Nullable(String)",
        )
        .execute()
        .await
        .ok();

    // Secondary copy of slack_messages sorted by user so per-user reads (stats
    // pages and score recompute) only touch that user's granules instead of
    // scanning the whole table. Kept in sync by the materialized view plus a
    // one-time backfill at startup.
    client
        .query(
            "CREATE TABLE IF NOT EXISTS slack_messages_by_user (
                user_id String,
                channel_id String,
                message_ts UInt64,
                text String,
                thread_ts Nullable(String)
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (user_id, message_ts)",
        )
        .execute()
        .await?;

    client
        .query(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS slack_messages_by_user_mv
             TO slack_messages_by_user
             AS SELECT user_id, channel_id, message_ts, text, thread_ts FROM slack_messages",
        )
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS score_meta (
                id UInt8,
                formula String
            ) ENGINE = ReplacingMergeTree()
            ORDER BY id",
        )
        .execute()
        .await?;

    Ok(())
}

pub async fn optimize_slack_messages(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    client
        .query("OPTIMIZE TABLE slack_messages")
        .execute()
        .await?;
    client
        .query("OPTIMIZE TABLE slack_messages_by_user")
        .execute()
        .await?;
    Ok(())
}

/// Copies existing rows from slack_messages into slack_messages_by_user once.
/// The materialized view keeps new inserts in sync, so this only needs to run
/// when the secondary table is empty (first deploy or after a recreate).
pub async fn backfill_slack_messages_by_user(
    client: &Client,
) -> Result<(), Box<dyn std::error::Error>> {
    let count: u64 = client
        .query("SELECT count() FROM slack_messages_by_user")
        .fetch_one()
        .await
        .unwrap_or(0);
    if count > 0 {
        return Ok(());
    }
    tracing::info!("Backfilling slack_messages_by_user from slack_messages...");
    client
        .query(
            "INSERT INTO slack_messages_by_user
             SELECT user_id, channel_id, message_ts, text, thread_ts FROM slack_messages",
        )
        .execute()
        .await?;
    tracing::info!("slack_messages_by_user backfill complete");
    Ok(())
}

/// Recomputes scores only for users who are missing from `user_scores` or have
/// messages newer than their last recompute, so a restart doesn't trigger a full
/// recompute over every user (hours of full-table scans on slow hardware). A full
/// recompute still runs when the Slack Time formula changed since the last one,
/// and once after a migration to fill in columns added to `user_scores` (marked
/// by `longest = 0`, since real users always have a session of at least 300s).
pub async fn backfill_stale_user_scores(
    client: &Client,
    formula: &crate::formula::Formula,
) -> Result<usize, Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct UserRow {
        user_id: String,
    }

    #[derive(Debug, Row, Deserialize)]
    struct ScoreMetaRead {
        formula: String,
    }

    #[derive(Debug, Row, Serialize)]
    struct ScoreMetaRow {
        id: u8,
        formula: String,
    }

    let source = formula.source();
    let stored: Option<ScoreMetaRead> = client
        .query("SELECT formula FROM score_meta FINAL WHERE id = 1")
        .fetch_optional()
        .await?;
    if stored.as_ref().map(|m| m.formula.as_str()) != Some(source) {
        tracing::info!("Slack Time formula changed, recomputing scores for all users");
        let ids: Vec<String> = client
            .query("SELECT DISTINCT user_id FROM slack_messages_by_user")
            .fetch_all::<UserRow>()
            .await?
            .into_iter()
            .map(|r| r.user_id)
            .collect();
        recompute_user_scores(client, &ids, formula).await?;
        let mut insert = client.insert::<ScoreMetaRow>("score_meta").await?;
        insert
            .write(&ScoreMetaRow {
                id: 1,
                formula: source.to_string(),
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
    recompute_user_scores(client, &ids, formula).await?;
    Ok(ids.len())
}

async fn migrate_slack_messages(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct CountRow {
        count: u64,
    }

    // Check if table is already ReplacingMergeTree
    let existing: Option<CountRow> = client
        .query("SELECT count() as count FROM system.tables WHERE database = currentDatabase() AND name = 'slack_messages' AND engine = 'ReplacingMergeTree'")
        .fetch_optional()
        .await
        .unwrap_or(None);

    if existing.map(|r| r.count > 0).unwrap_or(false) {
        return Ok(());
    }

    let pre_count: u64 = client
        .query("SELECT count() FROM slack_messages")
        .fetch_one()
        .await
        .unwrap_or(0);

    if pre_count == 0 {
        return Ok(());
    }

    tracing::info!(
        "Migrating slack_messages to ReplacingMergeTree ({} rows)...",
        pre_count
    );

    client
        .query(
            "CREATE TABLE IF NOT EXISTS slack_messages_new (
            user_id String,
            channel_id String,
            message_ts String,
            text String,
            thread_ts Nullable(String)
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (channel_id, message_ts)",
        )
        .execute()
        .await?;

    client
        .query("INSERT INTO slack_messages_new SELECT * FROM slack_messages")
        .execute()
        .await?;

    client
        .query("DROP TABLE IF EXISTS slack_messages")
        .execute()
        .await?;

    client
        .query("RENAME TABLE slack_messages_new TO slack_messages")
        .execute()
        .await?;

    client
        .query("OPTIMIZE TABLE slack_messages FINAL")
        .execute()
        .await?;

    let dedupd: u64 = client
        .query("SELECT count() FROM slack_messages")
        .fetch_one()
        .await
        .unwrap_or(0);

    tracing::info!(
        "Migration complete: {} rows -> {} rows after dedup",
        pre_count,
        dedupd
    );
    Ok(())
}

async fn migrate_slack_messages_ts(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct TypeRow {
        type_: String,
    }

    let current: Option<TypeRow> = client
        .query(
            "SELECT type AS type_ FROM system.columns
             WHERE database = currentDatabase() AND table = 'slack_messages' AND name = 'message_ts'",
        )
        .fetch_optional()
        .await?;

    if current
        .as_ref()
        .map(|r| r.type_.as_str() == "UInt64")
        .unwrap_or(false)
    {
        return Ok(());
    }
    if current.is_none() {
        return Ok(());
    }

    tracing::info!("Migrating slack_messages.message_ts to UInt64...");

    client
        .query("DROP TABLE IF EXISTS slack_messages_new")
        .execute()
        .await?;
    client
        .query(
            "CREATE TABLE IF NOT EXISTS slack_messages_new (
            user_id String,
            channel_id String,
            message_ts UInt64,
            text String,
            thread_ts Nullable(String)
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (channel_id, message_ts)",
        )
        .execute()
        .await?;

    client
        .query(
            "INSERT INTO slack_messages_new
             SELECT user_id,
                    channel_id,
                    toUInt64OrZero(splitByChar('.', message_ts)[1]) * 1000000
                        + toUInt64OrZero(splitByChar('.', message_ts)[2]) AS message_ts,
                    text,
                    thread_ts
             FROM slack_messages",
        )
        .execute()
        .await?;

    client
        .query("DROP TABLE IF EXISTS slack_messages_old")
        .execute()
        .await?;
    client
        .query("RENAME TABLE slack_messages TO slack_messages_old, slack_messages_new TO slack_messages")
        .execute()
        .await?;
    client
        .query("DROP TABLE IF EXISTS slack_messages_old")
        .execute()
        .await?;
    client
        .query("OPTIMIZE TABLE slack_messages FINAL")
        .execute()
        .await?;

    let count: u64 = client
        .query("SELECT count() FROM slack_messages")
        .fetch_one()
        .await
        .unwrap_or(0);
    tracing::info!(
        "slack_messages.message_ts migration complete ({} rows)",
        count
    );
    Ok(())
}

async fn migrate_coding_activity_date(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct TypeRow {
        type_: String,
    }

    let current: Option<TypeRow> = client
        .query(
            "SELECT type AS type_ FROM system.columns
             WHERE database = currentDatabase() AND table = 'coding_activity' AND name = 'date'",
        )
        .fetch_optional()
        .await?;

    if current
        .as_ref()
        .map(|r| r.type_.as_str() == "Date")
        .unwrap_or(false)
    {
        return Ok(());
    }
    if current.is_none() {
        return Ok(());
    }

    tracing::info!("Migrating coding_activity.date to Date...");

    client
        .query("DROP TABLE IF EXISTS coding_activity_new")
        .execute()
        .await?;
    client
        .query(
            "CREATE TABLE IF NOT EXISTS coding_activity_new (
            user_id String,
            date Date,
            minutes Int64,
            language Nullable(String)
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (user_id, date)",
        )
        .execute()
        .await?;

    client
        .query(
            "INSERT INTO coding_activity_new
             SELECT user_id, toDateOrZero(date), minutes, language FROM coding_activity",
        )
        .execute()
        .await?;

    client
        .query("DROP TABLE IF EXISTS coding_activity_old")
        .execute()
        .await?;
    client
        .query("RENAME TABLE coding_activity TO coding_activity_old, coding_activity_new TO coding_activity")
        .execute()
        .await?;
    client
        .query("DROP TABLE IF EXISTS coding_activity_old")
        .execute()
        .await?;
    client
        .query("OPTIMIZE TABLE coding_activity FINAL")
        .execute()
        .await?;

    let count: u64 = client
        .query("SELECT count() FROM coding_activity")
        .fetch_one()
        .await
        .unwrap_or(0);
    tracing::info!("coding_activity.date migration complete ({} rows)", count);
    Ok(())
}

/// Ensures user_scores columns have the exact types the code reads and writes.
/// Tables created before the DDL used unsigned types (e.g. Int64) need this;
/// ALTER MODIFY COLUMN to the declared type is a no-op on already-correct tables.
async fn migrate_user_scores_types(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    for column in [
        "score Int64",
        "total_time UInt64",
        "messages UInt64",
        "sessions UInt64",
        "total_chars UInt64",
        "longest UInt64",
        "days UInt64",
        "channels UInt64",
        "first_ts UInt64",
        "last_ts UInt64",
        "active_hour UInt8",
        "updated UInt64",
    ] {
        client
            .query(&format!(
                "ALTER TABLE user_scores MODIFY COLUMN {column} SETTINGS mutations_sync = 2"
            ))
            .execute()
            .await?;
    }
    tracing::info!("user_scores column types aligned with the DDL");
    Ok(())
}

pub async fn insert_messages(
    client: &Client,
    messages: &[SlackMessageRow],
) -> Result<u64, Box<dyn std::error::Error>> {
    if messages.is_empty() {
        return Ok(0);
    }
    let count = messages.len() as u64;
    let mut insert = client.insert::<SlackMessageRow>("slack_messages").await?;
    for msg in messages {
        insert.write(msg).await?;
    }
    insert.end().await?;
    Ok(count)
}

/// Recomputes Slack Time scores for the given users (or every user when `user_ids`
/// is empty) and upserts them into `user_scores`. The leaderboard reads top 100
/// from `user_scores`, so scores only need to be refreshed when new messages arrive.
const SCORE_RECOMPUTE_CHUNK: usize = 50;

pub async fn recompute_user_scores(
    client: &Client,
    user_ids: &[String],
    formula: &crate::formula::Formula,
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
        done += recompute_user_scores_chunk(client, chunk, formula).await?;
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
    formula: &crate::formula::Formula,
) -> Result<usize, Box<dyn std::error::Error>> {
    let where_clause = format!("WHERE user_id IN ('{}')", ids.join("', '"));

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
                 SELECT user_id, toInt64(message_ts / 1000000) AS ts
                 FROM slack_messages_by_user
                 {}
                 GROUP BY user_id, ts
             ),
             flagged AS (
                 SELECT user_id, ts,
                     if(ts - lag(ts) OVER (PARTITION BY user_id ORDER BY ts) > 2100, 1, 0) AS boundary
                 FROM msg
             ),
             sess AS (
                 SELECT user_id, ts,
                     sum(boundary) OVER (PARTITION BY user_id ORDER BY ts) AS sid
                 FROM flagged
             ),
             sessions AS (
                 SELECT user_id, sid, min(ts) AS start_ts, max(ts) AS end_ts
                 FROM sess
                 GROUP BY user_id, sid
             )
             SELECT user_id,
                    sum(least(end_ts + 300 - start_ts, 14400)) AS total_time,
                    max(least(end_ts + 300 - start_ts, 14400)) AS longest,
                    count() AS sessions,
                    greatest(dateDiff('day', toDateTime(min(start_ts)), toDateTime(max(start_ts))) + 1, 1) AS days
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
    struct CharRow {
        user_id: String,
        total_chars: u64,
    }

    let chars: Vec<CharRow> = client
        .query(&format!(
            "SELECT user_id, sum(char_length(text)) AS total_chars
             FROM slack_messages_by_user
             {}
             GROUP BY user_id",
            where_clause
        ))
        .fetch_all()
        .await?;

    let char_map: HashMap<String, u64> = chars
        .into_iter()
        .map(|r| (r.user_id, r.total_chars))
        .collect();

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
        total_chars: u64,
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
            let total_chars = char_map.get(&m.user_id).copied().unwrap_or(0);
            let avg_length = if count.messages > 0 {
                total_chars as f64 / count.messages as f64
            } else {
                0.0
            };
            let score = formula
                .eval(&crate::formula::Metrics {
                    message_count: count.messages,
                    session_seconds: m.total_time,
                    session_count: m.sessions,
                    avg_message_length: avg_length,
                    total_chars,
                })
                .max(0.0) as i64;
            let active_hour = hour_map.get(&m.user_id).copied().unwrap_or(0);
            ScoreRow {
                user_id: m.user_id,
                score,
                total_time: m.total_time,
                messages: count.messages,
                sessions: m.sessions,
                total_chars,
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

pub async fn get_known_channel_ids(
    client: &Client,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let rows: Vec<ChannelIdRow> = client
        .query("SELECT channel_id FROM slack_channels FINAL")
        .fetch_all()
        .await?;

    Ok(rows.into_iter().map(|r| r.channel_id).collect())
}

pub async fn insert_new_channels(
    client: &Client,
    channels: &[SlackChannelRow],
) -> Result<u64, Box<dyn std::error::Error>> {
    let known = get_known_channel_ids(client).await?;
    let new_channels: Vec<&SlackChannelRow> = channels
        .iter()
        .filter(|ch| !known.contains(&ch.channel_id))
        .collect();

    if new_channels.is_empty() {
        return Ok(0);
    }

    let count = new_channels.len() as u64;
    let mut insert = client.insert::<SlackChannelRow>("slack_channels").await?;
    for ch in new_channels {
        insert.write(ch).await?;
    }
    insert.end().await?;

    tracing::info!("Inserted {} new channels into ClickHouse", count);
    Ok(count)
}

#[derive(Debug, Row, Serialize)]
pub struct SlackUserRow {
    pub user_id: String,
    pub display_name: String,
    pub pfp: String,
    pub updated: u64,
    pub is_bot: u8,
    pub is_deleted: u8,
}

pub async fn upsert_users(
    client: &Client,
    users: &[SlackUserRow],
) -> Result<(), Box<dyn std::error::Error>> {
    if users.is_empty() {
        return Ok(());
    }
    let mut insert = client.insert::<SlackUserRow>("users").await?;
    for u in users {
        insert.write(u).await?;
    }
    insert.end().await?;
    Ok(())
}

pub async fn get_user_updates(
    client: &Client,
) -> Result<HashMap<String, u64>, Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct UserUpdateRow {
        user_id: String,
        updated: u64,
    }
    let rows: Vec<UserUpdateRow> = client
        .query("SELECT user_id, updated FROM users FINAL")
        .fetch_all()
        .await?;
    Ok(rows.into_iter().map(|r| (r.user_id, r.updated)).collect())
}

pub async fn get_user_ids_without_pfp(
    client: &Client,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct PfpRow {
        user_id: String,
    }
    let rows: Vec<PfpRow> = client
        .query("SELECT user_id FROM users FINAL WHERE pfp = ''")
        .fetch_all()
        .await?;
    Ok(rows.into_iter().map(|r| r.user_id).collect())
}

pub async fn get_max_message_ts(
    client: &Client,
    channel_id: &str,
) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    let row: Option<u64> = client
        .query(&format!(
            "SELECT max(message_ts) FROM slack_messages WHERE channel_id = '{}'",
            channel_id
        ))
        .fetch_optional()
        .await?;

    Ok(row)
}

pub async fn get_scraped_channel_ids(
    client: &Client,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let rows: Vec<ChannelIdRow> = client
        .query("SELECT channel_id FROM scraped_channels")
        .fetch_all()
        .await?;

    Ok(rows.into_iter().map(|r| r.channel_id).collect())
}

pub async fn mark_channels_scraped(
    client: &Client,
    channel_ids: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    if channel_ids.is_empty() {
        return Ok(());
    }

    #[derive(Debug, Row, Serialize)]
    struct ScrapedRow {
        channel_id: String,
    }

    let mut insert = client.insert::<ScrapedRow>("scraped_channels").await?;
    for id in channel_ids {
        insert
            .write(&ScrapedRow {
                channel_id: id.clone(),
            })
            .await?;
    }
    insert.end().await?;

    tracing::info!("Recorded {} channels as scraped", channel_ids.len());
    Ok(())
}

pub async fn mark_channel_scraped(
    client: &Client,
    channel_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Serialize)]
    struct ScrapedRow {
        channel_id: String,
    }

    let mut insert = client.insert::<ScrapedRow>("scraped_channels").await?;
    insert
        .write(&ScrapedRow {
            channel_id: channel_id.to_string(),
        })
        .await?;
    insert.end().await?;
    Ok(())
}

pub async fn backfill_scraped_channels(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    let rows: Vec<ChannelIdRow> = client
        .query(
            "SELECT channel_id FROM (
                SELECT channel_id FROM scrape_checkpoints FINAL WHERE fully_scraped = 1
                UNION DISTINCT
                SELECT DISTINCT channel_id FROM slack_messages
            )
            WHERE channel_id NOT IN (SELECT channel_id FROM scraped_channels)",
        )
        .fetch_all()
        .await?;

    if rows.is_empty() {
        return Ok(());
    }

    let ids: Vec<String> = rows.into_iter().map(|r| r.channel_id).collect();
    tracing::info!(
        "Backfilling {} previously-scraped channels into scraped_channels",
        ids.len()
    );
    mark_channels_scraped(client, &ids).await?;
    Ok(())
}

pub async fn is_fully_scraped(
    client: &Client,
    channel_id: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let count: u64 = client
        .query(&format!(
            "SELECT count() FROM scrape_checkpoints FINAL WHERE channel_id = '{}' AND fully_scraped = 1",
            channel_id
        ))
        .fetch_one()
        .await?;

    Ok(count > 0)
}

pub async fn mark_fully_scraped(
    client: &Client,
    channel_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Serialize)]
    struct CheckpointRow {
        channel_id: String,
        fully_scraped: u8,
    }

    let mut insert = client.insert::<CheckpointRow>("scrape_checkpoints").await?;
    insert
        .write(&CheckpointRow {
            channel_id: channel_id.to_string(),
            fully_scraped: 1,
        })
        .await?;
    insert.end().await?;
    Ok(())
}

pub async fn is_thread_fully_scraped(
    client: &Client,
    channel_id: &str,
    thread_ts: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let count: u64 = client
        .query(&format!(
            "SELECT count() FROM thread_checkpoints FINAL WHERE channel_id = '{}' AND thread_ts = '{}' AND fully_scraped = 1",
            channel_id, thread_ts
        ))
        .fetch_one()
        .await?;
    Ok(count > 0)
}

pub async fn mark_thread_fully_scraped(
    client: &Client,
    channel_id: &str,
    thread_ts: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Serialize)]
    struct ThreadCheckpointRow {
        channel_id: String,
        thread_ts: String,
        fully_scraped: u8,
    }

    let mut insert = client
        .insert::<ThreadCheckpointRow>("thread_checkpoints")
        .await?;
    insert
        .write(&ThreadCheckpointRow {
            channel_id: channel_id.to_string(),
            thread_ts: thread_ts.to_string(),
            fully_scraped: 1,
        })
        .await?;
    insert.end().await?;
    Ok(())
}

pub async fn get_max_thread_reply_ts(
    client: &Client,
    channel_id: &str,
    thread_ts: &str,
) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    let row: Option<u64> = client
        .query(&format!(
            "SELECT max(message_ts) FROM slack_messages WHERE channel_id = '{}' AND thread_ts = '{}'",
            channel_id, thread_ts
        ))
        .fetch_optional()
        .await?;

    Ok(row)
}

#[derive(Debug, Row, Serialize)]
pub struct HackatimeConnectionRow {
    pub slack_id: String,
    pub access_token: String,
    pub last_synced_date: Option<String>,
}

#[derive(Debug, Row, Deserialize)]
pub struct HackatimeConnectionReadRow {
    pub slack_id: String,
    pub access_token: String,
    pub last_synced_date: Option<String>,
}

pub async fn upsert_hackatime_connection(
    client: &Client,
    slack_id: &str,
    access_token: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut insert = client
        .insert::<HackatimeConnectionRow>("hackatime_connections")
        .await?;
    insert
        .write(&HackatimeConnectionRow {
            slack_id: slack_id.to_string(),
            access_token: access_token.to_string(),
            last_synced_date: None,
        })
        .await?;
    insert.end().await?;
    Ok(())
}

pub async fn update_hackatime_connection(
    client: &Client,
    row: &HackatimeConnectionRow,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut insert = client
        .insert::<HackatimeConnectionRow>("hackatime_connections")
        .await?;
    insert.write(row).await?;
    insert.end().await?;
    Ok(())
}

pub async fn get_hackatime_connections(
    client: &Client,
) -> Result<Vec<HackatimeConnectionReadRow>, Box<dyn std::error::Error>> {
    let rows: Vec<HackatimeConnectionReadRow> = client
        .query("SELECT slack_id, access_token, last_synced_date FROM hackatime_connections FINAL")
        .fetch_all()
        .await?;
    Ok(rows)
}

pub async fn delete_hackatime_connection(
    client: &Client,
    slack_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    client
        .query(&format!(
            "ALTER TABLE hackatime_connections DELETE WHERE slack_id = '{}'",
            slack_id
        ))
        .execute()
        .await?;
    Ok(())
}

pub async fn is_hackatime_connected(
    client: &Client,
    slack_id: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct CountRow {
        count: u64,
    }
    let row: Option<CountRow> = client
        .query(&format!(
            "SELECT count() as count FROM hackatime_connections FINAL WHERE slack_id = '{}'",
            slack_id
        ))
        .fetch_optional()
        .await?;
    Ok(row.map(|r| r.count > 0).unwrap_or(false))
}

pub async fn clear_coding_activity_from(
    client: &Client,
    slack_id: &str,
    from_date: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    client
        .query(&format!(
            "ALTER TABLE coding_activity DELETE WHERE user_id = '{}' AND date >= '{}' SETTINGS mutations_sync = 2",
            slack_id, from_date
        ))
        .execute()
        .await?;
    Ok(())
}

pub async fn insert_coding_activity(
    client: &Client,
    rows: &[CodingActivityRow],
) -> Result<(), Box<dyn std::error::Error>> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut insert = client
        .insert::<CodingActivityRow>("coding_activity")
        .await?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}

#[derive(Debug, Row, Serialize)]
pub struct CodingActivityRow {
    pub user_id: String,
    #[serde(with = "clickhouse::serde::time::date")]
    pub date: time::Date,
    pub minutes: i64,
    pub language: Option<String>,
}
