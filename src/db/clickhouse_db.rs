use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct SlackMessageRow {
    pub user_id: String,
    pub channel_id: String,
    pub message_ts: String,
    pub text: String,
    pub thread_ts: Option<String>,
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
                message_ts String,
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
                updated UInt64 DEFAULT 0
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
        .query(
            "CREATE TABLE IF NOT EXISTS coding_activity (
                user_id String,
                date String,
                minutes Int64,
                language Nullable(String)
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (user_id, date)",
        )
        .execute()
        .await?;

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
                updated UInt64
            ) ENGINE = ReplacingMergeTree(updated)
            ORDER BY user_id",
        )
        .execute()
        .await?;

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

    Ok(())
}

pub async fn optimize_slack_messages(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    client
        .query("OPTIMIZE TABLE slack_messages")
        .execute()
        .await?;
    Ok(())
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

pub async fn insert_messages(
    client: &Client,
    messages: &[SlackMessageRow],
) -> Result<u64, Box<dyn std::error::Error>> {
    if messages.is_empty() {
        return Ok(0);
    }
    let count = messages.len() as u64;
    let mut insert = client.insert("slack_messages")?;
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
            .query("SELECT DISTINCT user_id FROM slack_messages")
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
        messages: u64,
        sessions: u64,
    }

    let metrics: Vec<MetricsRow> = client
        .query(&format!(
            "WITH
             msg AS (
                 SELECT user_id, toInt64(splitByChar('.', message_ts)[1]) AS ts
                 FROM slack_messages
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
                 SELECT user_id, sid, min(ts) AS start_ts, max(ts) AS end_ts, count() AS msg_count
                 FROM sess
                 GROUP BY user_id, sid
             )
             SELECT user_id,
                    sum(least(end_ts + 300 - start_ts, 14400)) AS total_time,
                    sum(msg_count) AS messages,
                    count() AS sessions
             FROM sessions
             GROUP BY user_id
             SETTINGS max_bytes_before_external_sort = 268435456",
            where_clause
        ))
        .fetch_all()
        .await?;

    #[derive(Debug, Row, Deserialize)]
    struct CharRow {
        user_id: String,
        total_chars: u64,
    }

    let chars: Vec<CharRow> = client
        .query(&format!(
            "SELECT user_id, sum(char_length(text)) AS total_chars
             FROM slack_messages
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
        updated: u64,
    }

    let rows: Vec<ScoreRow> = metrics
        .into_iter()
        .map(|m| {
            let total_chars = char_map.get(&m.user_id).copied().unwrap_or(0);
            let avg_length = if m.messages > 0 {
                total_chars as f64 / m.messages as f64
            } else {
                0.0
            };
            let score = formula
                .eval(&crate::formula::Metrics {
                    message_count: m.messages,
                    session_seconds: m.total_time,
                    session_count: m.sessions,
                    avg_message_length: avg_length,
                    total_chars,
                })
                .max(0.0) as i64;
            ScoreRow {
                user_id: m.user_id,
                score,
                total_time: m.total_time,
                messages: m.messages,
                sessions: m.sessions,
                total_chars,
                updated,
            }
        })
        .collect();

    if rows.is_empty() {
        return Ok(0);
    }

    let mut insert = client.insert("user_scores")?;
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
    let mut insert = client.insert("slack_channels")?;
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
}

pub async fn upsert_users(
    client: &Client,
    users: &[SlackUserRow],
) -> Result<(), Box<dyn std::error::Error>> {
    if users.is_empty() {
        return Ok(());
    }
    let mut insert = client.insert("users")?;
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
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct MaxTsRow {
        max_ts: String,
    }

    let row: Option<MaxTsRow> = client
        .query(&format!(
            "SELECT max(message_ts) as max_ts FROM slack_messages WHERE channel_id = '{}'",
            channel_id
        ))
        .fetch_optional()
        .await?;

    Ok(row.map(|r| r.max_ts))
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

    let mut insert = client.insert("scraped_channels")?;
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

    let mut insert = client.insert("scraped_channels")?;
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

    let mut insert = client.insert("scrape_checkpoints")?;
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

    let mut insert = client.insert("thread_checkpoints")?;
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
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct MaxTsRow {
        max_ts: String,
    }

    let row: Option<MaxTsRow> = client
        .query(&format!(
            "SELECT max(message_ts) as max_ts FROM slack_messages WHERE channel_id = '{}' AND thread_ts = '{}'",
            channel_id, thread_ts
        ))
        .fetch_optional()
        .await?;

    Ok(row.map(|r| r.max_ts))
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
    let mut insert = client.insert("hackatime_connections")?;
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
    let mut insert = client.insert("hackatime_connections")?;
    insert.write(row).await?;
    insert.end().await?;
    Ok(())
}

pub async fn get_hackatime_connections(
    client: &Client,
) -> Result<Vec<HackatimeConnectionReadRow>, Box<dyn std::error::Error>> {
    let rows: Vec<HackatimeConnectionReadRow> = client
        .query("SELECT slack_id, access_token FROM hackatime_connections FINAL")
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
    let mut insert = client.insert("coding_activity")?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}

#[derive(Debug, Row, Serialize)]
pub struct CodingActivityRow {
    pub user_id: String,
    pub date: String,
    pub minutes: i64,
    pub language: Option<String>,
}
