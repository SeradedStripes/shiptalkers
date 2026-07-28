use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct SlackMessageRow {
    pub user_id: String,
    pub channel_id: String,
    pub message_ts: String,
    pub text: String,
}

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct SlackChannelRow {
    pub channel_id: String,
    pub name: String,
    pub is_archived: bool,
}

#[derive(Debug, Row, Deserialize)]
pub struct ChannelIdRow {
    pub channel_id: String,
}

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct CodingActivityRow {
    pub user_id: String,
    pub date: String,
    pub minutes: i64,
    pub language: Option<String>,
}

pub async fn init_tables(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    let admin_client = client.clone().with_database("");

    admin_client
        .query(
            "CREATE DATABASE IF NOT EXISTS ship_talkers"
        )
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS slack_messages (
                user_id String,
                channel_id String,
                message_ts String,
                text String
            ) ENGINE = MergeTree()
            ORDER BY (user_id, message_ts)"
        )
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS slack_channels (
                channel_id String,
                name String,
                is_archived Bool
            ) ENGINE = ReplacingMergeTree()
            ORDER BY channel_id"
        )
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS metrics (
                key String,
                value UInt64,
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY key"
        )
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS channel_scrape_state (
                channel_id String,
                last_scraped_ts String,
                message_count UInt64 DEFAULT 0,
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY channel_id"
        )
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS coding_activity (
                user_id String,
                date String,
                minutes Int64,
                language Nullable(String)
            ) ENGINE = MergeTree()
            ORDER BY (user_id, date)"
        )
        .execute()
        .await?;

    Ok(())
}

pub async fn get_max_message_ts(client: &Client, channel_id: &str) -> Result<Option<String>, Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct MaxTsRow {
        max_ts: Option<String>,
    }

    let row: Option<MaxTsRow> = client
        .query(&format!(
            "SELECT max(message_ts) as max_ts FROM slack_messages WHERE channel_id = '{}'",
            channel_id
        ))
        .fetch_optional()
        .await?;

    Ok(row.and_then(|r| r.max_ts))
}

pub async fn insert_messages(client: &Client, messages: &[SlackMessageRow]) -> Result<u64, Box<dyn std::error::Error>> {
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

pub async fn insert_coding_activity(client: &Client, activities: &[CodingActivityRow]) -> Result<(), Box<dyn std::error::Error>> {
    let mut insert = client.insert("coding_activity")?;
    for act in activities {
        insert.write(act).await?;
    }
    insert.end().await?;
    Ok(())
}

pub async fn get_known_channel_ids(client: &Client) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let rows: Vec<ChannelIdRow> = client
        .query("SELECT channel_id FROM slack_channels")
        .fetch_all()
        .await?;

    Ok(rows.into_iter().map(|r| r.channel_id).collect())
}

pub async fn get_active_channel_ids(client: &Client) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let rows: Vec<ChannelIdRow> = client
        .query("SELECT channel_id FROM slack_channels WHERE is_archived = false")
        .fetch_all()
        .await?;

    Ok(rows.into_iter().map(|r| r.channel_id).collect())
}

pub async fn insert_new_channels(client: &Client, channels: &[SlackChannelRow]) -> Result<u64, Box<dyn std::error::Error>> {
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

pub async fn update_channel_archive_status(client: &Client, channel_id: &str, is_archived: bool) -> Result<(), Box<dyn std::error::Error>> {
    client
        .query(&format!(
            "ALTER TABLE slack_channels UPDATE is_archived = {} WHERE channel_id = '{}'",
            is_archived, channel_id
        ))
        .execute()
        .await?;
    Ok(())
}

pub async fn set_metric(client: &Client, key: &str, value: u64) -> Result<(), Box<dyn std::error::Error>> {
    client
        .query(&format!(
            "INSERT INTO metrics (key, value) VALUES ('{}', {})",
            key, value
        ))
        .execute()
        .await?;
    Ok(())
}

pub async fn get_metric(client: &Client, key: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let value: u64 = client
        .query(&format!(
            "SELECT value FROM metrics WHERE key = '{}' LIMIT 1",
            key
        ))
        .fetch_one()
        .await
        .unwrap_or(0);
    Ok(value)
}

pub async fn get_scrape_state(client: &Client, channel_id: &str) -> Result<Option<(String, u64)>, Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct ScrapeStateRow {
        last_scraped_ts: String,
        message_count: u64,
    }

    let row: Option<ScrapeStateRow> = client
        .query(&format!(
            "SELECT last_scraped_ts, message_count FROM channel_scrape_state WHERE channel_id = '{}' LIMIT 1",
            channel_id
        ))
        .fetch_optional()
        .await?;

    Ok(row.map(|r| (r.last_scraped_ts, r.message_count)))
}

pub async fn update_scrape_state(client: &Client, channel_id: &str, last_ts: &str, message_count: u64) -> Result<(), Box<dyn std::error::Error>> {
    client
        .query(&format!(
            "INSERT INTO channel_scrape_state (channel_id, last_scraped_ts, message_count) VALUES ('{}', '{}', {})",
            channel_id, last_ts, message_count
        ))
        .execute()
        .await?;
    Ok(())
}
