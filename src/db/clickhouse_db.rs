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
pub struct CodingActivityRow {
    pub user_id: String,
    pub date: String,
    pub minutes: i64,
    pub language: Option<String>,
}

pub async fn init_tables(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
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

pub async fn insert_messages(client: &Client, messages: &[SlackMessageRow]) -> Result<(), Box<dyn std::error::Error>> {
    let mut insert = client.insert("slack_messages")?;
    for msg in messages {
        insert.write(msg).await?;
    }
    insert.end().await?;
    Ok(())
}

pub async fn insert_coding_activity(client: &Client, activities: &[CodingActivityRow]) -> Result<(), Box<dyn std::error::Error>> {
    let mut insert = client.insert("coding_activity")?;
    for act in activities {
        insert.write(act).await?;
    }
    insert.end().await?;
    Ok(())
}
