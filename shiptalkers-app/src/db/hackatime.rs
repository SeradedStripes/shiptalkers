use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};

#[derive(Debug, Row, Serialize)]
pub struct HackatimeConnectionRow {
    pub slack_id: String,
    pub access_token: String,
    pub last_synced_date: Option<String>,
    pub status: String,
    pub total_minutes: u64,
}

#[derive(Debug, Row, Deserialize)]
pub struct HackatimeConnectionReadRow {
    pub slack_id: String,
    pub access_token: String,
    pub last_synced_date: Option<String>,
    pub status: String,
    pub total_minutes: u64,
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
            status: String::new(),
            total_minutes: 0,
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
        .query(
            "SELECT slack_id, access_token, last_synced_date, status, total_minutes \
             FROM hackatime_connections FINAL",
        )
        .fetch_all()
        .await?;
    Ok(rows)
}

pub async fn get_hackatime_connection(
    client: &Client,
    slack_id: &str,
) -> Result<Option<HackatimeConnectionReadRow>, Box<dyn std::error::Error>> {
    let row: Option<HackatimeConnectionReadRow> = client
        .query(
            "SELECT slack_id, access_token, last_synced_date, status, total_minutes \
             FROM hackatime_connections FINAL WHERE slack_id = ?",
        )
        .bind(slack_id)
        .fetch_optional()
        .await?;
    Ok(row)
}

#[derive(Debug, Row, Serialize)]
pub struct HackatimeSpanRow {
    pub slack_id: String,
    pub start_ts: u64,
    pub duration: u64,
    pub updated: u64,
}

pub async fn insert_hackatime_spans(
    client: &Client,
    rows: &[HackatimeSpanRow],
) -> Result<(), Box<dyn std::error::Error>> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut insert = client.insert::<HackatimeSpanRow>("hackatime_spans").await?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}

/// Total coding seconds across all of a user's spans.
pub async fn get_hackatime_total_seconds(
    client: &Client,
    slack_id: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    let seconds: u64 = client
        .query("SELECT sum(toUInt64(duration)) FROM hackatime_spans FINAL WHERE slack_id = ?")
        .bind(slack_id)
        .fetch_one()
        .await?;
    Ok(seconds)
}

pub async fn get_hackatime_span_count(
    client: &Client,
    slack_id: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    let count: u64 = client
        .query("SELECT count() FROM hackatime_spans FINAL WHERE slack_id = ?")
        .bind(slack_id)
        .fetch_one()
        .await?;
    Ok(count)
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
    // Only rows with a real token count as connected; the sync-state rows kept
    // for public-only / private / no-account users must not hide the connect
    // button on the link page.
    let row: Option<CountRow> = client
        .query(&format!(
            "SELECT count() as count FROM hackatime_connections FINAL \
             WHERE slack_id = '{}' AND access_token != ''",
            slack_id
        ))
        .fetch_optional()
        .await?;
    Ok(row.map(|r| r.count > 0).unwrap_or(false))
}

pub async fn get_coding_user_ids(
    client: &Client,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct IdRow {
        user_id: String,
    }
    let rows: Vec<IdRow> = client
        .query("SELECT user_id FROM users FINAL WHERE is_bot = 0 AND is_deleted = 0")
        .fetch_all()
        .await?;
    Ok(rows.into_iter().map(|r| r.user_id).collect())
}
