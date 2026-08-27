use sqlx::PgPool;

use ship_talkers_lib::db::INSERT_CHUNK;

#[derive(Debug, Clone)]
pub struct HackatimeConnectionRow {
    pub slack_id: String,
    pub access_token: String,
    pub last_synced_date: Option<String>,
    pub status: String,
    pub total_minutes: u64,
}

pub async fn upsert_hackatime_connection(
    pool: &PgPool,
    slack_id: &str,
    access_token: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query(
        "INSERT INTO hackatime_connections (slack_id, access_token, last_synced_date, status, total_minutes)
         VALUES ($1, $2, NULL, '', 0)
         ON CONFLICT (slack_id) DO UPDATE SET access_token = EXCLUDED.access_token, last_synced_date = NULL, status = '', total_minutes = 0",
    )
    .bind(slack_id)
    .bind(access_token)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_hackatime_connection(
    pool: &PgPool,
    row: &HackatimeConnectionRow,
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query(
        "INSERT INTO hackatime_connections (slack_id, access_token, last_synced_date, status, total_minutes)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (slack_id) DO UPDATE SET access_token = EXCLUDED.access_token, last_synced_date = EXCLUDED.last_synced_date, status = EXCLUDED.status, total_minutes = EXCLUDED.total_minutes",
    )
    .bind(&row.slack_id)
    .bind(&row.access_token)
    .bind(&row.last_synced_date)
    .bind(&row.status)
    .bind(row.total_minutes as i64)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_hackatime_connections(
    pool: &PgPool,
) -> Result<Vec<HackatimeConnectionRow>, Box<dyn std::error::Error>> {
    let rows: Vec<(String, String, Option<String>, String, i64)> = sqlx::query_as(
        "SELECT slack_id, access_token, last_synced_date, status, total_minutes FROM hackatime_connections",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(slack_id, access_token, last_synced_date, status, total_minutes)| {
                HackatimeConnectionRow {
                    slack_id,
                    access_token,
                    last_synced_date,
                    status,
                    total_minutes: total_minutes.max(0) as u64,
                }
            },
        )
        .collect())
}

pub async fn get_hackatime_connection(
    pool: &PgPool,
    slack_id: &str,
) -> Result<Option<HackatimeConnectionRow>, Box<dyn std::error::Error>> {
    let row: Option<(String, String, Option<String>, String, i64)> = sqlx::query_as(
        "SELECT slack_id, access_token, last_synced_date, status, total_minutes \
         FROM hackatime_connections WHERE slack_id = $1",
    )
    .bind(slack_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(
        |(slack_id, access_token, last_synced_date, status, total_minutes)| {
            HackatimeConnectionRow {
                slack_id,
                access_token,
                last_synced_date,
                status,
                total_minutes: total_minutes.max(0) as u64,
            }
        },
    ))
}

#[derive(Debug, Clone)]
pub struct HackatimeSpanRow {
    pub slack_id: String,
    pub start_ts: u64,
    pub duration: u64,
    pub updated: u64,
}

pub async fn insert_hackatime_spans(
    pool: &PgPool,
    rows: &[HackatimeSpanRow],
) -> Result<(), Box<dyn std::error::Error>> {
    if rows.is_empty() {
        return Ok(());
    }
    for chunk in rows.chunks(INSERT_CHUNK) {
        let mut sql = String::from(
            "INSERT INTO hackatime_spans (slack_id, start_ts, duration, updated) VALUES ",
        );
        sql.push_str(&super::postgres_db::placeholders(chunk.len(), 4));
        sql.push_str(
            " ON CONFLICT (slack_id, start_ts) DO UPDATE SET duration = EXCLUDED.duration, updated = EXCLUDED.updated",
        );
        let mut q = sqlx::query(&sql);
        for row in chunk {
            q = q
                .bind(&row.slack_id)
                .bind(row.start_ts as i64)
                .bind(row.duration as i64)
                .bind(row.updated as i64);
        }
        q.execute(pool).await?;
    }
    Ok(())
}

/// Total coding seconds across all of a user's spans.
pub async fn get_hackatime_total_seconds(
    pool: &PgPool,
    slack_id: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    let seconds: Option<i64> =
        sqlx::query_scalar("SELECT sum(duration) FROM hackatime_spans WHERE slack_id = $1")
            .bind(slack_id)
            .fetch_one(pool)
            .await?;
    Ok(seconds.unwrap_or(0).max(0) as u64)
}

pub async fn get_hackatime_span_count(
    pool: &PgPool,
    slack_id: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM hackatime_spans WHERE slack_id = $1")
        .bind(slack_id)
        .fetch_one(pool)
        .await?;
    Ok(count.max(0) as u64)
}

pub async fn delete_hackatime_connection(
    pool: &PgPool,
    slack_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query("DELETE FROM hackatime_connections WHERE slack_id = $1")
        .bind(slack_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn is_hackatime_connected(
    pool: &PgPool,
    slack_id: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    // Only rows with a real token count as connected; the sync-state rows kept
    // for public-only / private / no-account users must not hide the connect
    // button on the link page.
    let row: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM hackatime_connections \
         WHERE slack_id = $1 AND access_token != ''",
    )
    .bind(slack_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.is_some())
}

pub async fn get_coding_user_ids(pool: &PgPool) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let rows: Vec<String> =
        sqlx::query_scalar("SELECT user_id FROM users WHERE is_bot = 0 AND is_deleted = 0")
            .fetch_all(pool)
            .await?;
    Ok(rows)
}
