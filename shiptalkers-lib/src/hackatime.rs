use serde::Deserialize;
use sqlx::PgPool;

use crate::db::{INSERT_CHUNK, placeholders};

const HACKATIME_ME_URL: &str = "https://hackatime.hackclub.com/api/v1/authenticated/me";
const HACKATIME_USER_STATS_URL: &str = "https://hackatime.hackclub.com/api/v1/users";

#[derive(Deserialize)]
struct HackatimeMeResponse {
    slack_id: Option<String>,
}

#[derive(Deserialize)]
struct SpansResponse {
    spans: Vec<CodingSpan>,
}

#[derive(Deserialize)]
pub struct CodingSpan {
    pub start_time: f64,
    pub end_time: f64,
    pub duration: f64,
}

/// First tuple element is the HTTP status when the failure was an HTTP error, None for transport or parse failures.
pub async fn fetch_hackatime_me(
    client: &reqwest::Client,
    access_token: &str,
) -> Result<Option<String>, (Option<u16>, String)> {
    let response = client
        .get(HACKATIME_ME_URL)
        .bearer_auth(access_token)
        .send()
        .await
        .map_err(|e| (None, e.to_string()))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .unwrap_or_else(|e| format!("(no body: {e})"));
    if !status.is_success() {
        return Err((Some(status.as_u16()), body));
    }
    let me: HackatimeMeResponse = serde_json::from_str(&body).map_err(|e| {
        (
            None,
            format!("me endpoint returned bad JSON ({body:?}): {e}"),
        )
    })?;
    Ok(me.slack_id)
}

/// Fetches coding spans for a Slack user from hackatime over the
/// `[start_date, end_date)` window. The endpoint is keyed by Slack UID;
/// One request covers the whole window, so a full-history backfill is a single
/// call. A 403 means public stats are disabled (only reachable on the
/// unauthenticated path) and a 404 means no hackatime account exists for this Slack UID
pub async fn fetch_coding_spans(
    client: &reqwest::Client,
    slack_uid: &str,
    token: Option<&str>,
    start_date: &str,
    end_date: &str,
) -> Result<Vec<CodingSpan>, (Option<u16>, String)> {
    let mut request = client
        .get(format!(
            "{HACKATIME_USER_STATS_URL}/{slack_uid}/heartbeats/spans"
        ))
        .query(&[("start_date", start_date), ("end_date", end_date)]);
    if let Some(tok) = token {
        request = request.bearer_auth(tok);
    }
    let response = request.send().await.map_err(|e| (None, e.to_string()))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .unwrap_or_else(|e| format!("(no body: {e})"));
    if !status.is_success() {
        return Err((Some(status.as_u16()), body));
    }
    let spans: SpansResponse =
        serde_json::from_str(&body).map_err(|e| (None, format!("bad JSON ({body:?}): {e}")))?;
    Ok(spans.spans)
}

/// Seconds of a coding span (`start_ts` unix seconds + `duration` seconds) that fall inside the range `[range_start, range_end)` (unix seconds; `None` means unbounded).
pub fn span_overlap_seconds(
    start_ts: u64,
    duration: u64,
    range_start: Option<i64>,
    range_end: Option<i64>,
) -> u64 {
    let start = start_ts;
    let end = start + duration;
    let start = match range_start {
        Some(rs) if end > rs as u64 => start.max(rs as u64),
        Some(_) => return 0,
        None => start,
    };
    let end = match range_end {
        Some(re) if start < re as u64 => end.min(re as u64),
        Some(_) => return 0,
        None => end,
    };
    end - start
}

pub fn today_utc() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = secs / 86400;
    let secs_of_day = secs % 86400;
    let (mut year, month, day) = civil_from_days(days as i64);
    let _ = secs_of_day;
    let _ = &mut year;
    format!("{year:04}-{month:02}-{day:02}")
}

/// UTC date `days` days after today, e.g. the end boundary for an all-time
/// hackatime total: tomorrow at midnight includes everything up to now.
pub fn days_from_now(days: i64) -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let (year, month, day) = civil_from_days((secs / 86400) as i64 + days);
    format!("{year:04}-{month:02}-{day:02}")
}

/// UTC date `days` days before today, e.g. the no-account retry cutoff.
pub fn date_days_ago(days: u64) -> String {
    days_from_now(-(days as i64))
}

/// UTC date `days` days after the given `YYYY-MM-DD` date, e.g. the start of an
/// incremental hackatime window one day before the last sync.
pub fn date_plus_days(date: &str, days: i64) -> Option<String> {
    let (y, m, d) = parse_iso_date(date)?;
    let day = days_from_civil(y, m, d) + days;
    let (y, m, d) = civil_from_days(day);
    Some(format!("{y:04}-{m:02}-{d:02}"))
}

fn parse_iso_date(date: &str) -> Option<(i64, i64, i64)> {
    let mut parts = date.split('-');
    let y = parts.next()?.parse().ok()?;
    let m = parts.next()?.parse().ok()?;
    let d = parts.next()?.parse().ok()?;
    if parts.next().is_some() || !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    Some((y, m, d))
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

pub fn civil_from_days(z: i64) -> (u32, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    ((if m <= 2 { y + 1 } else { y }) as u32, m, d)
}

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
        sql.push_str(&placeholders(chunk.len(), 4));
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
        sqlx::query_scalar("SELECT sum(duration)::bigint FROM hackatime_spans WHERE slack_id = $1")
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
    // Only rows with a real token count as connected; the sync-state rows kept for public-only / private / no-account users must not hide the connect
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
