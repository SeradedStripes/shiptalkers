use crate::sqlx;
use crate::sqlx::PgPool;
use crate::sqlx::Row;
use std::collections::HashMap;

pub use ship_talkers_lib::db::{
    INSERT_CHUNK, SlackChannelRow, SlackUserRow, connect, insert_new_channels_rows, migrate,
    placeholders, upsert_users,
};

#[derive(Clone)]
pub struct SlackOAuthToken {
    pub slack_id: String,
    pub access_token: String,
}

pub async fn get_slack_oauth_tokens(pool: &PgPool) -> Result<Vec<SlackOAuthToken>, sqlx::Error> {
    sqlx::query_as::<_, (String, String)>(
        "SELECT slack_id, access_token FROM slack_oauth_tokens
         WHERE disabled_at IS NULL ORDER BY slack_id",
    )
    .fetch_all(pool)
    .await
    .map(|rows| {
        rows.into_iter()
            .map(|(slack_id, access_token)| SlackOAuthToken {
                slack_id,
                access_token,
            })
            .collect()
    })
}

pub async fn replace_private_channel_access(
    pool: &PgPool,
    slack_id: &str,
    channel_ids: &[String],
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM slack_oauth_channel_access WHERE slack_id = $1")
        .bind(slack_id)
        .execute(&mut *tx)
        .await?;
    for channel_id in channel_ids {
        sqlx::query(
            "INSERT INTO slack_oauth_channel_access (slack_id, channel_id)
             VALUES ($1, $2) ON CONFLICT (slack_id, channel_id) DO UPDATE SET refreshed_at = now()",
        )
        .bind(slack_id)
        .bind(channel_id)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await
}

pub async fn get_private_channel_access(
    pool: &PgPool,
) -> Result<Vec<(String, String)>, sqlx::Error> {
    sqlx::query_as(
        "SELECT slack_id, channel_id FROM slack_oauth_channel_access ORDER BY channel_id, slack_id",
    )
    .fetch_all(pool)
    .await
}

pub async fn get_private_channel_ids(pool: &PgPool) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar("SELECT channel_id FROM slack_channels WHERE is_private = 1")
        .fetch_all(pool)
        .await
}

/// Reconciles the maintained `message_count` with the real row count.
pub async fn seed_message_count(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(total) =
        sqlx::query_scalar::<_, i64>("SELECT total FROM message_count WHERE id = 1")
            .fetch_optional(pool)
            .await?
    {
        tracing::debug!("Using maintained message count of {}", total.max(0));
        return Ok(());
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM slack_messages")
        .fetch_one(pool)
        .await?;
    sqlx::query(
        "INSERT INTO message_count (id, total) VALUES (1, $1)
         ON CONFLICT (id) DO UPDATE SET total = EXCLUDED.total",
    )
    .bind(count.max(0))
    .execute(pool)
    .await?;
    tracing::info!("Reconciled message_count to {} messages", count.max(0));
    Ok(())
}

#[derive(Debug, Clone)]
pub struct SlackMessageRow {
    pub user_id: String,
    pub channel_id: String,
    pub message_ts: u64,
    pub char_count: i32,
    pub thread_ts: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct SlackReactionRow {
    pub channel_id: String,
    pub message_ts: u64,
    pub emoji: String,
    pub user_id: String,
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

static IDENTITY_CACHE: std::sync::OnceLock<std::sync::Mutex<HashMap<String, i32>>> =
    std::sync::OnceLock::new();
static CHANNEL_CACHE: std::sync::OnceLock<std::sync::Mutex<HashMap<String, i32>>> =
    std::sync::OnceLock::new();
pub async fn load_locator_caches(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let identities: Vec<(String, i32)> =
        sqlx::query_as("SELECT ship_talkers_id, internal_id FROM slack_identities")
            .fetch_all(pool)
            .await?;
    let channels: Vec<(String, i32)> =
        sqlx::query_as("SELECT ship_talkers_id, internal_id FROM slack_channels")
            .fetch_all(pool)
            .await?;
    IDENTITY_CACHE
        .get_or_init(Default::default)
        .lock()
        .map_err(|_| "locator cache poisoned")?
        .extend(identities);
    CHANNEL_CACHE
        .get_or_init(Default::default)
        .lock()
        .map_err(|_| "locator cache poisoned")?
        .extend(channels);
    Ok(())
}

async fn locator_id(
    pool: &PgPool,
    raw: &str,
    channel: bool,
) -> Result<i32, Box<dyn std::error::Error>> {
    let anonymous = ship_talkers_lib::base36::encode(raw.as_bytes());
    let cache = if channel {
        CHANNEL_CACHE.get_or_init(Default::default)
    } else {
        IDENTITY_CACHE.get_or_init(Default::default)
    };
    if let Some(id) = cache
        .lock()
        .map_err(|_| "locator cache poisoned")?
        .get(&anonymous)
        .copied()
    {
        return Ok(id);
    }
    let id = if channel {
        sqlx::query_scalar::<_, i32>("INSERT INTO slack_channels (ship_talkers_id, channel_id) VALUES ($1, $2) ON CONFLICT (ship_talkers_id) DO UPDATE SET ship_talkers_id = EXCLUDED.ship_talkers_id RETURNING internal_id")
            .bind(&anonymous).bind(raw).fetch_one(pool).await?
    } else {
        sqlx::query_scalar::<_, i32>("INSERT INTO slack_identities (ship_talkers_id) VALUES ($1) ON CONFLICT (ship_talkers_id) DO UPDATE SET ship_talkers_id = EXCLUDED.ship_talkers_id RETURNING internal_id")
            .bind(&anonymous).fetch_one(pool).await?
    };
    cache
        .lock()
        .map_err(|_| "locator cache poisoned")?
        .insert(anonymous, id);
    Ok(id)
}

/// Parses an ISO "YYYY-MM-DD" date string into a `time::Date`.
pub fn parse_date(s: &str) -> Option<time::Date> {
    let mut parts = s.split('-');
    let year: i32 = parts.next()?.parse().ok()?;
    let month: u8 = parts.next()?.parse().ok()?;
    let day: u8 = parts.next()?.parse().ok()?;
    time::Date::from_calendar_date(year, time::Month::try_from(month).ok()?, day).ok()
}

pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub async fn insert_messages(
    pool: &PgPool,
    messages: &[SlackMessageRow],
) -> Result<u64, Box<dyn std::error::Error>> {
    if messages.is_empty() {
        return Ok(0);
    }
    let count = messages.len() as u64;
    for chunk in messages.chunks(INSERT_CHUNK) {
        let mut sql = String::from(
            "INSERT INTO slack_messages (identity_id, channel_id, message_ts, char_count, thread_ts) VALUES ",
        );
        sql.push_str(&placeholders(chunk.len(), 5));
        sql.push_str(
            " ON CONFLICT (channel_id, message_ts) DO UPDATE SET identity_id = EXCLUDED.identity_id, char_count = EXCLUDED.char_count, thread_ts = EXCLUDED.thread_ts",
        );
        let mut q = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        for msg in chunk {
            let identity_id = locator_id(pool, &msg.user_id, false).await?;
            let channel_id = locator_id(pool, &msg.channel_id, true).await?;
            q = q
                .bind(identity_id)
                .bind(channel_id)
                .bind(msg.message_ts as i64)
                .bind(msg.char_count)
                .bind(msg.thread_ts.map(|ts| ts as i64));
        }
        q.execute(pool).await?;
    }
    Ok(count)
}

pub async fn insert_reactions(
    pool: &PgPool,
    rows: &[SlackReactionRow],
) -> Result<u64, Box<dyn std::error::Error>> {
    if rows.is_empty() {
        return Ok(0);
    }
    let mut touched: Vec<(String, i64)> = rows
        .iter()
        .map(|r| (r.channel_id.clone(), r.message_ts as i64))
        .collect();
    touched.sort();
    touched.dedup();

    let mut tx = pool.begin().await?;
    for chunk in touched.chunks(INSERT_CHUNK) {
        let ids: Vec<String> = chunk.iter().map(|(id, _)| id.clone()).collect();
        let tss: Vec<i64> = chunk.iter().map(|(_, ts)| *ts).collect();
        sqlx::query(
            "DELETE FROM slack_reactions r
             USING unnest($1::text[], $2::bigint[]) AS t(channel_id, message_ts)
             WHERE r.channel_id = t.channel_id AND r.message_ts = t.message_ts",
        )
        .bind(&ids)
        .bind(&tss)
        .execute(&mut *tx)
        .await?;
    }
    for chunk in rows.chunks(INSERT_CHUNK) {
        let mut sql = String::from(
            "INSERT INTO slack_reactions (channel_id, message_ts, emoji, user_id) VALUES ",
        );
        sql.push_str(&placeholders(chunk.len(), 4));
        sql.push_str(" ON CONFLICT (channel_id, message_ts, emoji, user_id) DO NOTHING");
        let mut q = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        for row in chunk {
            q = q
                .bind(&row.channel_id)
                .bind(row.message_ts as i64)
                .bind(&row.emoji)
                .bind(&row.user_id);
        }
        q.execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(rows.len() as u64)
}

pub async fn get_known_channel_ids(
    pool: &PgPool,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let rows: Vec<String> =
        sqlx::query_scalar("SELECT channel_id FROM slack_channels ORDER BY channel_id")
            .fetch_all(pool)
            .await?;
    Ok(rows)
}

pub async fn get_user_updates(
    pool: &PgPool,
) -> Result<HashMap<String, u64>, Box<dyn std::error::Error>> {
    let rows: Vec<(String, i64)> = sqlx::query_as("SELECT user_id, updated FROM users")
        .fetch_all(pool)
        .await?;
    Ok(rows
        .into_iter()
        .map(|(id, updated)| (id, updated as u64))
        .collect())
}

pub async fn get_user_ids_without_pfp(
    pool: &PgPool,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let rows: Vec<String> = sqlx::query_scalar("SELECT user_id FROM users WHERE pfp = ''")
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

// Non-bot, non-deleted users missing their full-name/handle fields, so the users.list sync re-fetches them even if Slack's `updated` timestamp hasn't moved
// (e.g. right after the split of display_name into separate fields).
pub async fn get_user_ids_missing_profile(
    pool: &PgPool,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT user_id FROM users \
         WHERE is_bot = 0 AND is_deleted = 0 \
           AND (real_name = '' OR username = '')",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn get_max_message_ts(
    pool: &PgPool,
    channel_id: &str,
) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    let row: Option<Option<i64>> =
        sqlx::query_scalar("SELECT max(m.message_ts) FROM slack_messages m JOIN slack_channels c ON c.internal_id = m.channel_id WHERE c.channel_id = $1")
            .bind(channel_id)
            .fetch_optional(pool)
            .await?;
    Ok(row.flatten().map(|v| v.max(0) as u64))
}

pub async fn get_scraped_channel_ids(
    pool: &PgPool,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let rows: Vec<String> = sqlx::query_scalar("SELECT channel_id FROM scraped_channels")
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub async fn mark_channels_scraped(
    pool: &PgPool,
    channel_ids: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    if channel_ids.is_empty() {
        return Ok(());
    }
    for chunk in channel_ids.chunks(INSERT_CHUNK) {
        let mut sql = String::from("INSERT INTO scraped_channels (channel_id) VALUES ");
        sql.push_str(&placeholders(chunk.len(), 1));
        sql.push_str(" ON CONFLICT (channel_id) DO NOTHING");
        let mut q = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        for id in chunk {
            q = q.bind(id);
        }
        q.execute(pool).await?;
    }
    tracing::info!("Recorded {} channels as scraped", channel_ids.len());
    Ok(())
}

pub async fn mark_channel_scraped(
    pool: &PgPool,
    channel_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query(
        "INSERT INTO scraped_channels (channel_id) VALUES ($1) ON CONFLICT (channel_id) DO NOTHING",
    )
    .bind(channel_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn backfill_scraped_channels(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let initialized: Option<i16> =
        sqlx::query_scalar("SELECT done FROM backfill_meta WHERE name = 'scraped_channels'")
            .fetch_optional(pool)
            .await?;
    if initialized == Some(1) {
        return Ok(());
    }
    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT channel_id FROM (
            SELECT channel_id FROM scrape_checkpoints WHERE fully_scraped = 1
            UNION
            SELECT DISTINCT c.channel_id FROM slack_messages m JOIN slack_channels c ON c.internal_id = m.channel_id
        ) s
        WHERE channel_id NOT IN (SELECT channel_id FROM scraped_channels)",
    )
    .fetch_all(pool)
    .await?;

    if ids.is_empty() {
        sqlx::query(
            "INSERT INTO backfill_meta (name, done) VALUES ('scraped_channels', 1)
             ON CONFLICT (name) DO UPDATE SET done = EXCLUDED.done",
        )
        .execute(pool)
        .await?;
        return Ok(());
    }

    tracing::info!(
        "Backfilling {} previously-scraped channels into scraped_channels",
        ids.len()
    );
    mark_channels_scraped(pool, &ids).await?;
    sqlx::query(
        "INSERT INTO backfill_meta (name, done) VALUES ('scraped_channels', 1)
         ON CONFLICT (name) DO UPDATE SET done = EXCLUDED.done",
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn is_fully_scraped(
    pool: &PgPool,
    channel_id: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let count: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM scrape_checkpoints WHERE channel_id = $1 AND fully_scraped = 1",
    )
    .bind(channel_id)
    .fetch_optional(pool)
    .await?;
    Ok(count.is_some())
}

pub async fn mark_fully_scraped(
    pool: &PgPool,
    channel_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query(
        "INSERT INTO scrape_checkpoints (channel_id, fully_scraped) VALUES ($1, 1)
         ON CONFLICT (channel_id) DO UPDATE SET fully_scraped = EXCLUDED.fully_scraped",
    )
    .bind(channel_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn clear_fully_scraped(
    pool: &PgPool,
    channel_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query("UPDATE scrape_checkpoints SET fully_scraped = 0 WHERE channel_id = $1")
        .bind(channel_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn get_thread_rescan_at(
    pool: &PgPool,
    channel_id: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    let timestamp: Option<i64> =
        sqlx::query_scalar("SELECT thread_rescan_at FROM scrape_checkpoints WHERE channel_id = $1")
            .bind(channel_id)
            .fetch_optional(pool)
            .await?;
    Ok(timestamp.unwrap_or(0).max(0) as u64)
}

pub async fn mark_thread_rescan(
    pool: &PgPool,
    channel_id: &str,
    timestamp: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query(
        "INSERT INTO scrape_checkpoints (channel_id, fully_scraped, thread_rescan_at)
         VALUES ($1, 0, $2)
         ON CONFLICT (channel_id) DO UPDATE SET thread_rescan_at = EXCLUDED.thread_rescan_at",
    )
    .bind(channel_id)
    .bind(timestamp as i64)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_archived_channel_ids(
    pool: &PgPool,
    channel_ids: &[String],
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    if channel_ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(
        "SELECT channel_id FROM slack_channels WHERE channel_id = ANY($1) AND is_archived = 1",
    )
    .bind(channel_ids)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| r.get(0)).collect())
}

pub async fn get_archived_scraped_channel_ids(
    pool: &PgPool,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let rows = sqlx::query(
        "SELECT c.channel_id
         FROM slack_channels c
         JOIN scrape_checkpoints s ON s.channel_id = c.channel_id
         WHERE c.is_archived = 1 AND s.fully_scraped = 1",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| r.get(0)).collect())
}

pub async fn is_thread_fully_scraped(
    pool: &PgPool,
    channel_id: &str,
    thread_ts: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let row: Option<i16> = sqlx::query_scalar(
        "SELECT fully_scraped FROM thread_checkpoints WHERE channel_id = $1 AND thread_ts = $2",
    )
    .bind(channel_id)
    .bind(thread_ts)
    .fetch_optional(pool)
    .await?;
    Ok(row == Some(1))
}

pub async fn mark_thread_fully_scraped(
    pool: &PgPool,
    channel_id: &str,
    thread_ts: &str,
    latest_reply_ts: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query(
        "INSERT INTO thread_checkpoints
             (channel_id, thread_ts, fully_scraped, latest_reply_ts)
         VALUES ($1, $2, 1, $3)
         ON CONFLICT (channel_id, thread_ts) DO UPDATE SET
             fully_scraped = EXCLUDED.fully_scraped,
             latest_reply_ts = EXCLUDED.latest_reply_ts",
    )
    .bind(channel_id)
    .bind(thread_ts)
    .bind(latest_reply_ts as i64)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_thread_high_water_mark(
    pool: &PgPool,
    channel_id: &str,
    thread_ts: &str,
) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    let stored: Option<i64> = sqlx::query_scalar(
        "SELECT latest_reply_ts FROM thread_checkpoints
         WHERE channel_id = $1 AND thread_ts = $2",
    )
    .bind(channel_id)
    .bind(thread_ts)
    .fetch_optional(pool)
    .await?;
    if let Some(timestamp) = stored.filter(|&timestamp| timestamp > 0) {
        return Ok(Some(timestamp as u64));
    }
    get_max_thread_reply_ts(pool, channel_id, thread_ts).await
}

pub async fn get_thread_activities(
    pool: &PgPool,
    channel_id: &str,
    thread_ts: &[String],
) -> Result<HashMap<String, (bool, i64, u64)>, Box<dyn std::error::Error>> {
    if thread_ts.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<(String, i16, i64, i64)> = sqlx::query_as(
        "SELECT thread_ts, fully_scraped, slack_reply_count, slack_latest_reply_ts
         FROM thread_checkpoints
         WHERE channel_id = $1 AND thread_ts = ANY($2)",
    )
    .bind(channel_id)
    .bind(thread_ts)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(ts, fully_scraped, reply_count, latest_reply)| {
            (
                ts,
                (fully_scraped == 1, reply_count, latest_reply.max(0) as u64),
            )
        })
        .collect())
}

pub async fn upsert_thread_activities(
    pool: &PgPool,
    channel_id: &str,
    activities: &[(String, i64, u64)],
) -> Result<(), Box<dyn std::error::Error>> {
    if activities.is_empty() {
        return Ok(());
    }
    let timestamps: Vec<String> = activities.iter().map(|(ts, _, _)| ts.clone()).collect();
    let reply_counts: Vec<i64> = activities.iter().map(|(_, count, _)| *count).collect();
    let latest_replies: Vec<i64> = activities.iter().map(|(_, _, ts)| *ts as i64).collect();
    sqlx::query(
        "INSERT INTO thread_checkpoints
             (channel_id, thread_ts, fully_scraped, latest_reply_ts,
              slack_reply_count, slack_latest_reply_ts)
         SELECT $1, thread_ts, 0, 0, reply_count, latest_reply_ts
         FROM unnest($2::text[], $3::bigint[], $4::bigint[])
              AS activity(thread_ts, reply_count, latest_reply_ts)
         ON CONFLICT (channel_id, thread_ts) DO UPDATE SET
             slack_reply_count = EXCLUDED.slack_reply_count,
             slack_latest_reply_ts = EXCLUDED.slack_latest_reply_ts",
    )
    .bind(channel_id)
    .bind(&timestamps)
    .bind(&reply_counts)
    .bind(&latest_replies)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_max_thread_reply_ts(
    pool: &PgPool,
    channel_id: &str,
    thread_ts: &str,
) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    let row: Option<Option<i64>> = sqlx::query_scalar(
        "SELECT max(m.message_ts) FROM slack_messages m JOIN slack_channels c ON c.internal_id = m.channel_id WHERE c.channel_id = $1 AND m.thread_ts = $2",
    )
    .bind(channel_id)
    .bind(slack_ts_to_micros(thread_ts) as i64)
    .fetch_optional(pool)
    .await?;
    Ok(row.flatten().map(|v| v.max(0) as u64))
}

/// All distinct thread root timestamps stored for a channel. Used by the
/// thread-reply recovery pass to re-fetch threads whose first-scrape thread
/// phase was interrupted, since their roots are older than the rescan window.
pub async fn get_thread_roots(
    pool: &PgPool,
    channel_id: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let rows: Vec<i64> = sqlx::query_scalar(
        "SELECT DISTINCT thread_ts FROM slack_messages m JOIN slack_channels c ON c.internal_id = m.channel_id \
         WHERE c.channel_id = $1 AND thread_ts IS NOT NULL",
    )
    .bind(channel_id)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|ts| micros_to_slack_ts(ts.max(0) as u64))
        .collect())
}

/// The channel id where the incremental sweep should resume. Empty means a fresh sweep from the start of the channel list.
pub async fn get_sweep_resume(pool: &PgPool) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let row: Option<String> =
        sqlx::query_scalar("SELECT resume_channel FROM scrape_sweep WHERE id = 1")
            .fetch_optional(pool)
            .await?;
    Ok(row.filter(|r| !r.is_empty()))
}

/// Records the current sweep position as the last fully-processed channel.
pub async fn set_sweep_resume(
    pool: &PgPool,
    channel_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query(
        "INSERT INTO scrape_sweep (id, resume_channel, updated) VALUES (1, $1, now())
         ON CONFLICT (id) DO UPDATE SET resume_channel = EXCLUDED.resume_channel, updated = now()",
    )
    .bind(channel_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Clears the resume cursor so the next sweep starts fresh from the start.
pub async fn clear_sweep_resume(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query(
        "INSERT INTO scrape_sweep (id, resume_channel, updated) VALUES (1, '', now())
         ON CONFLICT (id) DO UPDATE SET resume_channel = '', updated = now()",
    )
    .execute(pool)
    .await?;
    Ok(())
}
