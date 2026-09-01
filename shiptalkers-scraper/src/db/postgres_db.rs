use sqlx::PgPool;
use std::collections::HashMap;

pub use ship_talkers_lib::db::{
    INSERT_CHUNK, SlackChannelRow, connect, init_tables, insert_new_channels_rows, placeholders,
};

/// Reconciles the maintained `message_count` with the real row count.
pub async fn seed_message_count(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
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
    pub text: String,
    pub thread_ts: Option<String>,
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

/// Parses an ISO "YYYY-MM-DD" date string into a `time::Date`.
pub fn parse_date(s: &str) -> Option<time::Date> {
    let mut parts = s.split('-');
    let year: i32 = parts.next()?.parse().ok()?;
    let month: u8 = parts.next()?.parse().ok()?;
    let day: u8 = parts.next()?.parse().ok()?;
    time::Date::from_calendar_date(year, time::Month::try_from(month).ok()?, day).ok()
}

#[derive(Debug, Clone)]
pub struct SlackUserRow {
    pub user_id: String,
    pub display_name: String,
    pub pfp: String,
    pub updated: u64,
    pub is_bot: u8,
    pub is_deleted: u8,
}

#[derive(Debug, Clone)]
pub struct WordCountRow {
    pub word: String,
    pub user_id: String,
    pub channel_id: String,
    pub message_ts: u64,
    pub count: u64,
    pub inserted_at: u64,
}

pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Builds word_counts for every existing slack_messages row once, so the Top
/// Words leaderboard is all-time on first deploy. New inserts keep it in sync
/// from then on. Completion is recorded in `backfill_meta`, so the full-table
/// scan runs exactly once and never again on later restarts; a non-empty
/// `word_counts` counts as done too.
pub async fn backfill_word_counts(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
    if word_counts_backfilled(pool).await {
        return Ok(());
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM word_counts")
        .fetch_one(pool)
        .await
        .unwrap_or(0);
    if count > 0 {
        mark_word_counts_backfilled(pool).await?;
        return Ok(());
    }
    tracing::info!("Backfilling word_counts from slack_messages...");
    sqlx::query(
        "INSERT INTO word_counts (word, user_id, channel_id, message_ts, count, inserted_at)
         SELECT word, user_id, channel_id, message_ts, count(*), $1::bigint
         FROM (
             SELECT user_id, channel_id, message_ts,
                    (regexp_matches(lower(text), '[a-z]+', 'g'))[1] AS word
             FROM slack_messages
         ) t
         WHERE length(word) > 1
         GROUP BY word, user_id, channel_id, message_ts",
    )
    .bind(now_secs() as i64)
    .execute(pool)
    .await?;
    mark_word_counts_backfilled(pool).await?;
    tracing::info!("word_counts backfill complete");
    Ok(())
}

/// Whether the word_counts one-time backfill has already completed, read from
/// `backfill_meta`. A transient DB failure falls back to false, which only
/// re-attempts the backfill.
async fn word_counts_backfilled(pool: &PgPool) -> bool {
    sqlx::query_scalar::<_, i16>("SELECT done FROM backfill_meta WHERE name = 'word_counts'")
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .map(|done| done == 1)
        .unwrap_or(false)
}

/// Records that the word_counts backfill is complete.
async fn mark_word_counts_backfilled(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query(
        "INSERT INTO backfill_meta (name, done) VALUES ('word_counts', 1)
         ON CONFLICT (name) DO UPDATE SET done = EXCLUDED.done",
    )
    .execute(pool)
    .await?;
    Ok(())
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
            "INSERT INTO slack_messages (user_id, channel_id, message_ts, text, thread_ts) VALUES ",
        );
        sql.push_str(&placeholders(chunk.len(), 5));
        sql.push_str(
            " ON CONFLICT (channel_id, message_ts) DO UPDATE SET user_id = EXCLUDED.user_id, text = EXCLUDED.text, thread_ts = EXCLUDED.thread_ts",
        );
        let mut q = sqlx::query(&sql);
        for msg in chunk {
            q = q
                .bind(&msg.user_id)
                .bind(&msg.channel_id)
                .bind(msg.message_ts as i64)
                .bind(&msg.text)
                .bind(&msg.thread_ts);
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
        let mut q = sqlx::query(&sql);
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

pub async fn insert_word_counts(
    pool: &PgPool,
    rows: &[WordCountRow],
) -> Result<u64, Box<dyn std::error::Error>> {
    if rows.is_empty() {
        return Ok(0);
    }
    let now = now_secs();
    let count = rows.len() as u64;
    for chunk in rows.chunks(INSERT_CHUNK) {
        let mut sql = String::from(
            "INSERT INTO word_counts (word, user_id, channel_id, message_ts, count, inserted_at) VALUES ",
        );
        sql.push_str(&placeholders(chunk.len(), 6));
        sql.push_str(
            " ON CONFLICT (word, channel_id, message_ts) DO UPDATE SET count = EXCLUDED.count, user_id = EXCLUDED.user_id, inserted_at = EXCLUDED.inserted_at \
             WHERE word_counts.count IS DISTINCT FROM EXCLUDED.count",
        );
        let mut q = sqlx::query(&sql);
        for row in chunk {
            q = q
                .bind(&row.word)
                .bind(&row.user_id)
                .bind(&row.channel_id)
                .bind(row.message_ts as i64)
                .bind(row.count as i64)
                .bind(
                    (if row.inserted_at == 0 {
                        now
                    } else {
                        row.inserted_at
                    }) as i64,
                );
        }
        q.execute(pool).await?;
    }
    Ok(count)
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

pub async fn upsert_users(
    pool: &PgPool,
    users: &[SlackUserRow],
) -> Result<(), Box<dyn std::error::Error>> {
    if users.is_empty() {
        return Ok(());
    }
    for chunk in users.chunks(INSERT_CHUNK) {
        let mut sql = String::from(
            "INSERT INTO users (user_id, display_name, pfp, updated, is_bot, is_deleted) VALUES ",
        );
        sql.push_str(&placeholders(chunk.len(), 6));
        sql.push_str(
            " ON CONFLICT (user_id) DO UPDATE SET display_name = EXCLUDED.display_name, pfp = EXCLUDED.pfp, updated = EXCLUDED.updated, is_bot = EXCLUDED.is_bot, is_deleted = EXCLUDED.is_deleted",
        );
        let mut q = sqlx::query(&sql);
        for u in chunk {
            q = q
                .bind(&u.user_id)
                .bind(&u.display_name)
                .bind(&u.pfp)
                .bind(u.updated as i64)
                .bind(u.is_bot as i16)
                .bind(u.is_deleted as i16);
        }
        q.execute(pool).await?;
    }
    Ok(())
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

pub async fn get_max_message_ts(
    pool: &PgPool,
    channel_id: &str,
) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    let row: Option<Option<i64>> =
        sqlx::query_scalar("SELECT max(message_ts) FROM slack_messages WHERE channel_id = $1")
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
        let mut q = sqlx::query(&sql);
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
    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT channel_id FROM (
            SELECT channel_id FROM scrape_checkpoints WHERE fully_scraped = 1
            UNION
            SELECT DISTINCT channel_id FROM slack_messages
        ) s
        WHERE channel_id NOT IN (SELECT channel_id FROM scraped_channels)",
    )
    .fetch_all(pool)
    .await?;

    if ids.is_empty() {
        return Ok(());
    }

    tracing::info!(
        "Backfilling {} previously-scraped channels into scraped_channels",
        ids.len()
    );
    mark_channels_scraped(pool, &ids).await?;
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
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query(
        "INSERT INTO thread_checkpoints (channel_id, thread_ts, fully_scraped) VALUES ($1, $2, 1)
         ON CONFLICT (channel_id, thread_ts) DO UPDATE SET fully_scraped = EXCLUDED.fully_scraped",
    )
    .bind(channel_id)
    .bind(thread_ts)
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
        "SELECT max(message_ts) FROM slack_messages WHERE channel_id = $1 AND thread_ts = $2",
    )
    .bind(channel_id)
    .bind(thread_ts)
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
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT thread_ts FROM slack_messages \
         WHERE channel_id = $1 AND thread_ts IS NOT NULL AND thread_ts != ''",
    )
    .bind(channel_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}
