use sqlx::PgPool;
use std::collections::HashMap;

pub const INSERT_CHUNK: usize = 500;

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
pub struct SlackChannelRow {
    pub channel_id: String,
    pub name: String,
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

pub fn placeholders(rows: usize, cols: usize) -> String {
    (0..rows)
        .map(|r| {
            let inner: Vec<String> = ((r * cols + 1)..=(r * cols + cols))
                .map(|c| format!("${c}"))
                .collect();
            format!("({})", inner.join(", "))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub async fn connect(database_url: &str) -> Result<PgPool, Box<dyn std::error::Error>> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(20)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET TIME ZONE 'UTC'")
                    .execute(conn)
                    .await
                    .map(|_| ())
            })
        })
        .connect(database_url)
        .await?;
    Ok(pool)
}

pub async fn init_tables(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS slack_messages (
            user_id TEXT NOT NULL DEFAULT '',
            channel_id TEXT NOT NULL DEFAULT '',
            message_ts BIGINT NOT NULL,
            text TEXT NOT NULL DEFAULT '',
            thread_ts TEXT,
            PRIMARY KEY (channel_id, message_ts)
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS slack_messages_by_user (
            user_id TEXT NOT NULL DEFAULT '',
            channel_id TEXT NOT NULL DEFAULT '',
            message_ts BIGINT NOT NULL,
            text TEXT NOT NULL DEFAULT '',
            thread_ts TEXT,
            PRIMARY KEY (user_id, channel_id, message_ts)
        )",
    )
    .execute(pool)
    .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS slack_messages_by_user_channel_idx ON slack_messages_by_user (channel_id)")
        .execute(pool)
        .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS slack_messages_thread_idx ON slack_messages (channel_id, thread_ts) WHERE thread_ts IS NOT NULL",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE OR REPLACE FUNCTION sync_slack_messages_by_user() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             INSERT INTO slack_messages_by_user (user_id, channel_id, message_ts, text, thread_ts)
             VALUES (NEW.user_id, NEW.channel_id, NEW.message_ts, NEW.text, NEW.thread_ts)
             ON CONFLICT (user_id, channel_id, message_ts)
             DO UPDATE SET text = EXCLUDED.text, thread_ts = EXCLUDED.thread_ts;
             RETURN NULL;
         END;
         $$",
    )
    .execute(pool)
    .await?;
    sqlx::query("DROP TRIGGER IF EXISTS slack_messages_by_user_sync ON slack_messages")
        .execute(pool)
        .await?;
    sqlx::query(
        "CREATE TRIGGER slack_messages_by_user_sync AFTER INSERT ON slack_messages
         FOR EACH ROW EXECUTE FUNCTION sync_slack_messages_by_user()",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS slack_channels (
            channel_id TEXT PRIMARY KEY,
            name TEXT NOT NULL DEFAULT ''
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            display_name TEXT NOT NULL DEFAULT '',
            pfp TEXT NOT NULL DEFAULT '',
            updated BIGINT NOT NULL DEFAULT 0,
            is_bot SMALLINT NOT NULL DEFAULT 0,
            is_deleted SMALLINT NOT NULL DEFAULT 0
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS scrape_checkpoints (
            channel_id TEXT PRIMARY KEY,
            fully_scraped SMALLINT NOT NULL DEFAULT 0
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS thread_checkpoints (
            channel_id TEXT NOT NULL,
            thread_ts TEXT NOT NULL,
            fully_scraped SMALLINT NOT NULL DEFAULT 0,
            PRIMARY KEY (channel_id, thread_ts)
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS scraped_channels (
            channel_id TEXT PRIMARY KEY,
            scraped_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS user_scores (
            user_id TEXT PRIMARY KEY,
            score BIGINT NOT NULL DEFAULT 0,
            total_time BIGINT NOT NULL DEFAULT 0,
            messages BIGINT NOT NULL DEFAULT 0,
            sessions BIGINT NOT NULL DEFAULT 0,
            longest BIGINT NOT NULL DEFAULT 0,
            days BIGINT NOT NULL DEFAULT 0,
            channels BIGINT NOT NULL DEFAULT 0,
            first_ts BIGINT NOT NULL DEFAULT 0,
            last_ts BIGINT NOT NULL DEFAULT 0,
            active_hour SMALLINT NOT NULL DEFAULT 0,
            updated BIGINT NOT NULL DEFAULT 0
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS hackatime_connections (
            slack_id TEXT PRIMARY KEY,
            access_token TEXT NOT NULL DEFAULT '',
            last_synced_date TEXT,
            connected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            status TEXT NOT NULL DEFAULT '',
            total_minutes BIGINT NOT NULL DEFAULT 0
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS hackatime_spans (
            slack_id TEXT NOT NULL,
            start_ts BIGINT NOT NULL,
            duration BIGINT NOT NULL DEFAULT 0,
            updated BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (slack_id, start_ts)
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS slack_reactions (
            channel_id TEXT NOT NULL,
            message_ts BIGINT NOT NULL,
            emoji TEXT NOT NULL,
            user_id TEXT NOT NULL,
            PRIMARY KEY (channel_id, message_ts, emoji, user_id)
        )",
    )
    .execute(pool)
    .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS slack_reactions_message_idx ON slack_reactions (channel_id, message_ts)")
        .execute(pool)
        .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS word_counts (
            word TEXT NOT NULL,
            user_id TEXT NOT NULL DEFAULT '',
            channel_id TEXT NOT NULL DEFAULT '',
            message_ts BIGINT NOT NULL,
            count BIGINT NOT NULL DEFAULT 0,
            inserted_at BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (word, channel_id, message_ts)
        )",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS word_counts_inserted_at_idx ON word_counts (inserted_at)",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS word_totals (
            word TEXT PRIMARY KEY,
            cnt BIGINT NOT NULL DEFAULT 0,
            updated BIGINT NOT NULL DEFAULT 0
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS channel_scores (
            channel_id TEXT PRIMARY KEY,
            total_time BIGINT NOT NULL DEFAULT 0,
            messages BIGINT NOT NULL DEFAULT 0,
            updated BIGINT NOT NULL DEFAULT 0
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS score_meta (
            id SMALLINT PRIMARY KEY,
            formula TEXT NOT NULL DEFAULT ''
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS backfill_meta (
            name TEXT PRIMARY KEY,
            done SMALLINT NOT NULL DEFAULT 0
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS word_refresh_meta (
            id SMALLINT PRIMARY KEY,
            watermark BIGINT NOT NULL DEFAULT 0,
            last_full BIGINT NOT NULL DEFAULT 0
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS daily_stats (
            date DATE PRIMARY KEY,
            slack_secs BIGINT NOT NULL DEFAULT 0
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS linked_users (
            slack_id TEXT PRIMARY KEY,
            display_name TEXT NOT NULL,
            linked_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Copies existing rows from slack_messages into slack_messages_by_user once.
/// The trigger keeps new inserts in sync, so this only needs to run when the
/// secondary table is empty (first deploy or after a migration import).
pub async fn backfill_slack_messages_by_user(
    pool: &PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM slack_messages_by_user")
        .fetch_one(pool)
        .await
        .unwrap_or(0);
    if count > 0 {
        return Ok(());
    }
    tracing::info!("Backfilling slack_messages_by_user from slack_messages...");
    sqlx::query(
        "INSERT INTO slack_messages_by_user (user_id, channel_id, message_ts, text, thread_ts)
         SELECT user_id, channel_id, message_ts, text, thread_ts FROM slack_messages
         ON CONFLICT (user_id, channel_id, message_ts) DO NOTHING",
    )
    .execute(pool)
    .await?;
    tracing::info!("slack_messages_by_user backfill complete");
    Ok(())
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
            " ON CONFLICT (word, channel_id, message_ts) DO UPDATE SET count = EXCLUDED.count, user_id = EXCLUDED.user_id, inserted_at = EXCLUDED.inserted_at",
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

pub async fn insert_new_channels(
    pool: &PgPool,
    channels: &[SlackChannelRow],
    known: &mut std::collections::HashSet<String>,
) -> Result<u64, Box<dyn std::error::Error>> {
    let new_channels: Vec<&SlackChannelRow> = channels
        .iter()
        .filter(|ch| known.insert(ch.channel_id.clone()))
        .collect();

    if new_channels.is_empty() {
        return Ok(0);
    }

    let refs: Vec<SlackChannelRow> = new_channels.into_iter().cloned().collect();
    insert_new_channels_rows(pool, &refs).await
}

pub async fn insert_new_channels_rows(
    pool: &PgPool,
    channels: &[SlackChannelRow],
) -> Result<u64, Box<dyn std::error::Error>> {
    if channels.is_empty() {
        return Ok(0);
    }
    let count = channels.len() as u64;
    for chunk in channels.chunks(INSERT_CHUNK) {
        let mut sql = String::from("INSERT INTO slack_channels (channel_id, name) VALUES ");
        sql.push_str(&placeholders(chunk.len(), 2));
        sql.push_str(" ON CONFLICT (channel_id) DO UPDATE SET name = EXCLUDED.name");
        let mut q = sqlx::query(&sql);
        for ch in chunk {
            q = q.bind(&ch.channel_id).bind(&ch.name);
        }
        q.execute(pool).await?;
    }
    tracing::info!("Inserted {} new channels into Postgres", count);
    Ok(count)
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

/// Linked-user state backing OAuth sign-in and hackatime linking.
#[derive(Clone)]
pub struct AuthDb {
    pool: PgPool,
}

impl AuthDb {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn mark_linked(&self, slack_id: &str, display_name: &str) -> Result<(), String> {
        sqlx::query(
            "INSERT INTO linked_users (slack_id, display_name) VALUES ($1, $2)
             ON CONFLICT (slack_id) DO UPDATE SET display_name = EXCLUDED.display_name",
        )
        .bind(slack_id)
        .bind(display_name)
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
    }

    pub async fn is_linked(&self, slack_id: &str) -> bool {
        sqlx::query_scalar::<_, i32>("SELECT 1 FROM linked_users WHERE slack_id = $1")
            .bind(slack_id)
            .fetch_optional(&self.pool)
            .await
            .is_ok_and(|row| row.is_some())
    }
}
