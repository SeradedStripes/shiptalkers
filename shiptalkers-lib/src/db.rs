use sqlx::PgPool;

pub const INSERT_CHUNK: usize = 500;

#[derive(Debug, Clone)]
pub struct SlackChannelRow {
    pub channel_id: String,
    pub name: String,
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
    // Serves the per-user Slack Time queries directly off slack_messages
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS slack_messages_user_ts_idx ON slack_messages (user_id, message_ts)",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS slack_messages_thread_idx ON slack_messages (channel_id, thread_ts) WHERE thread_ts IS NOT NULL",
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
        "CREATE TABLE IF NOT EXISTS stats_meta (
            id SMALLINT PRIMARY KEY,
            total_messages BIGINT NOT NULL DEFAULT 0,
            total_channels BIGINT NOT NULL DEFAULT 0,
            total_users BIGINT NOT NULL DEFAULT 0,
            coding_minutes BIGINT NOT NULL DEFAULT 0,
            slack_time_secs BIGINT NOT NULL DEFAULT 0,
            db_size_bytes BIGINT NOT NULL DEFAULT 0,
            updated BIGINT NOT NULL DEFAULT 0
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS message_count (
            id SMALLINT PRIMARY KEY,
            total BIGINT NOT NULL DEFAULT 0
        )",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE OR REPLACE FUNCTION increment_message_count() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             INSERT INTO message_count (id, total) VALUES (1, 1)
             ON CONFLICT (id) DO UPDATE SET total = message_count.total + 1;
             RETURN NULL;
         END;
         $$",
    )
    .execute(pool)
    .await?;
    sqlx::query("DROP TRIGGER IF EXISTS message_count_insert ON slack_messages")
        .execute(pool)
        .await?;
    sqlx::query(
        "CREATE TRIGGER message_count_insert AFTER INSERT ON slack_messages
         FOR EACH ROW EXECUTE FUNCTION increment_message_count()",
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

    // One-time cleanup of leftovers no longer created by this schema: the
    // denormalized slack_messages_by_user copy (and its trigger/function/index)
    // and any stale compaction flag from the removed compact_toast_once task.
    // All idempotent, so they only do work on migrations where the objects exist.
    sqlx::query("DROP TABLE IF EXISTS slack_messages_by_user")
        .execute(pool)
        .await?;
    sqlx::query("DROP TRIGGER IF EXISTS slack_messages_by_user_sync ON slack_messages")
        .execute(pool)
        .await?;
    sqlx::query("DROP FUNCTION IF EXISTS sync_slack_messages_by_user()")
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM backfill_meta WHERE name = 'toast_compress'")
        .execute(pool)
        .await?;

    Ok(())
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
        let mut q = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        for ch in chunk {
            q = q.bind(&ch.channel_id).bind(&ch.name);
        }
        q.execute(pool).await?;
    }
    tracing::info!("Inserted {} new channels into Postgres", count);
    Ok(count)
}
