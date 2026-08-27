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
