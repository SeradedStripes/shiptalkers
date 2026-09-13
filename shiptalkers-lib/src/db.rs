use sqlx::PgPool;

pub const INSERT_CHUNK: usize = 5_000;

#[derive(Debug, Clone)]
pub struct SlackChannelRow {
    pub channel_id: String,
    pub name: String,
    pub is_archived: u8,
    pub num_members: u64,
}

#[derive(Debug, Clone)]
pub struct SlackUserRow {
    pub user_id: String,
    pub merged_name: String,
    pub display_name: String,
    pub real_name: String,
    pub username: String,
    pub email: String,
    pub title: String,
    pub status_text: String,
    pub status_emoji: String,
    pub tz: String,
    pub tz_label: String,
    pub locale: String,
    pub pfp: String,
    pub updated: u64,
    pub is_bot: u8,
    pub is_deleted: u8,
    pub is_admin: u8,
    pub is_owner: u8,
    pub is_restricted: u8,
    pub is_app_user: u8,
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

pub async fn migrate(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::migrate!("./migrations").run(pool).await?;
    Ok(())
}

/// Finds locators for a user's consent request.
pub async fn locators_for_slack_user(
    pool: &PgPool,
    slack_user_id: &str,
) -> Result<Vec<(i32, i64)>, Box<dyn std::error::Error>> {
    let anonymous_id = crate::base36::encode(slack_user_id.as_bytes());
    Ok(sqlx::query_as(
        "SELECT m.channel_id, m.message_ts
         FROM slack_messages m
         JOIN slack_identities i ON i.internal_id = m.identity_id
         WHERE i.anonymous_id = $1
         ORDER BY m.message_ts",
    )
    .bind(anonymous_id)
    .fetch_all(pool)
    .await?)
}

pub async fn init_tables(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS slack_identities (
            internal_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            anonymous_id TEXT UNIQUE NOT NULL
        )",
    )
    .execute(pool)
    .await?;

    // Keep the raw channel ID for Slack API calls.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS slack_channels (
            internal_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            anonymous_id TEXT UNIQUE NOT NULL,
            channel_id TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL DEFAULT '',
            is_archived SMALLINT NOT NULL DEFAULT 0,
            num_members BIGINT NOT NULL DEFAULT 0
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS slack_messages (
            identity_id INTEGER NOT NULL REFERENCES slack_identities(internal_id),
            channel_id INTEGER NOT NULL REFERENCES slack_channels(internal_id),
            message_ts BIGINT NOT NULL,
            char_count INTEGER NOT NULL DEFAULT 0,
            thread_ts BIGINT,
            PRIMARY KEY (channel_id, message_ts)
        )",
    )
    .execute(pool)
    .await?;
    // Add columns required by the new channel mapping.
    sqlx::query("ALTER TABLE slack_channels ADD COLUMN IF NOT EXISTS internal_id INTEGER GENERATED ALWAYS AS IDENTITY")
        .execute(pool).await?;
    sqlx::query("ALTER TABLE slack_channels ADD COLUMN IF NOT EXISTS anonymous_id TEXT")
        .execute(pool)
        .await?;
    sqlx::query(
        "ALTER TABLE slack_channels ADD COLUMN IF NOT EXISTS is_archived SMALLINT NOT NULL DEFAULT 0",
    )
        .execute(pool)
        .await?;
    sqlx::query(
        "ALTER TABLE slack_channels ADD COLUMN IF NOT EXISTS num_members BIGINT NOT NULL DEFAULT 0",
    )
    .execute(pool)
    .await?;

    sqlx::query("CREATE UNIQUE INDEX IF NOT EXISTS slack_channels_anonymous_id_idx ON slack_channels (anonymous_id) WHERE anonymous_id IS NOT NULL").execute(pool).await?;
    sqlx::query("CREATE UNIQUE INDEX IF NOT EXISTS slack_channels_internal_id_idx ON slack_channels (internal_id)").execute(pool).await?;
    migrate_locator_messages(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS slack_messages_identity_ts_idx ON slack_messages (identity_id, message_ts)").execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS slack_messages_identity_channel_ts_idx ON slack_messages (identity_id, channel_id, message_ts)").execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS slack_messages_channel_ts_idx ON slack_messages (channel_id, message_ts)").execute(pool).await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS slack_message_contents (
            channel_id INTEGER NOT NULL,
            message_ts BIGINT NOT NULL,
            text TEXT NOT NULL,
            PRIMARY KEY (channel_id, message_ts),
            FOREIGN KEY (channel_id, message_ts)
                REFERENCES slack_messages(channel_id, message_ts)
                ON DELETE CASCADE
        )",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS slack_consents (
            identity_id INTEGER PRIMARY KEY REFERENCES slack_identities(internal_id) ON DELETE CASCADE,
            consented_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            revoked_at TIMESTAMPTZ,
            consent_source TEXT
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            anonymous_id TEXT UNIQUE,
            merged_name TEXT NOT NULL DEFAULT '',
            display_name TEXT NOT NULL DEFAULT '',
            real_name TEXT NOT NULL DEFAULT '',
            username TEXT NOT NULL DEFAULT '',
            email TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL DEFAULT '',
            status_text TEXT NOT NULL DEFAULT '',
            status_emoji TEXT NOT NULL DEFAULT '',
            tz TEXT NOT NULL DEFAULT '',
            tz_label TEXT NOT NULL DEFAULT '',
            locale TEXT NOT NULL DEFAULT '',
            pfp TEXT NOT NULL DEFAULT '',
            updated BIGINT NOT NULL DEFAULT 0,
            is_bot SMALLINT NOT NULL DEFAULT 0,
            is_deleted SMALLINT NOT NULL DEFAULT 0,
            is_admin SMALLINT NOT NULL DEFAULT 0,
            is_owner SMALLINT NOT NULL DEFAULT 0,
            is_restricted SMALLINT NOT NULL DEFAULT 0,
            is_app_user SMALLINT NOT NULL DEFAULT 0
        )",
    )
    .execute(pool)
    .await?;
    sqlx::query("ALTER TABLE users ADD COLUMN IF NOT EXISTS anonymous_id TEXT UNIQUE")
        .execute(pool)
        .await?;
    let user_ids: Vec<String> =
        sqlx::query_scalar("SELECT user_id FROM users WHERE anonymous_id IS NULL")
            .fetch_all(pool)
            .await?;
    for user_id in user_ids {
        sqlx::query("UPDATE users SET anonymous_id = $1 WHERE user_id = $2")
            .bind(crate::base36::encode(user_id.as_bytes()))
            .bind(user_id)
            .execute(pool)
            .await?;
    }
    // Migrate pre-merged_name schemas: rename display_name to merged_name, add profile fields.
    sqlx::query(
        "DO $$
         BEGIN
             IF EXISTS (SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'users' AND column_name = 'display_name')
             AND NOT EXISTS (SELECT 1 FROM information_schema.columns
                             WHERE table_name = 'users' AND column_name = 'merged_name') THEN
                 ALTER TABLE users RENAME COLUMN display_name TO merged_name;
             END IF;
         END
         $$",
    )
    .execute(pool)
    .await?;
    sqlx::query("ALTER TABLE users ADD COLUMN IF NOT EXISTS display_name TEXT NOT NULL DEFAULT ''")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE users ADD COLUMN IF NOT EXISTS real_name TEXT NOT NULL DEFAULT ''")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT NOT NULL DEFAULT ''")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE users ADD COLUMN IF NOT EXISTS email TEXT NOT NULL DEFAULT ''")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE users ADD COLUMN IF NOT EXISTS title TEXT NOT NULL DEFAULT ''")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE users ADD COLUMN IF NOT EXISTS status_text TEXT NOT NULL DEFAULT ''")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE users ADD COLUMN IF NOT EXISTS status_emoji TEXT NOT NULL DEFAULT ''")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE users ADD COLUMN IF NOT EXISTS tz TEXT NOT NULL DEFAULT ''")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE users ADD COLUMN IF NOT EXISTS tz_label TEXT NOT NULL DEFAULT ''")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE users ADD COLUMN IF NOT EXISTS locale TEXT NOT NULL DEFAULT ''")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE users ADD COLUMN IF NOT EXISTS is_admin SMALLINT NOT NULL DEFAULT 0")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE users ADD COLUMN IF NOT EXISTS is_owner SMALLINT NOT NULL DEFAULT 0")
        .execute(pool)
        .await?;
    sqlx::query(
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_restricted SMALLINT NOT NULL DEFAULT 0",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_app_user SMALLINT NOT NULL DEFAULT 0",
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
        "CREATE TABLE IF NOT EXISTS scrape_sweep (
            id SMALLINT PRIMARY KEY,
            resume_channel TEXT NOT NULL DEFAULT '',
            updated TIMESTAMPTZ NOT NULL DEFAULT now()
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
    let content_cleanup_done: Option<i16> =
        sqlx::query_scalar("SELECT done FROM backfill_meta WHERE name = 'content_policy_cleanup'")
            .fetch_optional(pool)
            .await?;
    if content_cleanup_done != Some(1) {
        tracing::warn!("Clearing legacy content-derived word indexes without consent provenance");
        sqlx::query("DELETE FROM word_counts").execute(pool).await?;
        sqlx::query("DELETE FROM word_totals").execute(pool).await?;
        sqlx::query("UPDATE word_refresh_meta SET watermark = 0 WHERE id = 1")
            .execute(pool)
            .await?;
        sqlx::query(
            "INSERT INTO backfill_meta (name, done) VALUES ('content_policy_cleanup', 1)
             ON CONFLICT (name) DO UPDATE SET done = EXCLUDED.done",
        )
        .execute(pool)
        .await?;
    }

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
            archived_channels BIGINT NOT NULL DEFAULT 0,
            total_users BIGINT NOT NULL DEFAULT 0,
            coding_minutes BIGINT NOT NULL DEFAULT 0,
            slack_time_secs BIGINT NOT NULL DEFAULT 0,
            db_size_bytes BIGINT NOT NULL DEFAULT 0,
            updated BIGINT NOT NULL DEFAULT 0
        )",
    )
    .execute(pool)
    .await?;

    // Migrate pre-existing stats_meta rows
    sqlx::query(
        "ALTER TABLE stats_meta ADD COLUMN IF NOT EXISTS archived_channels BIGINT NOT NULL DEFAULT 0",
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

    // Only the hash of each API key is stored
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS api_keys (
            key_id TEXT PRIMARY KEY,
            slack_id TEXT NOT NULL,
            key_hash TEXT NOT NULL,
            created_at BIGINT NOT NULL,
            last_used_at BIGINT
        )",
    )
    .execute(pool)
    .await?;

    // A grant lets one user (grantor) open their data to a specific API key
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS api_key_grants (
            grantor_id TEXT NOT NULL,
            key_id TEXT NOT NULL REFERENCES api_keys(key_id) ON DELETE CASCADE,
            created_at BIGINT NOT NULL,
            PRIMARY KEY (grantor_id, key_id)
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

async fn migrate_locator_messages(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let has_old_user: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'slack_messages' AND column_name = 'user_id')").fetch_one(pool).await?;
    if !has_old_user {
        return Ok(());
    }
    let channel_ids: Vec<String> = sqlx::query_scalar("SELECT channel_id FROM slack_channels")
        .fetch_all(pool)
        .await?;
    for id in &channel_ids {
        sqlx::query("UPDATE slack_channels SET anonymous_id = $1 WHERE channel_id = $2 AND anonymous_id IS NULL")
            .bind(crate::base36::encode(id.as_bytes())).bind(id).execute(pool).await?;
    }

    let old_users: Vec<String> = sqlx::query_scalar("SELECT DISTINCT user_id FROM slack_messages")
        .fetch_all(pool)
        .await
        .unwrap_or_default();
    for id in &old_users {
        sqlx::query("INSERT INTO slack_identities (anonymous_id) VALUES ($1) ON CONFLICT (anonymous_id) DO NOTHING")
            .bind(crate::base36::encode(id.as_bytes())).execute(pool).await?;
    }

    // Rename legacy message columns during upgrade.
    sqlx::query("ALTER TABLE slack_messages ADD COLUMN IF NOT EXISTS identity_id INTEGER REFERENCES slack_identities(internal_id)").execute(pool).await?;
    sqlx::query("ALTER TABLE slack_messages ADD COLUMN IF NOT EXISTS channel_ref INTEGER REFERENCES slack_channels(internal_id)").execute(pool).await?;
    sqlx::query(
        "ALTER TABLE slack_messages ADD COLUMN IF NOT EXISTS char_count INTEGER NOT NULL DEFAULT 0",
    )
    .execute(pool)
    .await?;
    // The old table has text locator columns.
    sqlx::query("ALTER TABLE slack_messages RENAME COLUMN user_id TO legacy_user_id")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE slack_messages RENAME COLUMN channel_id TO legacy_channel_id")
        .execute(pool)
        .await?;
    // Populate compact IDs before dropping raw columns.
    let rows: Vec<(String, String, i64)> = sqlx::query_as("SELECT legacy_user_id, legacy_channel_id, message_ts FROM slack_messages WHERE identity_id IS NULL").fetch_all(pool).await?;
    for (user, channel, ts) in rows {
        let uid: Option<i32> =
            sqlx::query_scalar("SELECT internal_id FROM slack_identities WHERE anonymous_id = $1")
                .bind(crate::base36::encode(user.as_bytes()))
                .fetch_optional(pool)
                .await?;
        let cid: Option<i32> =
            sqlx::query_scalar("SELECT internal_id FROM slack_channels WHERE channel_id = $1")
                .bind(&channel)
                .fetch_optional(pool)
                .await?;
        if let (Some(uid), Some(cid)) = (uid, cid) {
            sqlx::query("UPDATE slack_messages SET identity_id = $1, channel_ref = $2 WHERE legacy_user_id = $3 AND legacy_channel_id = $4 AND message_ts = $5")
                    .bind(uid).bind(cid).bind(&user).bind(&channel).bind(ts).execute(pool).await?;
        }
    }
    sqlx::query("UPDATE slack_messages SET char_count = char_length(text) WHERE char_count = 0")
        .execute(pool)
        .await?;
    tracing::warn!("Dropping legacy message text; existing content has no consent provenance");
    sqlx::query("ALTER TABLE slack_messages DROP CONSTRAINT IF EXISTS slack_messages_pkey")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE slack_messages DROP COLUMN legacy_user_id, DROP COLUMN legacy_channel_id, DROP COLUMN text, DROP COLUMN thread_ts").execute(pool).await?;
    sqlx::query("ALTER TABLE slack_messages RENAME COLUMN channel_ref TO channel_id")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE slack_messages RENAME CONSTRAINT slack_messages_pkey TO slack_messages_old_pkey").execute(pool).await.ok();
    sqlx::query("ALTER TABLE slack_messages ADD PRIMARY KEY (channel_id, message_ts)")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE slack_messages ALTER COLUMN identity_id SET NOT NULL")
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
        let mut sql = String::from(
            "INSERT INTO slack_channels (anonymous_id, channel_id, name, is_archived, num_members) VALUES ",
        );
        sql.push_str(&placeholders(chunk.len(), 5));
        sql.push_str(
            " ON CONFLICT (channel_id) DO UPDATE SET name = EXCLUDED.name, is_archived = EXCLUDED.is_archived, num_members = EXCLUDED.num_members",
        );
        let mut q = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        for ch in chunk {
            q = q
                .bind(crate::base36::encode(ch.channel_id.as_bytes()))
                .bind(&ch.channel_id)
                .bind(&ch.name)
                .bind(ch.is_archived as i16)
                .bind(ch.num_members as i64);
        }
        q.execute(pool).await?;
    }
    tracing::info!("Inserted {} new channels into Postgres", count);
    Ok(count)
}

/// Upserts Slack users, refreshing profile fields on conflict.
/// Used by the scraper's `users.list` sync and the app's `team_join` handler.
pub async fn upsert_users(
    pool: &PgPool,
    users: &[SlackUserRow],
) -> Result<(), Box<dyn std::error::Error>> {
    if users.is_empty() {
        return Ok(());
    }
    for chunk in users.chunks(INSERT_CHUNK) {
        let mut sql = String::from(
            "INSERT INTO users (user_id, anonymous_id, merged_name, display_name, real_name, username, email, title, status_text, status_emoji, tz, tz_label, locale, pfp, updated, is_bot, is_deleted, is_admin, is_owner, is_restricted, is_app_user) VALUES ",
        );
        sql.push_str(&placeholders(chunk.len(), 21));
        sql.push_str(
            " ON CONFLICT (user_id) DO UPDATE SET anonymous_id = EXCLUDED.anonymous_id, merged_name = EXCLUDED.merged_name, display_name = EXCLUDED.display_name, real_name = EXCLUDED.real_name, username = EXCLUDED.username, email = EXCLUDED.email, title = EXCLUDED.title, status_text = EXCLUDED.status_text, status_emoji = EXCLUDED.status_emoji, tz = EXCLUDED.tz, tz_label = EXCLUDED.tz_label, locale = EXCLUDED.locale, pfp = EXCLUDED.pfp, updated = EXCLUDED.updated, is_bot = EXCLUDED.is_bot, is_deleted = EXCLUDED.is_deleted, is_admin = EXCLUDED.is_admin, is_owner = EXCLUDED.is_owner, is_restricted = EXCLUDED.is_restricted, is_app_user = EXCLUDED.is_app_user",
        );
        let mut q = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        for u in chunk {
            q = q
                .bind(&u.user_id)
                .bind(crate::base36::encode(u.user_id.as_bytes()))
                .bind(&u.merged_name)
                .bind(&u.display_name)
                .bind(&u.real_name)
                .bind(&u.username)
                .bind(&u.email)
                .bind(&u.title)
                .bind(&u.status_text)
                .bind(&u.status_emoji)
                .bind(&u.tz)
                .bind(&u.tz_label)
                .bind(&u.locale)
                .bind(&u.pfp)
                .bind(u.updated as i64)
                .bind(u.is_bot as i16)
                .bind(u.is_deleted as i16)
                .bind(u.is_admin as i16)
                .bind(u.is_owner as i16)
                .bind(u.is_restricted as i16)
                .bind(u.is_app_user as i16);
        }
        q.execute(pool).await?;
    }
    Ok(())
}
