use sqlx::PgPool;

pub const INSERT_CHUNK: usize = 5_000;

#[derive(Debug, Clone)]
pub struct SlackChannelRow {
    pub channel_id: String,
    pub name: String,
    pub is_private: u8,
    pub is_archived: u8,
    pub num_members: u64,
    pub created_at: u64,
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
    let max_connections = std::env::var("DATABASE_MAX_CONNECTIONS")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(3);
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(max_connections)
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

pub async fn locators_for_slack_user(
    pool: &PgPool,
    slack_user_id: &str,
) -> Result<Vec<(i32, i64)>, Box<dyn std::error::Error>> {
    let ship_talkers_id = crate::base36::encode(slack_user_id.as_bytes());
    Ok(sqlx::query_as(
        "SELECT m.channel_id, m.message_ts
         FROM slack_messages m
         JOIN slack_identities i ON i.internal_id = m.identity_id
         WHERE i.ship_talkers_id = $1
         ORDER BY m.message_ts",
    )
    .bind(ship_talkers_id)
    .fetch_all(pool)
    .await?)
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
            "INSERT INTO slack_channels (ship_talkers_id, channel_id, name, is_private, is_archived, num_members, created_at) VALUES ",
        );
        sql.push_str(&placeholders(chunk.len(), 7));
        sql.push_str(
            " ON CONFLICT (channel_id) DO UPDATE SET name = EXCLUDED.name, is_private = EXCLUDED.is_private, is_archived = EXCLUDED.is_archived, num_members = EXCLUDED.num_members, created_at = GREATEST(slack_channels.created_at, EXCLUDED.created_at)",
        );
        let mut q = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        for ch in chunk {
            q = q
                .bind(crate::base36::encode(ch.channel_id.as_bytes()))
                .bind(&ch.channel_id)
                .bind(&ch.name)
                .bind(ch.is_private as i16)
                .bind(ch.is_archived as i16)
                .bind(ch.num_members as i64)
                .bind(ch.created_at as i64);
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
    const USER_INSERT_CHUNK: usize = 3_000;
    for chunk in users.chunks(USER_INSERT_CHUNK) {
        let mut sql = String::from(
            "INSERT INTO users (user_id, ship_talkers_id, merged_name, display_name, real_name, username, email, title, status_text, status_emoji, tz, tz_label, locale, pfp, updated, is_bot, is_deleted, is_admin, is_owner, is_restricted, is_app_user) VALUES ",
        );
        sql.push_str(&placeholders(chunk.len(), 21));
        sql.push_str(
            " ON CONFLICT (user_id) DO UPDATE SET ship_talkers_id = EXCLUDED.ship_talkers_id, merged_name = EXCLUDED.merged_name, display_name = EXCLUDED.display_name, real_name = EXCLUDED.real_name, username = EXCLUDED.username, email = EXCLUDED.email, title = EXCLUDED.title, status_text = EXCLUDED.status_text, status_emoji = EXCLUDED.status_emoji, tz = EXCLUDED.tz, tz_label = EXCLUDED.tz_label, locale = EXCLUDED.locale, pfp = EXCLUDED.pfp, updated = EXCLUDED.updated, is_bot = EXCLUDED.is_bot, is_deleted = EXCLUDED.is_deleted, is_admin = EXCLUDED.is_admin, is_owner = EXCLUDED.is_owner, is_restricted = EXCLUDED.is_restricted, is_app_user = EXCLUDED.is_app_user",
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
