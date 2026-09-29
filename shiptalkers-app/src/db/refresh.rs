use crate::sqlx;
use crate::sqlx::PgPool;

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

const EXCLUDE_SCORE_BOTS_DELETED: &str =
    "user_id NOT IN (SELECT user_id FROM users WHERE is_bot = 1 OR is_deleted = 1)";

pub async fn refresh_page_stats(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let total_messages: i64 = sqlx::query_scalar("SELECT total FROM message_count WHERE id = 1")
        .fetch_one(pool)
        .await
        .unwrap_or(0);
    let total_channels: i64 = sqlx::query_scalar("SELECT count(*) FROM slack_channels")
        .fetch_one(pool)
        .await
        .unwrap_or(0);
    let archived_channels: i64 =
        sqlx::query_scalar("SELECT count(*) FROM slack_channels WHERE is_archived = 1")
            .fetch_one(pool)
            .await
            .unwrap_or(0);
    let total_users: i64 =
        sqlx::query_scalar("SELECT count(*) FROM users WHERE is_bot = 0 AND is_deleted = 0")
            .fetch_one(pool)
            .await
            .unwrap_or(0);
    let hackatime_users: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM hackatime_connections WHERE status != 'no_account'",
    )
    .fetch_one(pool)
    .await
    .unwrap_or(0);
    let private_hackatime_users: i64 =
        sqlx::query_scalar("SELECT count(*) FROM hackatime_connections WHERE status = 'private'")
            .fetch_one(pool)
            .await
            .unwrap_or(0);
    let no_hackatime_account_users: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM hackatime_connections WHERE status = 'no_account'",
    )
    .fetch_one(pool)
    .await
    .unwrap_or(0);
    let coding_minutes: i64 =
        sqlx::query_scalar("SELECT sum(total_minutes)::bigint FROM hackatime_connections")
            .fetch_one(pool)
            .await
            .ok()
            .flatten()
            .unwrap_or(0);
    let slack_time_secs: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT sum(total_time)::bigint FROM user_scores WHERE {EXCLUDE_SCORE_BOTS_DELETED}"
    )))
    .fetch_one(pool)
    .await
    .ok()
    .flatten()
    .unwrap_or(0);
    let db_size_bytes: i64 = sqlx::query_scalar("SELECT pg_database_size(current_database())")
        .fetch_one(pool)
        .await
        .unwrap_or(0);
    let opted_in_users: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM users u
         JOIN slack_user_consents c ON c.slack_user_id = u.user_id
         WHERE c.revoked_at IS NULL",
    )
    .fetch_one(pool)
    .await
    .unwrap_or(0);
    let non_opted_in_users: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM users u
         WHERE NOT EXISTS (
               SELECT 1 FROM slack_user_consents c
               WHERE c.slack_user_id = u.user_id AND c.revoked_at IS NULL
           )",
    )
    .fetch_one(pool)
    .await
    .unwrap_or(0);

    sqlx::query(
        "INSERT INTO stats_meta (id, total_messages, total_channels, archived_channels, total_users, hackatime_users, private_hackatime_users, no_hackatime_account_users, coding_minutes, slack_time_secs, db_size_bytes, opted_in_users, non_opted_in_users, updated)
         VALUES (1, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
         ON CONFLICT (id) DO UPDATE SET
           total_messages = EXCLUDED.total_messages,
           total_channels = EXCLUDED.total_channels,
           archived_channels = EXCLUDED.archived_channels,
           total_users = EXCLUDED.total_users,
           hackatime_users = EXCLUDED.hackatime_users,
           private_hackatime_users = EXCLUDED.private_hackatime_users,
           no_hackatime_account_users = EXCLUDED.no_hackatime_account_users,
           coding_minutes = EXCLUDED.coding_minutes,
           slack_time_secs = EXCLUDED.slack_time_secs,
           db_size_bytes = EXCLUDED.db_size_bytes,
           opted_in_users = EXCLUDED.opted_in_users,
           non_opted_in_users = EXCLUDED.non_opted_in_users,
           updated = EXCLUDED.updated",
    )
    .bind(total_messages.max(0))
    .bind(total_channels.max(0))
    .bind(archived_channels.max(0))
    .bind(total_users.max(0))
    .bind(hackatime_users.max(0))
    .bind(private_hackatime_users.max(0))
    .bind(no_hackatime_account_users.max(0))
    .bind(coding_minutes.max(0))
    .bind(slack_time_secs.max(0))
    .bind(db_size_bytes.max(0))
    .bind(opted_in_users.max(0))
    .bind(non_opted_in_users.max(0))
    .bind(now_secs() as i64)
    .execute(pool)
    .await?;
    Ok(())
}
