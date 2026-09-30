use crate::sqlx;
use crate::sqlx::PgPool;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rand::Rng;
use rand::rng;
use serde::Serialize;
use sha2::{Digest, Sha256};

pub use ship_talkers_lib::db::{
    BlacklistedChannelRow, SlackChannelRow, SlackUserRow, blacklist_channel, connect,
    get_blacklisted_channel_ids, insert_new_channels_rows, migrate, placeholders,
    unblacklist_channel, upsert_users,
};

pub async fn record_admin_audit_log(
    pool: &PgPool,
    target_type: &str,
    target_id: &str,
    action: &str,
    actor_slack_id: &str,
    data_deleted: bool,
) -> Result<(), String> {
    sqlx::query(
        "INSERT INTO admin_audit_log
             (target_type, target_id, action, actor_slack_id, data_deleted)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(target_type)
    .bind(target_id)
    .bind(action)
    .bind(actor_slack_id)
    .bind(data_deleted)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|e| e.to_string())
}

pub async fn lock_blacklisted_user_consents(pool: &PgPool) -> Result<(), String> {
    let user_ids: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT target_id
         FROM admin_audit_log
         WHERE target_type = 'user'
           AND action = 'blacklist_and_disable_consent'
           AND data_deleted",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| e.to_string())?;
    for user_id in user_ids {
        opt_out_slack_user(pool, &user_id).await?;
    }
    Ok(())
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

pub async fn opt_in_slack_user(
    pool: &PgPool,
    slack_user_id: &str,
    source: &str,
) -> Result<bool, String> {
    let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
    let permanently_revoked: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM slack_user_consents
         WHERE slack_user_id = $1 AND opt_out_locked",
    )
    .bind(slack_user_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| e.to_string())?;
    if permanently_revoked.is_some() {
        return Err("This user has opted out permanently and cannot opt in again".into());
    }
    let was_active: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM slack_user_consents
         WHERE slack_user_id = $1 AND revoked_at IS NULL",
    )
    .bind(slack_user_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| e.to_string())?;
    sqlx::query(
        "INSERT INTO slack_user_consents (slack_user_id, ship_talkers_id, consent_source)
         VALUES ($1, $3, $2)
         ON CONFLICT (slack_user_id) DO UPDATE SET consented_at = now(),
         revoked_at = NULL, manual_revoked_at = NULL,
         consent_source = EXCLUDED.consent_source,
         backfill_started_at = NULL, backfill_completed_at = NULL",
    )
    .bind(slack_user_id)
    .bind(source)
    .bind(ship_talkers_lib::base36::encode(slack_user_id.as_bytes()))
    .execute(&mut *tx)
    .await
    .map_err(|e| e.to_string())?;
    if was_active.is_none() {
        let ship_talkers_id = ship_talkers_lib::base36::encode(slack_user_id.as_bytes());
        sqlx::query("DELETE FROM user_scores WHERE user_id = $1")
            .bind(slack_user_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query("DELETE FROM channel_scores")
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        tracing::info!(
            user = slack_user_id,
            ship_talkers_id,
            "queued consent backfill"
        );
    }
    tx.commit().await.map_err(|e| e.to_string())?;
    Ok(was_active.is_none())
}

pub async fn opt_out_slack_user(pool: &PgPool, slack_user_id: &str) -> Result<bool, String> {
    let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
    sqlx::query(
        "INSERT INTO slack_user_consents
             (slack_user_id, ship_talkers_id, consent_source, oauth_active)
         VALUES ($1, $2, 'manual_opt_out', false)
         ON CONFLICT (slack_user_id) DO NOTHING",
    )
    .bind(slack_user_id)
    .bind(ship_talkers_lib::base36::encode(slack_user_id.as_bytes()))
    .execute(&mut *tx)
    .await
    .map_err(|e| e.to_string())?;
    let result = sqlx::query(
        "UPDATE slack_user_consents
         SET revoked_at = now(), manual_revoked_at = now(),
             opt_out_locked = true,
             backfill_started_at = NULL,
             backfill_completed_at = NULL
         WHERE slack_user_id = $1 AND revoked_at IS NULL",
    )
    .bind(slack_user_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| e.to_string())?;
    if result.rows_affected() > 0 {
        sqlx::query(
            "DELETE FROM slack_reactions r
             USING slack_messages m
             JOIN slack_identities i ON i.internal_id = m.identity_id
             JOIN slack_channels c ON c.internal_id = m.channel_id
             WHERE r.channel_id = c.channel_id
               AND r.message_ts = m.message_ts
               AND i.anonymous_id = $1",
        )
        .bind(slack_user_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
        sqlx::query(
            "DELETE FROM slack_messages m
             USING slack_identities i
             WHERE i.internal_id = m.identity_id AND i.anonymous_id = $1",
        )
        .bind(slack_user_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
        sqlx::query("DELETE FROM slack_identities WHERE anonymous_id = $1")
            .bind(slack_user_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        for (table, column) in [
            ("slack_reactions", "user_id"),
            ("word_counts", "user_id"),
            ("hackatime_spans", "slack_id"),
            ("hackatime_connections", "slack_id"),
            ("slack_oauth_channel_access", "slack_id"),
            ("slack_oauth_tokens", "slack_id"),
            ("linked_users", "slack_id"),
            ("api_keys", "slack_id"),
        ] {
            let query = format!("DELETE FROM {table} WHERE {column} = $1");
            sqlx::query(sqlx::AssertSqlSafe(query.as_str()))
                .bind(slack_user_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| e.to_string())?;
        }
        sqlx::query("DELETE FROM api_key_grants WHERE grantor_id = $1")
            .bind(slack_user_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query("DELETE FROM users WHERE user_id = $1")
            .bind(slack_user_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query("DELETE FROM user_scores WHERE user_id = $1")
            .bind(slack_user_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query("DELETE FROM channel_scores")
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query("DELETE FROM word_totals")
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query(
            "UPDATE message_count SET total = (SELECT count(*) FROM slack_messages) WHERE id = 1",
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
    }
    tx.commit().await.map_err(|e| e.to_string())?;
    Ok(result.rows_affected() > 0)
}

pub async fn mark_slack_oauth_active(pool: &PgPool, slack_user_id: &str) -> Result<(), String> {
    let ship_talkers_id = ship_talkers_lib::base36::encode(slack_user_id.as_bytes());
    sqlx::query(
        "INSERT INTO slack_user_consents
             (slack_user_id, ship_talkers_id, consent_source, oauth_active)
         VALUES ($1, $2, 'slack_oauth', true)
         ON CONFLICT (slack_user_id) DO UPDATE SET
             oauth_active = true,
             revoked_at = CASE WHEN slack_user_consents.opt_out_locked
                               THEN slack_user_consents.revoked_at ELSE NULL END",
    )
    .bind(slack_user_id)
    .bind(ship_talkers_id)
    .execute(pool)
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
}

pub async fn revoke_slack_oauth_consent(pool: &PgPool, slack_user_id: &str) -> Result<(), String> {
    let result = sqlx::query(
        "UPDATE slack_user_consents
         SET oauth_active = false,
             revoked_at = CASE
                 WHEN manual_revoked_at IS NOT NULL THEN manual_revoked_at
                 WHEN consent_source = 'slack_oauth' THEN now()
                 ELSE revoked_at
             END
         WHERE slack_user_id = $1 AND oauth_active",
    )
    .bind(slack_user_id)
    .execute(pool)
    .await
    .map_err(|e| e.to_string())?;
    if result.rows_affected() > 0 {
        sqlx::query("DELETE FROM user_scores WHERE user_id = $1")
            .bind(slack_user_id)
            .execute(pool)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query("DELETE FROM channel_scores")
            .execute(pool)
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

pub async fn sync_active_slack_oauth_consents(pool: &PgPool) -> Result<(), String> {
    let slack_ids: Vec<String> =
        sqlx::query_scalar("SELECT slack_id FROM slack_oauth_tokens WHERE disabled_at IS NULL")
            .fetch_all(pool)
            .await
            .map_err(|e| e.to_string())?;
    for slack_id in slack_ids {
        mark_slack_oauth_active(pool, &slack_id).await?;
    }
    Ok(())
}

pub async fn is_slack_user_consented(pool: &PgPool, slack_user_id: &str) -> bool {
    sqlx::query_scalar::<_, i32>(
        "SELECT 1 FROM slack_user_consents
         WHERE slack_user_id = $1 AND revoked_at IS NULL",
    )
    .bind(slack_user_id)
    .fetch_optional(pool)
    .await
    .is_ok_and(|row| row.is_some())
}

pub async fn backfill_main_channel_consents(
    pool: &PgPool,
    channel_id: &str,
) -> Result<u64, String> {
    let result = sqlx::query(
        "INSERT INTO slack_user_consents
             (slack_user_id, ship_talkers_id, consent_source)
         SELECT DISTINCT u.user_id, u.ship_talkers_id, 'main_channel_backfill'
         FROM slack_messages m
         JOIN slack_identities i ON i.internal_id = m.identity_id
         JOIN users u ON u.ship_talkers_id = i.ship_talkers_id
         JOIN slack_channels c ON c.internal_id = m.channel_id
         WHERE c.channel_id = $1 AND u.is_bot = 0
         ON CONFLICT (slack_user_id) DO NOTHING",
    )
    .bind(channel_id)
    .execute(pool)
    .await
    .map_err(|e| e.to_string())?;
    let count = result.rows_affected();
    if count > 0 {
        sqlx::query("DELETE FROM user_scores WHERE user_id IN (SELECT slack_user_id FROM slack_user_consents WHERE consent_source = 'main_channel_backfill' AND revoked_at IS NULL)")
            .execute(pool)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query("DELETE FROM channel_scores")
            .execute(pool)
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(count)
}

/// Linked-user state backing OAuth sign-in and hackatime linking.
#[derive(Clone)]
pub struct AuthDb {
    pool: PgPool,
}

impl AuthDb {
    pub async fn is_admin_user(&self, slack_id: &str) -> Result<bool, String> {
        sqlx::query_scalar::<_, i32>("SELECT 1 FROM admin_users WHERE slack_id = $1")
            .bind(slack_id)
            .fetch_optional(&self.pool)
            .await
            .map(|row| row.is_some())
            .map_err(|e| e.to_string())
    }

    pub async fn list_admin_users(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<(String, String, String)>, String> {
        sqlx::query_as(
            "SELECT a.slack_id, COALESCE(u.username, ''), a.added_by
             FROM admin_users a
             LEFT JOIN users u ON u.user_id = a.slack_id
             ORDER BY a.slack_id LIMIT $1 OFFSET $2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())
    }

    pub async fn count_admin_users(&self) -> Result<i64, String> {
        sqlx::query_scalar("SELECT count(*) FROM admin_users")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn add_admin_user(&self, slack_id: &str, added_by: &str) -> Result<(), String> {
        sqlx::query(
            "INSERT INTO admin_users (slack_id, added_by) VALUES ($1, $2)
             ON CONFLICT (slack_id) DO UPDATE SET added_by = EXCLUDED.added_by",
        )
        .bind(slack_id)
        .bind(added_by)
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
    }

    pub async fn remove_admin_user(&self, slack_id: &str) -> Result<(), String> {
        sqlx::query("DELETE FROM admin_users WHERE slack_id = $1")
            .bind(slack_id)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

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

    pub async fn has_slack_oauth_token(&self, slack_id: &str) -> bool {
        sqlx::query_scalar::<_, i32>(
            "SELECT 1 FROM slack_oauth_tokens WHERE slack_id = $1 AND disabled_at IS NULL",
        )
        .bind(slack_id)
        .fetch_optional(&self.pool)
        .await
        .is_ok_and(|row| row.is_some())
    }

    pub async fn is_slack_oauth_blacklisted(&self, slack_id: &str) -> Result<bool, String> {
        sqlx::query_scalar::<_, i32>(
            "SELECT 1 FROM blacklisted_slack_users WHERE slack_user_id = $1",
        )
        .bind(slack_id)
        .fetch_optional(&self.pool)
        .await
        .map(|row| row.is_some())
        .map_err(|e| e.to_string())
    }

    pub async fn list_blacklisted_slack_users(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<(String, String, String)>, String> {
        sqlx::query_as(
            "SELECT b.slack_user_id, COALESCE(u.merged_name, ''),
                    CASE
                        WHEN c.slack_user_id IS NULL THEN 'None'
                        WHEN c.revoked_at IS NULL THEN 'Active'
                        ELSE 'Revoked'
                    END
             FROM blacklisted_slack_users b
             LEFT JOIN users u ON u.user_id = b.slack_user_id
             LEFT JOIN slack_user_consents c ON c.slack_user_id = b.slack_user_id
             ORDER BY b.slack_user_id LIMIT $1 OFFSET $2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())
    }

    pub async fn count_blacklisted_slack_users(&self) -> Result<i64, String> {
        sqlx::query_scalar("SELECT count(*) FROM blacklisted_slack_users")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn blacklist_slack_user(
        &self,
        slack_id: &str,
        disable_consent: bool,
    ) -> Result<(), String> {
        sqlx::query(
            "INSERT INTO blacklisted_slack_users (slack_user_id) VALUES ($1)
             ON CONFLICT (slack_user_id) DO NOTHING",
        )
        .bind(slack_id)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        self.disable_slack_oauth_token_only(slack_id).await?;
        if disable_consent {
            opt_out_slack_user(&self.pool, slack_id).await?;
        }
        Ok(())
    }

    pub async fn unblacklist_slack_user(&self, slack_id: &str) -> Result<(), String> {
        sqlx::query("DELETE FROM blacklisted_slack_users WHERE slack_user_id = $1")
            .bind(slack_id)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn disable_slack_oauth_token_only(&self, slack_id: &str) -> Result<(), String> {
        sqlx::query(
            "UPDATE slack_oauth_tokens SET disabled_at = now(), updated_at = now()
             WHERE slack_id = $1",
        )
        .bind(slack_id)
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
    }

    pub async fn upsert_slack_oauth_token(
        &self,
        slack_id: &str,
        team_id: &str,
        access_token: &str,
        scopes: &str,
    ) -> Result<(), String> {
        sqlx::query(
            "INSERT INTO slack_oauth_tokens (slack_id, team_id, access_token, scopes)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (slack_id) DO UPDATE SET team_id = EXCLUDED.team_id,
             access_token = EXCLUDED.access_token, scopes = EXCLUDED.scopes,
             updated_at = now(), disabled_at = NULL",
        )
        .bind(slack_id)
        .bind(team_id)
        .bind(access_token)
        .bind(scopes)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        mark_slack_oauth_active(&self.pool, slack_id).await
    }

    pub async fn disable_slack_oauth_token(&self, slack_id: &str) -> Result<(), String> {
        sqlx::query(
            "UPDATE slack_oauth_tokens SET disabled_at = now(), updated_at = now()
             WHERE slack_id = $1",
        )
        .bind(slack_id)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        revoke_slack_oauth_consent(&self.pool, slack_id).await
    }
}

/// A stored API key's public metadata. The secret itself is never persisted.
#[derive(Clone, Serialize)]
pub struct ApiKeyRow {
    pub key_id: String,
    pub created_at: i64,
    pub last_used_at: Option<i64>,
}

impl AuthDb {
    pub async fn create_api_key(
        &self,
        slack_id: &str,
        created_at: i64,
    ) -> Result<(String, String), String> {
        let mut secret_bytes = [0u8; 32];
        rng().fill_bytes(&mut secret_bytes);
        let key = format!("shiptalkers_{}", URL_SAFE_NO_PAD.encode(secret_bytes));
        let key_id = format!("key_{}", URL_SAFE_NO_PAD.encode(&secret_bytes[..6]));
        let key_hash = sha256_hex(&key);
        sqlx::query(
            "INSERT INTO api_keys (key_id, slack_id, key_hash, created_at)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(&key_id)
        .bind(slack_id)
        .bind(key_hash)
        .bind(created_at)
        .execute(&self.pool)
        .await
        .map(|_| (key, key_id))
        .map_err(|e| e.to_string())
    }

    pub async fn list_api_keys(&self, slack_id: &str) -> Result<Vec<ApiKeyRow>, String> {
        sqlx::query_as::<_, (String, i64, Option<i64>)>(
            "SELECT key_id, created_at, last_used_at
             FROM api_keys WHERE slack_id = $1 ORDER BY created_at DESC",
        )
        .bind(slack_id)
        .fetch_all(&self.pool)
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|(key_id, created_at, last_used_at)| ApiKeyRow {
                    key_id,
                    created_at,
                    last_used_at,
                })
                .collect()
        })
        .map_err(|e| e.to_string())
    }

    pub async fn revoke_api_key(&self, slack_id: &str, key_id: &str) -> Result<bool, String> {
        sqlx::query("DELETE FROM api_keys WHERE slack_id = $1 AND key_id = $2")
            .bind(slack_id)
            .bind(key_id)
            .execute(&self.pool)
            .await
            .map(|result| result.rows_affected() > 0)
            .map_err(|e| e.to_string())
    }

    /// Looks up the owner of a full API key by its hash, touching `last_used_at` on success.
    pub async fn slack_id_for_key(&self, key: &str) -> Result<Option<String>, String> {
        self.resolve_key(key)
            .await
            .map(|row| row.map(|(_, slack_id)| slack_id))
    }

    /// Resolves a full API key to its `(key_id, owner_slack_id)`, touching `last_used_at` on success.
    pub async fn resolve_key(&self, key: &str) -> Result<Option<(String, String)>, String> {
        let row: Option<(String, String)> = sqlx::query_as(
            "UPDATE api_keys SET last_used_at = $2
             WHERE key_hash = $1 RETURNING key_id, slack_id",
        )
        .bind(sha256_hex(key))
        .bind(time::OffsetDateTime::now_utc().unix_timestamp())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(row)
    }
}

/// A stored grant's public metadata: which key the grantor opened their data to.
#[derive(Clone, Serialize)]
pub struct ApiGrantRow {
    pub key_id: String,
    pub created_at: i64,
}

impl AuthDb {
    /// Grants the grantor's data to `key_id`, creating the row when the key
    /// exists. Refreshing the timestamp for an existing grant is a no-op.
    /// Returns false when the key does not exist.
    pub async fn create_grant(
        &self,
        grantor_id: &str,
        key_id: &str,
        created_at: i64,
    ) -> Result<bool, String> {
        sqlx::query(
            "INSERT INTO api_key_grants (grantor_id, key_id, created_at)
             SELECT $1, key_id, $3 FROM api_keys WHERE key_id = $2
             ON CONFLICT (grantor_id, key_id) DO UPDATE SET created_at = EXCLUDED.created_at",
        )
        .bind(grantor_id)
        .bind(key_id)
        .bind(created_at)
        .execute(&self.pool)
        .await
        .map(|result| result.rows_affected() > 0)
        .map_err(|e| e.to_string())
    }

    pub async fn revoke_grant(&self, grantor_id: &str, key_id: &str) -> Result<bool, String> {
        sqlx::query("DELETE FROM api_key_grants WHERE grantor_id = $1 AND key_id = $2")
            .bind(grantor_id)
            .bind(key_id)
            .execute(&self.pool)
            .await
            .map(|result| result.rows_affected() > 0)
            .map_err(|e| e.to_string())
    }

    pub async fn list_grants(&self, grantor_id: &str) -> Result<Vec<ApiGrantRow>, String> {
        sqlx::query_as::<_, (String, i64)>(
            "SELECT key_id, created_at FROM api_key_grants
             WHERE grantor_id = $1 ORDER BY created_at DESC",
        )
        .bind(grantor_id)
        .fetch_all(&self.pool)
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|(key_id, created_at)| ApiGrantRow { key_id, created_at })
                .collect()
        })
        .map_err(|e| e.to_string())
    }

    /// Users who granted this key access to their data, newest first.
    pub async fn granted_users(&self, key_id: &str) -> Result<Vec<(String, i64)>, String> {
        sqlx::query_as(
            "SELECT grantor_id, created_at FROM api_key_grants
             WHERE key_id = $1 ORDER BY created_at DESC",
        )
        .bind(key_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())
    }

    pub async fn has_grant(&self, grantor_id: &str, key_id: &str) -> Result<bool, String> {
        sqlx::query_scalar::<_, i32>(
            "SELECT 1 FROM api_key_grants WHERE grantor_id = $1 AND key_id = $2",
        )
        .bind(grantor_id)
        .bind(key_id)
        .fetch_optional(&self.pool)
        .await
        .map(|row| row.is_some())
        .map_err(|e| e.to_string())
    }
}

fn sha256_hex(input: &str) -> String {
    let hash = Sha256::digest(input.as_bytes());
    hash.iter().map(|b| format!("{b:02x}")).collect()
}
