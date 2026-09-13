use crate::sqlx;
use crate::sqlx::PgPool;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rand::Rng;
use rand::rng;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;

pub use ship_talkers_lib::db::{
    SlackChannelRow, SlackUserRow, connect, insert_new_channels_rows, migrate, placeholders,
    upsert_users,
};

pub async fn grant_slack_consent(pool: &PgPool, slack_id: &str) -> Result<(), String> {
    let anonymous_id = ship_talkers_lib::base36::encode(slack_id.as_bytes());
    sqlx::query(
        "WITH identity AS (
             INSERT INTO slack_identities (anonymous_id) VALUES ($1)
             ON CONFLICT (anonymous_id) DO UPDATE SET anonymous_id = EXCLUDED.anonymous_id
             RETURNING internal_id
         )
         INSERT INTO slack_consents (identity_id, consent_source)
         SELECT internal_id, 'slack_channel' FROM identity
          ON CONFLICT (identity_id) DO UPDATE SET consented_at = NOW(), revoked_at = NULL, content_backfilled_at = NULL,
             consent_source = EXCLUDED.consent_source",
    )
    .bind(anonymous_id)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|e| e.to_string())
}

pub async fn slack_user_has_consent(pool: &PgPool, slack_id: &str) -> Result<bool, String> {
    let anonymous_id = ship_talkers_lib::base36::encode(slack_id.as_bytes());
    sqlx::query_scalar::<_, i32>(
        "SELECT 1 FROM slack_consents c
         JOIN slack_identities i ON i.internal_id = c.identity_id
         WHERE i.anonymous_id = $1 AND c.consented_at IS NOT NULL AND c.revoked_at IS NULL",
    )
    .bind(anonymous_id)
    .fetch_optional(pool)
    .await
    .map(|row| row.is_some())
    .map_err(|e| e.to_string())
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

/// Linked-user state backing OAuth sign-in and hackatime linking.
#[derive(Clone)]
pub struct AuthDb {
    pool: PgPool,
    consented: Arc<RwLock<HashSet<i32>>>,
}

impl AuthDb {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            consented: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    pub async fn load_consents(&self) -> Result<(), String> {
        let ids: Vec<i32> = sqlx::query_scalar(
            "SELECT identity_id FROM slack_consents WHERE consented_at IS NOT NULL AND revoked_at IS NULL",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        self.consented.write().await.extend(ids);
        Ok(())
    }

    pub async fn identity_is_consented(&self, identity_id: i32) -> bool {
        self.consented.read().await.contains(&identity_id)
    }

    async fn identity_id(&self, slack_id: &str) -> Result<Option<i32>, String> {
        let anonymous_id = ship_talkers_lib::base36::encode(slack_id.as_bytes());
        sqlx::query_scalar("SELECT internal_id FROM slack_identities WHERE anonymous_id = $1")
            .bind(anonymous_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn grant_consent(&self, slack_id: &str, source: Option<&str>) -> Result<(), String> {
        let identity_id = self
            .identity_id(slack_id)
            .await?
            .ok_or_else(|| "Slack identity has no stored locator".to_owned())?;
        sqlx::query(
            "INSERT INTO slack_consents (identity_id, consent_source) VALUES ($1, $2)
             ON CONFLICT (identity_id) DO UPDATE SET consented_at = NOW(), revoked_at = NULL, content_backfilled_at = NULL, consent_source = EXCLUDED.consent_source",
        )
        .bind(identity_id)
        .bind(source)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        self.consented.write().await.insert(identity_id);
        Ok(())
    }

    pub async fn revoke_consent(&self, slack_id: &str) -> Result<(), String> {
        let identity_id = self.identity_id(slack_id).await?;
        let Some(identity_id) = identity_id else {
            return Ok(());
        };
        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        sqlx::query("UPDATE slack_consents SET revoked_at = NOW() WHERE identity_id = $1 AND revoked_at IS NULL")
            .bind(identity_id).execute(&mut *tx).await.map_err(|e| e.to_string())?;
        sqlx::query(
            "DELETE FROM slack_message_contents c USING slack_messages m
             WHERE c.channel_id = m.channel_id AND c.message_ts = m.message_ts AND m.identity_id = $1",
        )
        .bind(identity_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
        sqlx::query(
            "DELETE FROM word_counts w USING users u, slack_identities i
             WHERE w.user_id = u.user_id AND u.anonymous_id = i.anonymous_id AND i.internal_id = $1",
        )
        .bind(identity_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
        sqlx::query("DELETE FROM word_totals")
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query("UPDATE word_refresh_meta SET watermark = 0 WHERE id = 1")
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        tx.commit().await.map_err(|e| e.to_string())?;
        self.consented.write().await.remove(&identity_id);
        Ok(())
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
