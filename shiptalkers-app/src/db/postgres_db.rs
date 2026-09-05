use crate::sqlx;
use crate::sqlx::PgPool;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rand::Rng;
use rand::rng;
use serde::Serialize;
use sha2::{Digest, Sha256};

pub use ship_talkers_lib::db::{
    SlackChannelRow, connect, init_tables, insert_new_channels_rows, placeholders,
};

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
        let row: Option<String> = sqlx::query_scalar(
            "UPDATE api_keys SET last_used_at = $2
             WHERE key_hash = $1 RETURNING slack_id",
        )
        .bind(sha256_hex(key))
        .bind(time::OffsetDateTime::now_utc().unix_timestamp())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(row)
    }
}

fn sha256_hex(input: &str) -> String {
    let hash = Sha256::digest(input.as_bytes());
    hash.iter().map(|b| format!("{b:02x}")).collect()
}
