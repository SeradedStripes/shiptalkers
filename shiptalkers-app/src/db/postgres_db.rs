use sqlx::PgPool;

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
