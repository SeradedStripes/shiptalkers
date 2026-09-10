use std::collections::HashMap;

use crate::sqlx::PgPool;
use futures_util::stream::{self, StreamExt};

use ship_talkers_lib::hackatime;
pub use ship_talkers_lib::hackatime::{SyncFailure, sync_coding_activity};

const NO_ACCOUNT_RETRY_DAYS: u64 = 30;

/// 30m resync pass: sync every user via token or public API, recording private/no_account states.
pub async fn resync_all(pool: &PgPool, http: &reqwest::Client) {
    let user_ids = match hackatime::get_coding_user_ids(pool).await {
        Ok(ids) => ids,
        Err(e) => {
            tracing::error!("Failed to list users for hackatime resync: {}", e);
            return;
        }
    };
    let connections = match hackatime::get_hackatime_connections(pool).await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("Failed to list hackatime connections: {}", e);
            return;
        }
    };
    let conns: HashMap<String, hackatime::HackatimeConnectionRow> = connections
        .into_iter()
        .map(|c| (c.slack_id.clone(), c))
        .collect();
    let retry_cutoff = hackatime::date_days_ago(NO_ACCOUNT_RETRY_DAYS);

    let total_users = user_ids.len() as u64;

    let work: Vec<(String, Option<String>)> = user_ids
        .into_iter()
        .filter_map(|user_id| {
            let conn = conns.get(&user_id);
            if let Some(c) = conn {
                if c.status == "no_account" {
                    let probed = c.last_synced_date.as_deref().unwrap_or("");
                    if probed >= retry_cutoff.as_str() {
                        return None;
                    }
                }
                if c.status == "private" && c.access_token.is_empty() {
                    return None;
                }
            }
            let token = conn.and_then(|c| {
                if c.access_token.is_empty() {
                    None
                } else {
                    Some(c.access_token.clone())
                }
            });
            Some((user_id, token))
        })
        .collect();

    let skipped = total_users - work.len() as u64;
    tracing::info!(
        "hackatime resync pass starting: {} users to sync, {} skipped (no account / private)",
        work.len(),
        skipped
    );
    let pc = pool.clone();
    let hc = http.clone();
    let results: Vec<_> = stream::iter(work)
        .map(|(user_id, token)| {
            let ch = pc.clone();
            let hc = hc.clone();
            async move {
                let result = sync_coding_activity(&ch, &hc, &user_id, token.as_deref()).await;
                (user_id, result)
            }
        })
        .buffer_unordered(8)
        .collect()
        .await;

    let mut synced = 0u64;
    for (user_id, result) in results {
        match result {
            Ok(()) => synced += 1,
            Err(SyncFailure::PrivateProfile) => {
                record_hackatime_status(pool, &user_id, "private").await;
                tracing::info!("{} has a private hackatime profile, needs OAuth", user_id);
            }
            Err(SyncFailure::NoAccount) => {
                record_hackatime_status(pool, &user_id, "no_account").await;
                tracing::debug!("{} has no hackatime account", user_id);
            }
            Err(SyncFailure::Message(e)) => {
                tracing::warn!("Coding sync failed for {}: {}", user_id, e);
            }
        }
    }
    tracing::info!(
        "hackatime resync pass done: {} synced, {} skipped (no account / private)",
        synced,
        skipped
    );
}

/// Records why a user cannot be synced so the resync loop skips them.
async fn record_hackatime_status(pool: &PgPool, slack_id: &str, status: &str) {
    if let Err(e) = hackatime::update_hackatime_connection(
        pool,
        &hackatime::HackatimeConnectionRow {
            slack_id: slack_id.to_string(),
            access_token: String::new(),
            last_synced_date: Some(hackatime::today_utc()),
            status: status.to_string(),
            total_minutes: 0,
        },
    )
    .await
    {
        tracing::warn!("Failed to record hackatime status {status} for {slack_id}: {e}");
    }
}
