use std::collections::HashMap;

use crate::sqlx::PgPool;
use ship_talkers_lib::hackatime;
pub use ship_talkers_lib::hackatime::{SyncFailure, sync_coding_activity};

const STATUS_RETRY_DAYS: u64 = 30;
const REQUEST_DELAY_MS: u64 = 1000;

fn needs_resync(last_synced_date: Option<&str>, today: &str) -> bool {
    last_synced_date != Some(today)
}

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
    let retry_cutoff = hackatime::date_days_ago(STATUS_RETRY_DAYS);
    let today = hackatime::today_utc();

    let total_users = user_ids.len() as u64;

    let work: Vec<(String, Option<String>)> = user_ids
        .into_iter()
        .filter_map(|user_id| {
            let conn = conns.get(&user_id);
            if let Some(c) = conn {
                if matches!(c.status.as_str(), "no_account" | "private")
                    && c.access_token.is_empty()
                {
                    let probed = c.last_synced_date.as_deref().unwrap_or("");
                    if probed >= retry_cutoff.as_str() {
                        return None;
                    }
                }
                if !needs_resync(c.last_synced_date.as_deref(), &today) {
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
    let mut synced = 0u64;
    for (user_id, token) in work {
        let result = sync_coding_activity(pool, http, &user_id, token.as_deref()).await;
        tokio::time::sleep(std::time::Duration::from_millis(REQUEST_DELAY_MS)).await;
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
            Err(SyncFailure::RateLimited) => {
                tracing::warn!("Hackatime rate limit reached, stopping this resync pass");
                break;
            }
            Err(SyncFailure::BudgetExhausted) => {
                tracing::warn!(
                    "Hackatime daily request budget exhausted, stopping this resync pass"
                );
                break;
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

#[cfg(test)]
mod tests {
    use super::needs_resync;

    #[test]
    fn resync_skips_users_synced_today() {
        assert!(!needs_resync(Some("2026-09-15"), "2026-09-15"));
        assert!(needs_resync(Some("2026-09-14"), "2026-09-15"));
        assert!(needs_resync(None, "2026-09-15"));
    }
}
