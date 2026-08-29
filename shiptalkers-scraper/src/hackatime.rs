use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use futures_util::stream::{self, StreamExt};
use sqlx::PgPool;

use ship_talkers_lib::hackatime;

const START_DATE: &str = "2024-01-01";
const NO_ACCOUNT_RETRY_DAYS: u64 = 30;

/// Per-user lock so a link-time sync and the resync_all pass never write the same user's connection row at the same time.
static CODING_SYNC_LOCKS: OnceLock<std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> =
    OnceLock::new();

fn coding_sync_lock(slack_id: &str) -> Arc<tokio::sync::Mutex<()>> {
    let locks = CODING_SYNC_LOCKS.get_or_init(|| std::sync::Mutex::new(HashMap::new()));
    let mut guard = locks.lock().unwrap_or_else(|p| p.into_inner());
    guard
        .entry(slack_id.to_string())
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

/// Why a coding sync could not complete. `PrivateProfile` and `NoAccount` are permanent given the current account state (the resync loop records them so it does not retry every cycle); `Message` is transient.
#[derive(Debug)]
pub enum SyncFailure {
    /// Public stats are disabled for this user and there is no token to fall back on
    PrivateProfile,
    /// No hackatime account exists for this Slack UID.
    NoAccount,
    /// Transient failure (rate limit, outage, bad response, etc...).
    Message(String),
}

impl std::fmt::Display for SyncFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncFailure::PrivateProfile => write!(f, "profile is not public"),
            SyncFailure::NoAccount => write!(f, "no hackatime account"),
            SyncFailure::Message(m) => write!(f, "{m}"),
        }
    }
}

/// Fetches one user's coding spans from hackatime and stores them in
/// `hackatime_spans`, then rewrites the connection's `total_minutes` from the
/// span sum. With a token the spans endpoint is called authenticated (so
/// private profiles of the token's owner work too); without one, the public
/// API (keyed by Slack UID) is used, which only works for public profiles.
/// The fetch runs before any DB write, so a failed sync leaves the old data
/// intact. A user with no `hackatime_spans` rows yet (new, or predating span
/// storage) backfills everything since `START_DATE`; otherwise the window
/// starts one day before the last sync (covering in-flight sessions) and ends
/// tomorrow.
pub async fn sync_coding_activity(
    pool: &PgPool,
    http: &reqwest::Client,
    slack_id: &str,
    access_token: Option<&str>,
) -> Result<(), SyncFailure> {
    let lock = coding_sync_lock(slack_id);
    let _guard = lock.lock().await;
    let today = hackatime::today_utc();

    let conn = hackatime::get_hackatime_connection(pool, slack_id)
        .await
        .map_err(|e| SyncFailure::Message(format!("read hackatime connection: {e}")))?;
    let last_synced = conn
        .as_ref()
        .and_then(|c| c.last_synced_date.clone())
        .unwrap_or_default();
    let span_count = hackatime::get_hackatime_span_count(pool, slack_id)
        .await
        .map_err(|e| SyncFailure::Message(format!("read hackatime_spans count: {e}")))?;
    // No spans yet means the user is new or predates span storage (total-only
    // scheme), so backfill everything; otherwise refetch from one day before
    // the last sync so in-flight sessions crossing midnight are re-covered.
    let start_date = if span_count == 0 {
        START_DATE.to_string()
    } else {
        hackatime::date_plus_days(&last_synced, -1).unwrap_or_else(|| START_DATE.to_string())
    };
    let end_date = hackatime::days_from_now(1);

    let spans =
        match hackatime::fetch_coding_spans(http, slack_id, access_token, &start_date, &end_date)
            .await
        {
            Ok(s) => s,
            Err((status, message)) => match (access_token, status) {
                (Some(_), Some(401 | 403)) => {
                    // A 401/403 can mean a dead token or an outage in front of
                    // hackatime (maintenance, auth proxy). Confirm the token is
                    // really dead with the me endpoint before removing the link;
                    // otherwise keep it and let the next sync retry. Any other
                    // failure (HTTP 5xx, or no response at all) means hackatime
                    // is simply down, so the link is kept.
                    match hackatime::fetch_hackatime_me(http, access_token.unwrap()).await {
                        Err((Some(401 | 403), _)) => {
                            tracing::warn!(
                                "Hackatime token for {} is invalid, removing link",
                                slack_id
                            );
                            hackatime::delete_hackatime_connection(pool, slack_id)
                                .await
                                .map_err(|e| {
                                    SyncFailure::Message(format!(
                                        "delete stale hackatime connection: {}",
                                        e
                                    ))
                                })?;
                            return Ok(());
                        }
                        _ => {
                            return Err(SyncFailure::Message(format!(
                                "hackatime returned 401/403 but the me check did not confirm a \
                                 dead token (likely down), keeping link: {message}"
                            )));
                        }
                    }
                }
                (None, Some(403)) => return Err(SyncFailure::PrivateProfile),
                (None, Some(404)) => return Err(SyncFailure::NoAccount),
                (_, Some(code)) => {
                    return Err(SyncFailure::Message(format!(
                        "hackatime HTTP {code} (down, keeping data): {message}"
                    )));
                }
                (_, None) => {
                    return Err(SyncFailure::Message(format!(
                        "hackatime unreachable (down, keeping data): {message}"
                    )));
                }
            },
        };

    let updated = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let rows: Vec<hackatime::HackatimeSpanRow> = spans
        .iter()
        .map(|s| hackatime::HackatimeSpanRow {
            slack_id: slack_id.to_string(),
            start_ts: s.start_time as u64,
            duration: s.duration.round() as u64,
            updated,
        })
        .collect();
    hackatime::insert_hackatime_spans(pool, &rows)
        .await
        .map_err(|e| SyncFailure::Message(e.to_string()))?;

    let total_seconds = hackatime::get_hackatime_total_seconds(pool, slack_id)
        .await
        .map_err(|e| SyncFailure::Message(e.to_string()))?;
    let minutes = (total_seconds as f64 / 60.0).round() as u64;

    let conn = hackatime::HackatimeConnectionRow {
        slack_id: slack_id.to_string(),
        access_token: access_token.unwrap_or("").to_string(),
        last_synced_date: Some(today),
        status: String::new(),
        total_minutes: minutes,
    };
    hackatime::update_hackatime_connection(pool, &conn)
        .await
        .map_err(|e| SyncFailure::Message(e.to_string()))?;

    tracing::info!(
        "Synced {} coding spans, {} total minutes for {}",
        rows.len(),
        minutes,
        slack_id
    );
    Ok(())
}

/// Periodic hackatime resync over every user (every 30m). Users with an OAuth
/// connection sync through it; everyone else is fetched from the public stats
/// API by Slack UID. Public profiles that 404 (no account) or 403 (private,
/// no token to fall back on) are recorded so they are not retried every cycle.
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

/// Records why a public-only user cannot be synced (or that a user just
/// disappeared from hackatime) so the resync loop skips them until the state
/// changes.
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
