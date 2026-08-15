use crate::auth;
use crate::db::clickhouse_db;
use askama::Template;
use axum::extract::{Query, State};
use axum::http::header::{COOKIE, SET_COOKIE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse, Redirect};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use super::AppState;

const SESSION_COOKIE: &str = "st_session";
const STATE_COOKIE: &str = "st_state";
const START_DATE: &str = "2024-01-01";

/// Per-user lock so a link-time full sync and the resync_all pass never run
/// clear+insert for the same user at the same time. Concurrent syncs were
/// inserting duplicate rows into coding_activity.
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

fn cookies(headers: &HeaderMap) -> HashMap<String, String> {
    headers
        .get_all(COOKIE)
        .iter()
        .flat_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|kv| {
            let mut parts = kv.trim().splitn(2, '=');
            let k = parts.next()?.trim().to_string();
            let v = parts.next()?.trim().to_string();
            Some((k, v))
        })
        .collect()
}

fn set_cookie(key: &str, value: &str, max_age: Option<i64>) -> HeaderValue {
    let mut s = format!("{key}={value}; Path=/; HttpOnly; SameSite=Lax");
    if let Some(age) = max_age {
        s.push_str(&format!("; Max-Age={age}"));
    }
    HeaderValue::from_str(&s).expect("valid header value")
}

fn clear_cookie(key: &str) -> HeaderValue {
    set_cookie(key, "", Some(0))
}

fn auth_config(state: &AppState) -> crate::auth::AuthConfig {
    state.settings.auth_config()
}

pub(crate) fn session_from_request(
    headers: &HeaderMap,
    auth_config: &auth::AuthConfig,
) -> Option<auth::Session> {
    let cookies = cookies(headers);
    auth::parse_session(
        cookies.get(SESSION_COOKIE).map(String::as_str),
        &auth_config.session_secret,
    )
}

pub async fn get_link(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Html<String>, StatusCode> {
    let started = Instant::now();
    let session = session_from_request(&headers, &auth_config(&state));
    let hackatime_connected = match &session {
        Some(s) => clickhouse_db::is_hackatime_connected(&state.clickhouse, &s.slack_id)
            .await
            .unwrap_or(false),
        None => false,
    };
    let name = match &session {
        Some(s) => {
            let display_name: String = state
                .clickhouse
                .query("SELECT display_name FROM users FINAL WHERE user_id = ?")
                .bind(&s.slack_id)
                .fetch_one()
                .await
                .unwrap_or_default();
            if display_name.is_empty() {
                s.name.clone()
            } else {
                display_name
            }
        }
        None => String::new(),
    };
    let template = LinkTemplate {
        signed_in: session.is_some(),
        name,
        slack_id: session
            .as_ref()
            .map(|s| s.slack_id.clone())
            .unwrap_or_default(),
        hackatime_connected,
        page_load_ms: format!("{}ms", started.elapsed().as_millis()),
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

#[derive(Template)]
#[template(path = "link.html")]
struct LinkTemplate {
    signed_in: bool,
    name: String,
    slack_id: String,
    hackatime_connected: bool,
    page_load_ms: String,
}

pub async fn auth_hackclub_login(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, StatusCode> {
    let state_val = auth::random_state();
    let location = auth::hca_authorize_url(&auth_config(&state), &state_val);
    let mut response = Redirect::to(&location).into_response();
    response
        .headers_mut()
        .insert(SET_COOKIE, set_cookie(STATE_COOKIE, &state_val, Some(600)));
    Ok(response)
}

#[derive(Deserialize)]
pub struct HackclubCallbackParams {
    code: String,
    state: String,
}

pub async fn auth_hackclub_callback(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HackclubCallbackParams>,
) -> Result<impl IntoResponse, StatusCode> {
    let cookies = cookies(&headers);
    let expected_state = cookies
        .get(STATE_COOKIE)
        .cloned()
        .ok_or(StatusCode::BAD_REQUEST)?;
    if expected_state != params.state {
        return Err(StatusCode::BAD_REQUEST);
    }

    let token = auth::exchange_hca_code(&state.http, &auth_config(&state), &params.code)
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let identity = auth::fetch_hca_identity(&state.http, &token)
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let slack_id = identity.slack_id.ok_or(StatusCode::FORBIDDEN)?;
    let name = identity
        .first_name
        .or(identity.last_name)
        .unwrap_or_else(|| "Hacker".to_string());

    if let Err(e) = state.auth_db.mark_linked(&slack_id, &name).await {
        tracing::error!("Failed to record linked user {}: {}", slack_id, e);
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    let session = auth::Session { slack_id, name };
    let cookie = auth::issue_session(&session, auth_config(&state).session_secret.as_str());

    let mut response = Redirect::to("/link").into_response();
    response
        .headers_mut()
        .insert(SET_COOKIE, set_cookie(SESSION_COOKIE, &cookie, None));
    response
        .headers_mut()
        .append(SET_COOKIE, clear_cookie(STATE_COOKIE));
    Ok(response)
}

pub async fn auth_hackatime_login(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, StatusCode> {
    if session_from_request(&headers, &auth_config(&state)).is_none() {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let state_val = auth::random_state();
    let location = auth::hackatime_authorize_url(&auth_config(&state), &state_val);
    let mut response = Redirect::to(&location).into_response();
    response
        .headers_mut()
        .insert(SET_COOKIE, set_cookie(STATE_COOKIE, &state_val, Some(600)));
    Ok(response)
}

#[derive(Deserialize)]
pub struct HackatimeCallbackParams {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

pub async fn auth_hackatime_callback(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HackatimeCallbackParams>,
) -> Result<impl IntoResponse, StatusCode> {
    if let Some(error) = &params.error {
        tracing::warn!("hackatime authorization error: {}", error);
        return Ok(Redirect::to("/link").into_response());
    }
    let (Some(code), Some(callback_state)) = (params.code, params.state) else {
        tracing::warn!("hackatime callback missing code or state");
        return Err(StatusCode::BAD_REQUEST);
    };
    let session = session_from_request(&headers, &auth_config(&state));
    let Some(session) = session else {
        tracing::warn!("hackatime callback without session");
        return Err(StatusCode::UNAUTHORIZED);
    };
    let cookies = cookies(&headers);
    let expected_state = cookies.get(STATE_COOKIE).cloned();
    match expected_state {
        None => {
            tracing::warn!("hackatime callback missing state cookie");
            return Err(StatusCode::BAD_REQUEST);
        }
        Some(expected) if expected != callback_state => {
            tracing::warn!(
                "hackatime callback state mismatch (cookie={expected}, param={callback_state})"
            );
            return Err(StatusCode::BAD_REQUEST);
        }
        _ => {}
    }

    let token = match auth::exchange_hackatime_code(&state.http, &auth_config(&state), &code).await
    {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!("hackatime token exchange failed: {}", e);
            return Err(StatusCode::BAD_REQUEST);
        }
    };
    let me_slack_id = match auth::fetch_hackatime_me(&state.http, &token).await {
        Ok(s) => s,
        Err((_, message)) => {
            tracing::warn!("hackatime me fetch failed: {}", message);
            return Err(StatusCode::BAD_REQUEST);
        }
    };

    if me_slack_id.as_deref() != Some(session.slack_id.as_str()) {
        tracing::warn!(
            "hackatime slack_id mismatch: session={}, hackatime={:?}",
            session.slack_id,
            me_slack_id
        );
        return Err(StatusCode::FORBIDDEN);
    }

    if let Err(e) =
        clickhouse_db::upsert_hackatime_connection(&state.clickhouse, &session.slack_id, &token)
            .await
    {
        tracing::warn!(
            "upsert hackatime connection failed for {}: {}",
            session.slack_id,
            e
        );
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    let http = state.http.clone();
    let clickhouse = state.clickhouse.clone();
    let slack_id = session.slack_id.clone();
    tokio::spawn(async move {
        if let Err(e) = sync_coding_activity(&clickhouse, &http, &slack_id, Some(&token)).await
        {
            tracing::warn!("Coding activity sync failed for {}: {}", slack_id, e);
        }
    });

    let mut response = Redirect::to("/link").into_response();
    response
        .headers_mut()
        .append(SET_COOKIE, clear_cookie(STATE_COOKIE));
    Ok(response)
}

pub async fn auth_logout() -> impl IntoResponse {
    let mut response = Redirect::to("/").into_response();
    response
        .headers_mut()
        .insert(SET_COOKIE, clear_cookie(SESSION_COOKIE));
    response
}

pub async fn auth_hackatime_disconnect(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Redirect, StatusCode> {
    let session =
        session_from_request(&headers, &auth_config(&state)).ok_or(StatusCode::UNAUTHORIZED)?;
    clickhouse_db::delete_hackatime_connection(&state.clickhouse, &session.slack_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Redirect::to("/link"))
}

/// Why a coding sync could not complete. `PrivateProfile` and `NoAccount` are
/// permanent given the current account state (the resync loop records them so
/// it does not retry every cycle); `Message` is transient.
#[derive(Debug)]
pub enum SyncFailure {
    /// Public stats are disabled for this user and there is no token to fall
    /// back on, so only the OAuth path could read them.
    PrivateProfile,
    /// No hackatime account exists for this Slack UID.
    NoAccount,
    /// Transient failure (rate limit, outage, bad response, ...).
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

/// Fetches the total coding minutes for one user and stores it. With a token
/// the stats endpoint is called authenticated (so private profiles of the
/// token's owner work too); without one, the public stats API (keyed by Slack
/// UID) is used, which only works for public profiles. The fetch runs before
/// any DB write, so a failed sync leaves the old total intact.
pub async fn sync_coding_activity(
    clickhouse: &clickhouse::Client,
    http: &reqwest::Client,
    slack_id: &str,
    access_token: Option<&str>,
) -> Result<(), SyncFailure> {
    let lock = coding_sync_lock(slack_id);
    let _guard = lock.lock().await;
    let today = auth::today_utc();

    let minutes = match auth::fetch_total_minutes(http, slack_id, access_token, START_DATE).await {
        Ok(m) => m,
        Err((status, message)) => match (access_token, status) {
            (Some(_), Some(401 | 403)) => {
                // A 401/403 can mean a dead token or an outage in front of
                // hackatime (maintenance, auth proxy). Confirm the token is
                // really dead with the me endpoint before removing the link;
                // otherwise keep it and let the next sync retry. Any other
                // failure (HTTP 5xx, or no response at all) means hackatime is
                // simply down, so the link is kept.
                match auth::fetch_hackatime_me(http, access_token.unwrap()).await {
                    Err((Some(401 | 403), _)) => {
                        tracing::warn!(
                            "Hackatime token for {} is invalid, removing link",
                            slack_id
                        );
                        clickhouse_db::delete_hackatime_connection(clickhouse, slack_id)
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
                            "hackatime returned 401/403 but the me check did not confirm a dead \
                             token (likely down), keeping link: {message}"
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

    let conn = clickhouse_db::HackatimeConnectionRow {
        slack_id: slack_id.to_string(),
        access_token: access_token.unwrap_or("").to_string(),
        last_synced_date: Some(today),
        status: String::new(),
        total_minutes: minutes,
    };
    clickhouse_db::update_hackatime_connection(clickhouse, &conn)
        .await
        .map_err(|e| SyncFailure::Message(e.to_string()))?;

    tracing::info!("Synced {} coding minutes for {}", minutes, slack_id);
    Ok(())
}

const NO_ACCOUNT_RETRY_DAYS: u64 = 30;

/// Periodic hackatime resync over every user (every 30m). Users with an OAuth
/// connection sync through it; everyone else is fetched from the public stats
/// API by Slack UID. Public profiles that 404 (no account) or 403 (private,
/// no token to fall back on) are recorded so they are not retried every cycle.
pub async fn resync_all(clickhouse: &clickhouse::Client, http: &reqwest::Client) {
    let user_ids = match clickhouse_db::get_coding_user_ids(clickhouse).await {
        Ok(ids) => ids,
        Err(e) => {
            tracing::error!("Failed to list users for hackatime resync: {}", e);
            return;
        }
    };
    let connections = match clickhouse_db::get_hackatime_connections(clickhouse).await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("Failed to list hackatime connections: {}", e);
            return;
        }
    };
    let conns: HashMap<String, clickhouse_db::HackatimeConnectionReadRow> = connections
        .into_iter()
        .map(|c| (c.slack_id.clone(), c))
        .collect();
    let retry_cutoff = auth::date_days_ago(NO_ACCOUNT_RETRY_DAYS);
    let mut synced = 0u64;
    let mut skipped = 0u64;
    for user_id in user_ids {
        let conn = conns.get(&user_id);
        if let Some(c) = conn {
            if c.status == "no_account" {
                let probed = c.last_synced_date.as_deref().unwrap_or("");
                if probed >= retry_cutoff.as_str() {
                    skipped += 1;
                    continue;
                }
            }
            if c.status == "private" && c.access_token.is_empty() {
                skipped += 1;
                continue;
            }
        }
        let token = conn.and_then(|c| {
            if c.access_token.is_empty() {
                None
            } else {
                Some(c.access_token.as_str())
            }
        });
        let result = sync_coding_activity(clickhouse, http, &user_id, token).await;
        match result {
            Ok(()) => synced += 1,
            Err(SyncFailure::PrivateProfile) => {
                record_hackatime_status(clickhouse, &user_id, "private").await;
                tracing::info!("{} has a private hackatime profile, needs OAuth", user_id);
            }
            Err(SyncFailure::NoAccount) => {
                record_hackatime_status(clickhouse, &user_id, "no_account").await;
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
async fn record_hackatime_status(clickhouse: &clickhouse::Client, slack_id: &str, status: &str) {
    if let Err(e) = clickhouse_db::update_hackatime_connection(
        clickhouse,
        &clickhouse_db::HackatimeConnectionRow {
            slack_id: slack_id.to_string(),
            access_token: String::new(),
            last_synced_date: Some(auth::today_utc()),
            status: status.to_string(),
            total_minutes: 0,
        },
    )
    .await
    {
        tracing::warn!("Failed to record hackatime status {status} for {slack_id}: {e}");
    }
}
