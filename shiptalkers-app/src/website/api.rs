use crate::sqlx;
use axum::Json;
use axum::extract::Path;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use super::AppState;
use super::auth::{csrf_matches, session_from_request};

fn auth_config(state: &AppState) -> crate::auth::AuthConfig {
    state.settings.auth_config()
}

fn error_response(status: StatusCode, message: &str) -> Response {
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

fn unauthorized() -> Response {
    error_response(StatusCode::UNAUTHORIZED, "missing or invalid API key")
}

fn forbidden() -> Response {
    error_response(StatusCode::FORBIDDEN, "missing or invalid CSRF token")
}

/// State-changing session-authenticated endpoints require the CSRF token that matches the session cookie.
fn csrf_ok(headers: &HeaderMap, config: &crate::auth::AuthConfig) -> bool {
    let provided = headers.get("x-csrf-token").and_then(|v| v.to_str().ok());
    csrf_matches(headers, config, provided)
}

pub async fn create_api_key(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let session = match session_from_request(&headers, &auth_config(&state)) {
        Some(s) => s,
        None => return unauthorized(),
    };
    if !csrf_ok(&headers, &auth_config(&state)) {
        return forbidden();
    }
    let db = match state.auth_db() {
        Ok(db) => db,
        Err(status) => return status.into_response(),
    };
    let created_at = time::OffsetDateTime::now_utc().unix_timestamp();
    match db.create_api_key(&session.slack_id, created_at).await {
        Ok((key, key_id)) => (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "key_id": key_id,
                "key": key,
                "created_at": created_at,
            })),
        )
            .into_response(),
        Err(e) => {
            tracing::error!("create_api_key failed for {}: {}", session.slack_id, e);
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to create API key",
            )
        }
    }
}

pub async fn list_api_keys(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let session = match session_from_request(&headers, &auth_config(&state)) {
        Some(s) => s,
        None => return unauthorized(),
    };
    let db = match state.auth_db() {
        Ok(db) => db,
        Err(status) => return status.into_response(),
    };
    match db.list_api_keys(&session.slack_id).await {
        Ok(keys) => Json(serde_json::json!({ "keys": keys })).into_response(),
        Err(e) => {
            tracing::error!("list_api_keys failed for {}: {}", session.slack_id, e);
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed to list API keys")
        }
    }
}

pub async fn revoke_api_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(key_id): Path<String>,
) -> Response {
    let session = match session_from_request(&headers, &auth_config(&state)) {
        Some(s) => s,
        None => return unauthorized(),
    };
    if !csrf_ok(&headers, &auth_config(&state)) {
        return forbidden();
    }
    let db = match state.auth_db() {
        Ok(db) => db,
        Err(status) => return status.into_response(),
    };
    match db.revoke_api_key(&session.slack_id, &key_id).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => error_response(StatusCode::NOT_FOUND, "no such key for this user"),
        Err(e) => {
            tracing::error!("revoke_api_key failed for {}: {}", session.slack_id, e);
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to revoke API key",
            )
        }
    }
}

pub async fn get_me(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let token = match bearer_token(&headers) {
        Some(t) => t,
        None => return unauthorized(),
    };
    let db = match state.auth_db() {
        Ok(db) => db,
        Err(status) => return status.into_response(),
    };
    let slack_id = match db.slack_id_for_key(&token).await {
        Ok(Some(id)) => id,
        Ok(None) => return unauthorized(),
        Err(e) => {
            tracing::error!("slack_id_for_key failed: {}", e);
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to look up API key",
            );
        }
    };
    let pool = match state.pool() {
        Ok(pool) => pool,
        Err(status) => return status.into_response(),
    };
    match load_user_stats(pool, &slack_id).await {
        Ok(stats) => Json(stats).into_response(),
        Err(e) => {
            tracing::error!("load_user_stats failed for {}: {}", slack_id, e);
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load user stats",
            )
        }
    }
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(str::to_string)
}

#[derive(Serialize)]
struct UserStatsJson {
    slack_id: String,
    display_name: String,
    pfp: String,
    is_bot: bool,
    is_deleted: bool,
    found: bool,
    scores: Option<ScoreJson>,
    coding_minutes: u64,
    leaderboard_rank: Option<i64>,
    top_channels: Vec<TopChannelJson>,
}

#[derive(Serialize)]
struct ScoreJson {
    score: i64,
    total_time_secs: u64,
    messages: u64,
    sessions: u64,
    longest_secs: u64,
    days: u64,
    channels: u64,
    active_hour: u8,
}

#[derive(Serialize)]
struct TopChannelJson {
    channel_id: String,
    channel_name: String,
    messages: i64,
}

async fn load_user_stats(
    pool: &crate::sqlx::PgPool,
    slack_id: &str,
) -> Result<UserStatsJson, String> {
    let (display_name, pfp_url, is_bot, is_deleted): (String, String, i16, i16) = sqlx::query_as(
        "SELECT display_name, pfp, is_bot, is_deleted FROM users WHERE user_id = $1",
    )
    .bind(slack_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| e.to_string())?
    .unwrap_or(("".into(), "".into(), 0, 0));

    let is_bot = is_bot == 1;
    let is_deleted = is_deleted == 1;

    struct Scores {
        score: i64,
        total_time: i64,
        messages: i64,
        sessions: i64,
        longest: i64,
        days: i64,
        channels: i64,
        active_hour: i16,
    }

    let scores: Option<Scores> = sqlx::query_as::<_, (i64, i64, i64, i64, i64, i64, i64, i16)>(
        "SELECT score, total_time, messages, sessions, longest,
                days, channels, active_hour
         FROM user_scores WHERE user_id = $1",
    )
    .bind(slack_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| e.to_string())?
    .map(
        |(score, total_time, messages, sessions, longest, days, channels, active_hour)| Scores {
            score,
            total_time,
            messages,
            sessions,
            longest,
            days,
            channels,
            active_hour,
        },
    );

    let coding_minutes: u64 = sqlx::query_scalar::<_, i64>(
        "SELECT total_minutes FROM hackatime_connections WHERE slack_id = $1",
    )
    .bind(slack_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| e.to_string())?
    .unwrap_or(0)
    .max(0) as u64;

    let counts: Vec<(String, i64)> = sqlx::query_as(
        "SELECT channel_id, count(*) as messages
         FROM slack_messages
         WHERE user_id = $1
         GROUP BY channel_id
         ORDER BY messages DESC
         LIMIT 10",
    )
    .bind(slack_id)
    .fetch_all(pool)
    .await
    .map_err(|e| e.to_string())?;

    let name_ids: Vec<String> = counts.iter().map(|(id, _)| id.clone()).collect();
    let channel_names: std::collections::HashMap<String, String> = if name_ids.is_empty() {
        std::collections::HashMap::new()
    } else {
        sqlx::query_as::<_, (String, String)>(
            "SELECT channel_id, name FROM slack_channels WHERE channel_id = ANY($1)",
        )
        .bind(&name_ids)
        .fetch_all(pool)
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .collect()
    };

    let top_channels: Vec<TopChannelJson> = counts
        .into_iter()
        .map(|(channel_id, messages)| TopChannelJson {
            channel_id: channel_id.clone(),
            channel_name: channel_names
                .get(&channel_id)
                .cloned()
                .unwrap_or_else(|| channel_id.clone()),
            messages,
        })
        .collect();

    let total_messages = scores.as_ref().map(|s| s.messages).unwrap_or(0);
    let found = total_messages > 0 || coding_minutes > 0 || !display_name.is_empty();

    let leaderboard_rank: Option<i64> = if is_bot || is_deleted {
        None
    } else {
        match scores.as_ref() {
            Some(s) => sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(format!(
                "SELECT count(*) as rank
                 FROM (
                     SELECT user_id FROM user_scores
                      WHERE {sup} AND score > $1
                 )",
                sup = super::EXCLUDE_BOTS_DELETED
            )))
            .bind(s.score)
            .fetch_one(pool)
            .await
            .map_err(|e| e.to_string())
            .map(|rank| rank + 1)
            .ok(),
            None => None,
        }
    };

    Ok(UserStatsJson {
        slack_id: slack_id.to_string(),
        display_name,
        pfp: super::local_pfp(slack_id, &pfp_url),
        is_bot,
        is_deleted,
        found,
        scores: scores.map(|s| ScoreJson {
            score: s.score,
            total_time_secs: s.total_time.max(0) as u64,
            messages: s.messages.max(0) as u64,
            sessions: s.sessions.max(0) as u64,
            longest_secs: s.longest.max(0) as u64,
            days: s.days.max(0) as u64,
            channels: s.channels.max(0) as u64,
            active_hour: s.active_hour.clamp(0, 23) as u8,
        }),
        coding_minutes,
        leaderboard_rank,
        top_channels,
    })
}
