use crate::sqlx;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

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

#[derive(Deserialize)]
pub struct GrantParams {
    key_id: String,
}

async fn key_id_for(state: &AppState, headers: &HeaderMap) -> Result<(String, String), Response> {
    let Some(token) = bearer_token(headers) else {
        return Err(unauthorized());
    };
    let db = match state.auth_db() {
        Ok(db) => db,
        Err(status) => return Err(status.into_response()),
    };
    match db.resolve_key(&token).await {
        Ok(Some((key_id, slack_id))) => Ok((key_id, slack_id)),
        Ok(None) => Err(unauthorized()),
        Err(e) => {
            tracing::error!("resolve_key failed: {}", e);
            Err(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to look up API key",
            ))
        }
    }
}

/// Users who granted the calling key access to their data.
pub async fn list_grants(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let (key_id, owner) = match key_id_for(&state, &headers).await {
        Ok(pair) => pair,
        Err(resp) => return resp,
    };
    let db = match state.auth_db() {
        Ok(db) => db,
        Err(status) => return status.into_response(),
    };
    let pool = match state.pool() {
        Ok(pool) => pool,
        Err(status) => return status.into_response(),
    };
    let granted = match db.granted_users(&key_id).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!("granted_users failed for {}: {}", key_id, e);
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed to list grants");
        }
    };
    let mut names = std::collections::HashMap::new();
    let ids: Vec<&String> = granted
        .iter()
        .map(|(grantor_id, _)| grantor_id)
        .filter(|id| **id != owner)
        .collect();
    if !ids.is_empty() {
        let found: Vec<(String, String)> =
            sqlx::query_as("SELECT user_id, display_name FROM users WHERE user_id = ANY($1)")
                .bind(&ids)
                .fetch_all(pool)
                .await
                .unwrap_or_default();
        for (id, name) in found {
            names.insert(id, name);
        }
    }
    let grants: Vec<serde_json::Value> = granted
        .into_iter()
        .filter(|(grantor_id, _)| grantor_id != &owner)
        .map(|(grantor_id, created_at)| {
            serde_json::json!({
                "slack_id": grantor_id,
                "display_name": names.get(&grantor_id).cloned().unwrap_or(grantor_id),
                "created_at": created_at,
            })
        })
        .collect();
    Json(serde_json::json!({ "key_id": key_id, "grants": grants })).into_response()
}

/// Stats for one user whose data the calling key holds a grant for.
pub async fn get_granted_stats(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(grantor_id): Path<String>,
) -> Response {
    let (key_id, _owner) = match key_id_for(&state, &headers).await {
        Ok(pair) => pair,
        Err(resp) => return resp,
    };
    let db = match state.auth_db() {
        Ok(db) => db,
        Err(status) => return status.into_response(),
    };
    let granted = match db.has_grant(&grantor_id, &key_id).await {
        Ok(g) => g,
        Err(e) => {
            tracing::error!("has_grant failed for {}: {}", grantor_id, e);
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed to check grant");
        }
    };
    if !granted {
        return error_response(
            StatusCode::FORBIDDEN,
            "this key holds no grant for that user",
        );
    }
    let pool = match state.pool() {
        Ok(pool) => pool,
        Err(status) => return status.into_response(),
    };
    match load_user_stats(pool, &grantor_id).await {
        Ok(stats) => Json(stats).into_response(),
        Err(e) => {
            tracing::error!("load_user_stats failed for {}: {}", grantor_id, e);
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load user stats",
            )
        }
    }
}

/// Signed-in grantor opens their data to another user's key.
pub async fn create_grant(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(params): Json<GrantParams>,
) -> Response {
    let session = match session_from_request(&headers, &auth_config(&state)) {
        Some(s) => s,
        None => return unauthorized(),
    };
    if !csrf_ok(&headers, &auth_config(&state)) {
        return forbidden();
    }
    let key_id = params.key_id.trim();
    if key_id.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "key_id is required");
    }
    let db = match state.auth_db() {
        Ok(db) => db,
        Err(status) => return status.into_response(),
    };
    let created_at = time::OffsetDateTime::now_utc().unix_timestamp();
    match db.create_grant(&session.slack_id, key_id, created_at).await {
        Ok(true) => (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "grantor_id": session.slack_id,
                "key_id": key_id,
                "created_at": created_at,
            })),
        )
            .into_response(),
        Ok(false) => error_response(StatusCode::NOT_FOUND, "no such API key"),
        Err(e) => {
            tracing::error!("create_grant failed for {}: {}", session.slack_id, e);
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed to create grant")
        }
    }
}

/// Signed-in grantor closes their data to a key.
pub async fn revoke_grant(
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
    match db.revoke_grant(&session.slack_id, &key_id).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => error_response(StatusCode::NOT_FOUND, "no such grant for this key"),
        Err(e) => {
            tracing::error!("revoke_grant failed for {}: {}", session.slack_id, e);
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed to revoke grant")
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

#[derive(Deserialize)]
pub struct StatsParams {
    include: Option<String>,
}

const STATS_FIELDS: &[(&str, &str)] = &[
    ("messages", "total_messages"),
    ("channels", "total_channels"),
    ("users", "total_users"),
    ("coding", "coding_minutes"),
    ("slack_time", "slack_time_secs"),
    ("db_size", "db_size_bytes"),
    ("updated", "updated"),
];

pub async fn get_stats(
    State(state): State<AppState>,
    Query(params): Query<StatsParams>,
) -> Response {
    if let Err(status) = state.pool() {
        return status.into_response();
    }
    let snapshot = state
        .cache
        .stats
        .get_or(async { super::compute_stats(&state).await })
        .await;
    let requested: Option<Vec<&str>> = params
        .include
        .as_deref()
        .map(|s| s.split(',').map(str::trim).collect());
    let wanted = |token: &str| {
        requested
            .as_ref()
            .map(|list| list.contains(&token))
            .unwrap_or(true)
    };
    let mut map = serde_json::Map::new();
    for (token, key) in STATS_FIELDS {
        if !wanted(token) {
            continue;
        }
        let value: u64 = match *key {
            "total_messages" => snapshot.total_messages,
            "total_channels" => snapshot.total_channels,
            "total_users" => snapshot.total_users,
            "coding_minutes" => snapshot.coding_minutes,
            "slack_time_secs" => snapshot.slack_time_secs,
            "db_size_bytes" => snapshot.db_size_bytes,
            "updated" => snapshot.updated,
            _ => 0,
        };
        map.insert((*key).to_string(), serde_json::json!(value));
    }
    Json(serde_json::Value::Object(map)).into_response()
}

#[derive(Deserialize)]
pub struct LeaderboardParams {
    rank: Option<u64>,
    q: Option<String>,
    limit: Option<u32>,
}

enum LeaderboardKind {
    Users,
    Channels,
    Words,
}

pub async fn get_leaderboard(
    State(state): State<AppState>,
    Path(category): Path<String>,
    Query(params): Query<LeaderboardParams>,
) -> Response {
    let pool = match state.pool() {
        Ok(pool) => pool,
        Err(status) => return status.into_response(),
    };
    let q = params.q.unwrap_or_default();
    let parsed_rank = params.rank;
    let limit = params.limit.map(|n| n.clamp(1, 500) as u64).unwrap_or(100);

    if let Some(n) = parsed_rank
        && n == 0
    {
        return error_response(StatusCode::BAD_REQUEST, "rank must be at least 1");
    }
    if parsed_rank.is_some() && !q.trim().is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "rank and q are mutually exclusive");
    }

    let (inner, kind) = match category.as_str() {
        "talkers" => (
            format!(
                "SELECT user_id AS id, score AS value, messages::bigint AS extra, \
                 row_number() OVER (ORDER BY score DESC) AS rank \
                 FROM user_scores \
                 WHERE {sup}",
                sup = super::EXCLUDE_BOTS_DELETED
            ),
            LeaderboardKind::Users,
        ),
        "coders" => (
            format!(
                "SELECT id, value, CAST(NULL AS BIGINT) AS extra, rank \
                 FROM ( \
                     SELECT slack_id AS id, total_minutes::bigint AS value, \
                            row_number() OVER (ORDER BY total_minutes DESC) AS rank \
                     FROM hackatime_connections \
                     WHERE {sup} \
                 )",
                sup = super::EXCLUDE_BOTS_DELETED_SLACK_ID
            ),
            LeaderboardKind::Users,
        ),
        "channels" => (
            "SELECT channel_id AS id, total_time::bigint AS value, \
             messages::bigint AS extra, \
             row_number() OVER (ORDER BY total_time DESC) AS rank \
             FROM channel_scores"
                .to_string(),
            LeaderboardKind::Channels,
        ),
        "combined" => (
            format!(
                "SELECT id, value, CAST(NULL AS BIGINT) AS extra, rank \
                 FROM ( \
                     SELECT user_id AS id, value, row_number() OVER (ORDER BY value DESC) AS rank \
                     FROM ( \
                         SELECT user_id, sum(v)::bigint AS value \
                         FROM ( \
                             SELECT user_id, total_time::bigint AS v \
                             FROM user_scores \
                             UNION ALL \
                             SELECT slack_id AS user_id, (total_minutes * 60)::bigint AS v \
                             FROM hackatime_connections \
                         ) \
                         GROUP BY user_id \
                     ) \
                     WHERE {sup} \
                 )",
                sup = super::EXCLUDE_BOTS_DELETED
            ),
            LeaderboardKind::Users,
        ),
        "words" => (
            "SELECT word AS id, cnt::bigint AS value, CAST(NULL AS BIGINT) AS extra, rank \
             FROM ( \
                 SELECT word, cnt, row_number() OVER (ORDER BY cnt DESC) AS rank \
                 FROM word_totals \
             )"
            .to_string(),
            LeaderboardKind::Words,
        ),
        _ => {
            return error_response(
                StatusCode::NOT_FOUND,
                "unknown category; use talkers, coders, channels, combined, or words",
            );
        }
    };

    let total: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM ({inner}) q"
    )))
    .fetch_one(pool)
    .await
    .unwrap_or(0)
    .max(0);

    let qq = q.trim();
    let rows = if parsed_rank.is_none() && qq.is_empty() {
        super::fetch_rank_window(pool, &inner, 1, limit).await
    } else {
        let lo_hi =
            parsed_rank.map(|n| (n.saturating_sub(super::RANK_WINDOW), n + super::RANK_WINDOW));
        let lo_hi = match lo_hi {
            Some(lo_hi) => lo_hi,
            None => {
                let resolve = match kind {
                    LeaderboardKind::Users => super::resolve_user_sql(&inner, qq),
                    LeaderboardKind::Channels => {
                        let eq = super::sql_escape(&qq.to_lowercase());
                        format!(
                            "SELECT c.channel_id AS id FROM slack_channels AS c \
                             JOIN ({inner}) lb ON c.channel_id = lb.id \
                             WHERE lower(c.name) LIKE '%{eq}%' \
                             ORDER BY (lower(c.name) = '{eq}') DESC, lb.rank, lower(c.name) \
                             LIMIT 1"
                        )
                    }
                    LeaderboardKind::Words => {
                        let eq = super::sql_escape(&qq.to_lowercase());
                        format!(
                            "SELECT id FROM ({inner}) WHERE id = '{eq}' OR id LIKE '{eq}%' \
                             ORDER BY (id = '{eq}') DESC LIMIT 1"
                        )
                    }
                };
                match super::resolve_id(pool, &resolve).await {
                    Some(id) => match super::fetch_rank_of(pool, &inner, &id).await {
                        Some(rank) => (
                            rank.saturating_sub(super::RANK_WINDOW),
                            rank + super::RANK_WINDOW,
                        ),
                        None => {
                            return error_response(
                                StatusCode::NOT_FOUND,
                                &format!("'{}' is not on this leaderboard", qq),
                            );
                        }
                    },
                    None => {
                        return error_response(
                            StatusCode::NOT_FOUND,
                            &format!("no matches for '{}'", qq),
                        );
                    }
                }
            }
        };
        super::fetch_rank_window(pool, &inner, lo_hi.0, lo_hi.1).await
    };

    let names = fetch_display_names(pool, &rows, &kind).await;
    let entries: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|r| {
            serde_json::json!({
                "rank": r.rank,
                "id": r.id,
                "name": names.get(&r.id).cloned().unwrap_or_else(|| r.id.clone()),
                "value": r.value.max(0),
                "extra": r.extra.map(|v| v.max(0)),
            })
        })
        .collect();

    Json(serde_json::json!({
        "category": category,
        "total": total,
        "entries": entries,
    }))
    .into_response()
}

async fn fetch_display_names(
    pool: &crate::sqlx::PgPool,
    rows: &[super::RankedRow],
    kind: &LeaderboardKind,
) -> std::collections::HashMap<String, String> {
    let mut names = std::collections::HashMap::new();
    let ids: Vec<String> = rows.iter().map(|r| r.id.clone()).collect();
    if ids.is_empty() {
        return names;
    }
    match kind {
        LeaderboardKind::Users => {
            let found: Vec<(String, String)> =
                sqlx::query_as("SELECT user_id, display_name FROM users WHERE user_id = ANY($1)")
                    .bind(&ids)
                    .fetch_all(pool)
                    .await
                    .unwrap_or_default();
            for (id, name) in found {
                names.insert(id, name);
            }
        }
        LeaderboardKind::Channels => {
            let found: Vec<(String, String)> = sqlx::query_as(
                "SELECT channel_id, name FROM slack_channels WHERE channel_id = ANY($1)",
            )
            .bind(&ids)
            .fetch_all(pool)
            .await
            .unwrap_or_default();
            for (id, name) in found {
                names.insert(id, name);
            }
        }
        LeaderboardKind::Words => {}
    }
    names
}

pub async fn get_user(State(state): State<AppState>, Path(slack_id): Path<String>) -> Response {
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

#[derive(Serialize)]
struct ChannelStatsJson {
    channel_id: String,
    name: String,
    total_messages: i64,
    active_users: i64,
    first_message_ts: i64,
    last_message_ts: i64,
    top_posters: Vec<TopPosterJson>,
}

#[derive(Serialize)]
struct TopPosterJson {
    slack_id: String,
    display_name: String,
    pfp: String,
    messages: i64,
}

async fn load_channel_stats(
    pool: &crate::sqlx::PgPool,
    channel_id: &str,
) -> Result<Option<ChannelStatsJson>, String> {
    let name: Option<String> =
        sqlx::query_scalar("SELECT name FROM slack_channels WHERE channel_id = $1")
            .bind(channel_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| e.to_string())?;

    let total_messages: i64 =
        sqlx::query_scalar("SELECT count(*) FROM slack_messages WHERE channel_id = $1")
            .bind(channel_id)
            .fetch_one(pool)
            .await
            .map_err(|e| e.to_string())?;

    let active_users: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(DISTINCT user_id) FROM slack_messages \
         WHERE channel_id = $1 AND {sup}",
        sup = super::EXCLUDE_BOTS_DELETED
    )))
    .bind(channel_id)
    .fetch_one(pool)
    .await
    .map_err(|e| e.to_string())?;

    let first_message_ts: i64 = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT min(message_ts) FROM slack_messages WHERE channel_id = $1",
    )
    .bind(channel_id)
    .fetch_one(pool)
    .await
    .map_err(|e| e.to_string())?
    .unwrap_or(0)
    .max(0);

    let last_message_ts: i64 = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT max(message_ts) FROM slack_messages WHERE channel_id = $1",
    )
    .bind(channel_id)
    .fetch_one(pool)
    .await
    .map_err(|e| e.to_string())?
    .unwrap_or(0)
    .max(0);

    let posters: Vec<(String, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT user_id, count(*) AS messages \
         FROM slack_messages \
         WHERE channel_id = $1 AND {sup} \
         GROUP BY user_id \
         ORDER BY messages DESC \
         LIMIT 10",
        sup = super::EXCLUDE_BOTS_DELETED
    )))
    .bind(channel_id)
    .fetch_all(pool)
    .await
    .map_err(|e| e.to_string())?;

    let name_ids: Vec<String> = posters.iter().map(|(id, _)| id.clone()).collect();
    let mut poster_names = std::collections::HashMap::new();
    if !name_ids.is_empty() {
        let found: Vec<(String, String, String)> =
            sqlx::query_as("SELECT user_id, display_name, pfp FROM users WHERE user_id = ANY($1)")
                .bind(&name_ids)
                .fetch_all(pool)
                .await
                .map_err(|e| e.to_string())?;
        for (user_id, display_name, pfp) in found {
            poster_names.insert(user_id, (display_name, pfp));
        }
    }

    let top_posters: Vec<TopPosterJson> = posters
        .into_iter()
        .map(|(user_id, messages)| {
            let (display_name, pfp) = poster_names.get(&user_id).cloned().unwrap_or_default();
            TopPosterJson {
                slack_id: user_id.clone(),
                display_name: if display_name.is_empty() {
                    user_id.clone()
                } else {
                    display_name
                },
                pfp: super::local_pfp(&user_id, &pfp),
                messages,
            }
        })
        .collect();

    if total_messages == 0 && name.as_deref().unwrap_or_default().is_empty() {
        return Ok(None);
    }

    Ok(Some(ChannelStatsJson {
        channel_id: channel_id.to_string(),
        name: name.unwrap_or_else(|| channel_id.to_string()),
        total_messages,
        active_users,
        first_message_ts,
        last_message_ts,
        top_posters,
    }))
}

pub async fn get_channel(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
) -> Response {
    let pool = match state.pool() {
        Ok(pool) => pool,
        Err(status) => return status.into_response(),
    };
    match load_channel_stats(pool, &channel_id).await {
        Ok(Some(stats)) => Json(stats).into_response(),
        Ok(None) => error_response(StatusCode::NOT_FOUND, "no such channel"),
        Err(e) => {
            tracing::error!("load_channel_stats failed for {}: {}", channel_id, e);
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load channel stats",
            )
        }
    }
}

#[derive(Deserialize)]
pub struct DailyStatsParams {
    start: Option<String>,
    end: Option<String>,
}

fn is_iso_date(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 10
        && b[4] == b'-'
        && b[7] == b'-'
        && b[..4].iter().all(u8::is_ascii_digit)
        && b[5..7].iter().all(u8::is_ascii_digit)
        && b[8..10].iter().all(u8::is_ascii_digit)
}

pub async fn get_daily_stats(
    State(state): State<AppState>,
    Query(params): Query<DailyStatsParams>,
) -> Response {
    let pool = match state.pool() {
        Ok(pool) => pool,
        Err(status) => return status.into_response(),
    };
    if let Some(start) = params.start.as_deref()
        && !is_iso_date(start)
    {
        return error_response(StatusCode::BAD_REQUEST, "start must be YYYY-MM-DD");
    }
    if let Some(end) = params.end.as_deref()
        && !is_iso_date(end)
    {
        return error_response(StatusCode::BAD_REQUEST, "end must be YYYY-MM-DD");
    }
    let points: Vec<serde_json::Value> = sqlx::query_as::<_, (String, i64)>(
        "SELECT to_char(date, 'YYYY-MM-DD') AS date, slack_secs::bigint \
         FROM daily_stats \
         WHERE ($1::date IS NULL OR date >= $1::date) \
           AND ($2::date IS NULL OR date <= $2::date) \
         ORDER BY date",
    )
    .bind(params.start)
    .bind(params.end)
    .fetch_all(pool)
    .await
    .unwrap_or_default()
    .into_iter()
    .map(|(date, slack_secs)| serde_json::json!({ "date": date, "slack_secs": slack_secs.max(0) }))
    .collect();

    Json(serde_json::json!({ "points": points })).into_response()
}

#[derive(Deserialize)]
pub struct SearchParams {
    q: String,
}

pub async fn get_search(
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
) -> Response {
    let pool = match state.pool() {
        Ok(pool) => pool,
        Err(status) => return status.into_response(),
    };
    let q = params.q.trim();
    if q.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "q is required");
    }
    let pattern = format!("%{}%", q);
    let users: Vec<serde_json::Value> = sqlx::query_as::<_, (String, String, String, i16)>(
        "SELECT user_id, display_name, pfp, is_deleted FROM users \
         WHERE display_name ILIKE $1 OR user_id ILIKE $1 \
         ORDER BY (display_name ILIKE $1) DESC, display_name \
         LIMIT 25",
    )
    .bind(&pattern)
    .fetch_all(pool)
    .await
    .unwrap_or_default()
    .into_iter()
    .map(|(slack_id, display_name, pfp, is_deleted)| {
        serde_json::json!({
            "slack_id": slack_id,
            "display_name": display_name,
            "pfp": super::local_pfp(&slack_id, &pfp),
            "is_deleted": is_deleted == 1,
        })
    })
    .collect();

    let channels: Vec<serde_json::Value> = sqlx::query_as::<_, (String, String)>(
        "SELECT channel_id, name FROM slack_channels \
         WHERE name ILIKE $1 \
         ORDER BY name \
         LIMIT 25",
    )
    .bind(&pattern)
    .fetch_all(pool)
    .await
    .unwrap_or_default()
    .into_iter()
    .map(|(channel_id, name)| serde_json::json!({ "channel_id": channel_id, "name": name }))
    .collect();

    Json(serde_json::json!({
        "query": q,
        "users": users,
        "channels": channels,
    }))
    .into_response()
}
