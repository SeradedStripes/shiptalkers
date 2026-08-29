use crate::auth;
use crate::db::hackatime;
use askama::Template;
use axum::extract::{Query, State};
use axum::http::header::{COOKIE, SET_COOKIE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse, Redirect};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Instant;

use super::AppState;

const SESSION_COOKIE: &str = "st_session";
const STATE_COOKIE: &str = "st_state";

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
        Some(s) => hackatime::is_hackatime_connected(&state.pool, &s.slack_id)
            .await
            .unwrap_or(false),
        None => false,
    };
    let name = match &session {
        Some(s) => {
            let display_name: String = sqlx::query_scalar::<_, Option<String>>(
                "SELECT display_name FROM users WHERE user_id = $1",
            )
            .bind(&s.slack_id)
            .fetch_one(&state.pool)
            .await
            .ok()
            .flatten()
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
        hackatime::upsert_hackatime_connection(&state.pool, &session.slack_id, &token).await
    {
        tracing::warn!(
            "upsert hackatime connection failed for {}: {}",
            session.slack_id,
            e
        );
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

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
    hackatime::delete_hackatime_connection(&state.pool, &session.slack_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Redirect::to("/link"))
}
