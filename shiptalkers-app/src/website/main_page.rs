use super::{
    ApiDocsAccount, ApiDocsChannels, ApiDocsDailyStats, ApiDocsGrants, ApiDocsOverview,
    ApiDocsSearch, ApiDocsStats, AppState, IndexTemplate, State, StatusCode, signed_in,
};
use askama::Template;
use axum::extract::Path;
use axum::response::{Html, IntoResponse, Redirect, Response};
use std::time::Instant;

pub(super) async fn get_pfp(
    State(state): State<AppState>,
    Path(user_id): Path<String>,
) -> Response {
    let url: String = match state.pool() {
        Ok(pool) => super::sqlx::query_scalar::<_, Option<String>>(
            "SELECT pfp FROM users WHERE user_id = $1 OR ship_talkers_id = $1",
        )
        .bind(&user_id)
        .fetch_one(pool)
        .await
        .ok()
        .flatten()
        .unwrap_or_default(),
        Err(_) => String::new(),
    };
    if url.is_empty() {
        return StatusCode::NOT_FOUND.into_response();
    }
    Redirect::temporary(&url).into_response()
}

pub(super) async fn get_index(
    State(state): State<AppState>,
    headers: super::HeaderMap,
) -> Result<Html<String>, StatusCode> {
    let started = Instant::now();
    let signed_in = signed_in(&state, &headers);
    let template = IndexTemplate {
        signed_in,
        page_load_ms: format!("{}ms", started.elapsed().as_millis()),
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

pub(super) async fn get_api_docs(
    State(state): State<AppState>,
    headers: super::HeaderMap,
    topic: Option<Path<String>>,
) -> Result<Html<String>, StatusCode> {
    let started = Instant::now();
    let signed_in = signed_in(&state, &headers);
    let page_load_ms = format!("{}ms", started.elapsed().as_millis());
    let base_url = state.settings.get("BASE_URL");
    let topic = topic.as_ref().map(|p| p.as_str());
    let html = match topic {
        None | Some("overview") => ApiDocsOverview {
            signed_in,
            page_load_ms,
            base_url: base_url.clone(),
            current: "overview",
        }
        .render(),
        Some("stats") => ApiDocsStats {
            signed_in,
            page_load_ms: page_load_ms.clone(),
            base_url: base_url.clone(),
            current: "stats",
        }
        .render(),
        Some("channels") => ApiDocsChannels {
            signed_in,
            page_load_ms: page_load_ms.clone(),
            base_url: base_url.clone(),
            current: "channels",
        }
        .render(),
        Some("daily-stats") => ApiDocsDailyStats {
            signed_in,
            page_load_ms: page_load_ms.clone(),
            base_url: base_url.clone(),
            current: "daily-stats",
        }
        .render(),
        Some("search") => ApiDocsSearch {
            signed_in,
            page_load_ms: page_load_ms.clone(),
            base_url: base_url.clone(),
            current: "search",
        }
        .render(),
        Some("account") => ApiDocsAccount {
            signed_in,
            page_load_ms: page_load_ms.clone(),
            base_url,
            current: "account",
        }
        .render(),
        Some("grants") => ApiDocsGrants {
            signed_in,
            page_load_ms,
            base_url,
            current: "grants",
        }
        .render(),
        _ => return Err(StatusCode::NOT_FOUND),
    }
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}
