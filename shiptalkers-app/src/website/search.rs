use super::{AppState, SearchResult, SearchTemplate, State, StatusCode, local_pfp, signed_in};
use askama::Template;
use axum::extract::Query;
use axum::http::HeaderMap;
use axum::response::Html;
use std::collections::HashMap;
use std::time::Instant;

pub(super) async fn get_search(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Html<String>, StatusCode> {
    let started = Instant::now();
    let signed_in = signed_in(&state, &headers);
    let query = params.get("q").cloned().unwrap_or_default();
    let pool = state.pool().ok();
    let results = match (pool, query.trim().is_empty()) {
        (Some(pool), false) => {
            let pattern = format!("%{}%", query.trim());
            super::sqlx::query_as::<_, (String, String, String, i16, String)>(
                "SELECT user_id, merged_name, pfp, is_deleted, COALESCE(ship_talkers_id, user_id) FROM users
                 WHERE merged_name ILIKE $1 OR real_name ILIKE $1 OR username ILIKE $1 OR user_id ILIKE $1
                 ORDER BY (merged_name ILIKE $1) DESC, merged_name, real_name
                 LIMIT 25",
            )
            .bind(&pattern)
            .fetch_all(pool)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|(user_id, merged_name, pfp, is_deleted, url_id)| SearchResult {
                merged_name: if merged_name.is_empty() { user_id.clone() } else { merged_name },
                pfp: local_pfp(&url_id, &pfp),
                user_id,
                url_id,
                deactivated: is_deleted == 1,
                status: String::new(),
            })
            .collect()
        }
        _ => Vec::new(),
    };
    let channels = match (pool, query.trim().is_empty()) {
        (Some(pool), false) => {
            let pattern = format!("%{}%", query.trim());
            super::sqlx::query_as::<_, (String, String, String, i16, i16)>(
                "SELECT channel_id, name, COALESCE(ship_talkers_id, channel_id), is_private, is_archived FROM slack_channels
                 WHERE name ILIKE $1
                 ORDER BY name
                 LIMIT 25",
            )
            .bind(&pattern)
            .fetch_all(pool)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|(channel_id, name, url_id, is_private, is_archived)| SearchResult {
                merged_name: name,
                pfp: String::new(),
                user_id: channel_id,
                url_id,
                deactivated: false,
                status: super::channel_status(is_private, is_archived),
            })
            .collect()
        }
        _ => Vec::new(),
    };
    let template = SearchTemplate {
        query,
        results,
        channels,
        signed_in,
        page_load_ms: format!("{}ms", started.elapsed().as_millis()),
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}
