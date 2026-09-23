use super::{
    AppState, BoardCategoryTemplate, BoardEntry, DIRECTORY_PAGE_SIZE, Html, Query, State,
    StatusCode, directory_target, signed_in,
};
use askama::Template;
use axum::http::HeaderMap;
use std::collections::HashMap;
use std::time::Instant;

pub(super) async fn get_users_board(
    state: State<AppState>,
    headers: super::HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Html<String>, StatusCode> {
    render_users_board(state, headers, Query(params)).await
}

async fn render_users_board(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Html<String>, StatusCode> {
    let started = Instant::now();
    let ch = state.pool()?;
    let requested_page = params
        .get("page")
        .and_then(|page| page.parse::<u64>().ok())
        .filter(|page| *page > 0)
        .unwrap_or(1);
    let query = params.get("q").cloned().unwrap_or_default();
    let target = directory_target(ch, "users", "merged_name", "user_id", query.trim()).await;
    let page = target
        .as_ref()
        .map(|(rank, _)| rank.saturating_sub(1) / DIRECTORY_PAGE_SIZE as u64 + 1)
        .unwrap_or(requested_page);
    let search_mode = !query.trim().is_empty() && target.is_some();
    let row_offset = target
        .as_ref()
        .filter(|_| search_mode)
        .map(|(rank, _)| rank.saturating_sub(4))
        .unwrap_or(page.saturating_sub(1) * DIRECTORY_PAGE_SIZE as u64);
    let total: i64 = super::sqlx::query_scalar("SELECT count(*) FROM users")
        .fetch_one(ch)
        .await
        .unwrap_or(0);
    let page_count = ((total.max(0) as u64).saturating_add(DIRECTORY_PAGE_SIZE as u64 - 1)
        / DIRECTORY_PAGE_SIZE as u64)
        .max(1);
    let mut records: Vec<(String, String, String, String)> = super::sqlx::query_as("SELECT user_id, COALESCE(ship_talkers_id, user_id), merged_name, pfp FROM users ORDER BY COALESCE(ship_talkers_id, user_id), user_id LIMIT $1 OFFSET $2").bind(if search_mode { 6 } else { DIRECTORY_PAGE_SIZE + 1 }).bind(row_offset.min(i64::MAX as u64) as i64).fetch_all(ch).await.unwrap_or_default();
    if !query.trim().is_empty() && target.is_none() {
        records.clear();
    }
    let has_next = !search_mode && records.len() > DIRECTORY_PAGE_SIZE as usize;
    records.truncate(if search_mode {
        6
    } else {
        DIRECTORY_PAGE_SIZE as usize
    });
    let rows: Vec<BoardEntry> = records
        .into_iter()
        .enumerate()
        .map(|(index, (user_id, ship_talkers_id, merged_name, pfp))| {
            let highlight = target.as_ref().is_some_and(|(_, id)| id == &user_id);
            BoardEntry {
                user_id,
                url_id: ship_talkers_id.clone(),
                merged_name: if merged_name.is_empty() {
                    ship_talkers_id.clone()
                } else {
                    merged_name
                },
                pfp: super::local_pfp(&ship_talkers_id, &pfp),
                value: String::new(),
                extra: String::new(),
                linked: true,
                rank: row_offset + index as u64 + 1,
                label: ship_talkers_id,
                highlight,
                status: String::new(),
            }
        })
        .collect();
    let template = BoardCategoryTemplate {
        title: "All Users".into(),
        entity: "User".into(),
        unit: String::new(),
        extra_unit: None,
        rows,
        coming_soon: false,
        category: "users".into(),
        query,
        notice: None,
        numbered: false,
        show_pfp: true,
        has_previous: page > 1,
        has_next,
        page,
        page_count,
        directory_path: "/boards/users/".into(),
        signed_in: signed_in(&state, &headers),
        page_load_ms: format!("{}ms", started.elapsed().as_millis()),
    };
    Ok(Html(
        template
            .render()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    ))
}
