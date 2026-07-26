use axum::{
    Router,
    routing::get,
    Json,
    response::Html,
};
use serde::Serialize;
use tower_http::services::ServeDir;
use crate::db::Database;

#[derive(Serialize)]
pub struct Stats {
    pub total_messages: u64,
    pub active_users: u64,
    pub channels: u64,
    pub coding_minutes: u64,
    pub leaderboard: Vec<UserStats>,
}

#[derive(Serialize)]
pub struct UserStats {
    pub user_id: String,
    pub messages: u64,
    pub coding_minutes: u64,
}

pub fn router(db: Database) -> Router {
    let api_routes = Router::new()
        .route("/stats", get(get_stats));

    Router::new()
        .route("/", get(|| async { Html(include_str!("static/index.html")) }))
        .route("/link", get(|| async { Html(include_str!("static/link.html")) }))
        .route("/stats", get(|| async { Html(include_str!("static/stats.html")) }))
        .nest("/api", api_routes)
        .fallback_service(ServeDir::new("static"))
}

async fn get_stats() -> Json<Stats> {
    // TODO: Query ClickHouse for real data
    Json(Stats {
        total_messages: 0,
        active_users: 0,
        channels: 0,
        coding_minutes: 0,
        leaderboard: vec![],
    })
}
