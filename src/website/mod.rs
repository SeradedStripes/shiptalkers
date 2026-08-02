use askama::Template;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Html;
use axum::{Router, routing::get};
use clickhouse::Client;
use std::collections::HashMap;
use tower_http::services::ServeDir;

pub mod auth;

#[derive(Clone)]
pub struct AppState {
    pub clickhouse: Client,
    pub auth: crate::auth::AuthConfig,
    pub http: reqwest::Client,
}

#[derive(Template)]
#[template(path = "index.html")]
pub struct IndexTemplate {
    pub signed_in: bool,
}

#[derive(Template)]
#[template(path = "stats.html")]
pub struct Stats {
    pub total_messages: String,
    pub active_users: String,
    pub channels_tracked: String,
    pub total_channels: String,
    pub scraped_channels: String,
    pub scrape_in_progress: bool,
    pub total_users: String,
    pub coding_minutes: String,
    pub db_size_label: String,
    pub top: Vec<TopUser>,
    pub leaderboard: Vec<UserStats>,
    pub timers: Vec<UserStats>,
    pub signed_in: bool,
}

#[derive(Template)]
#[template(path = "user.html")]
pub struct UserTemplate {
    pub display_name: String,
    pub slack_id: String,
    pub total_messages: String,
    pub coding_minutes: String,
    pub channels: String,
    pub last_msg: String,
    pub top_channels: Vec<ChannelStats>,
    pub signed_in: bool,
    pub found: bool,
}

pub struct ChannelStats {
    pub channel_name: String,
    pub messages: String,
}

#[derive(Template)]
#[template(path = "search.html")]
pub struct SearchTemplate {
    pub query: String,
    pub results: Vec<SearchResult>,
    pub signed_in: bool,
}

pub struct SearchResult {
    pub display_name: String,
    pub user_id: String,
}

pub struct UserStats {
    pub display_name: String,
    pub user_id: String,
    pub messages: String,
    pub coding_minutes: String,
}

pub struct TopUser {
    pub display_name: String,
    pub user_id: String,
    pub messages: String,
    pub coding_minutes: String,
    pub last_msg: String,
}

pub fn router(clickhouse: Client, auth_config: crate::auth::AuthConfig) -> Router {
    let state = AppState {
        clickhouse,
        auth: auth_config,
        http: reqwest::Client::new(),
    };

    Router::new()
        .route("/", get(get_index))
        .route("/link", get(auth::get_link))
        .route("/stats", get(get_stats_page))
        .route("/stats/:slack_id", get(get_user_stats))
        .route("/search", get(get_search))
        .route("/auth/hackclub/login", get(auth::auth_hackclub_login))
        .route("/auth/hackclub/callback", get(auth::auth_hackclub_callback))
        .route("/auth/hackatime/login", get(auth::auth_hackatime_login))
        .route(
            "/auth/hackatime/callback",
            get(auth::auth_hackatime_callback),
        )
        .route("/auth/logout", get(auth::auth_logout))
        .route(
            "/auth/hackatime/disconnect",
            get(auth::auth_hackatime_disconnect),
        )
        .route(
            "/style.css",
            get(|| async {
                axum::response::Response::builder()
                    .header(axum::http::header::CONTENT_TYPE, "text/css")
                    .body(axum::body::Body::from(include_str!("static/style.css")))
                    .unwrap()
            }),
        )
        .fallback_service(ServeDir::new("static"))
        .with_state(state)
}

fn fmt_thousands(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

async fn get_index(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Html<String>, StatusCode> {
    let signed_in = auth::session_from_request(&headers, &state.auth).is_some();
    let template = IndexTemplate { signed_in };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

async fn get_stats_page(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Html<String>, StatusCode> {
    let stats = load_stats(&state.clickhouse, &headers, &state.auth).await;
    let html = stats
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

async fn get_search(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Html<String>, StatusCode> {
    let signed_in = auth::session_from_request(&headers, &state.auth).is_some();
    let query = params.get("q").cloned().unwrap_or_default();

    let results = if query.trim().is_empty() {
        Vec::new()
    } else {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct SearchRow {
            user_id: String,
            display_name: String,
        }
        let pattern = format!("%{}%", query.trim());
        state
            .clickhouse
            .query(
                "SELECT user_id, display_name FROM users FINAL
                 WHERE display_name ILIKE ? OR user_id ILIKE ?
                 ORDER BY (display_name ILIKE ?) DESC, display_name
                 LIMIT 25",
            )
            .bind(&pattern)
            .bind(&pattern)
            .bind(&pattern)
            .fetch_all::<SearchRow>()
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|r| SearchResult {
                display_name: r.display_name,
                user_id: r.user_id,
            })
            .collect()
    };

    let template = SearchTemplate {
        query,
        results,
        signed_in,
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

async fn get_user_stats(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(slack_id): Path<String>,
) -> Result<Html<String>, StatusCode> {
    let ch = &state.clickhouse;
    let signed_in = auth::session_from_request(&headers, &state.auth).is_some();

    let display_name: String = ch
        .query("SELECT display_name FROM users FINAL WHERE user_id = ?")
        .bind(&slack_id)
        .fetch_one()
        .await
        .unwrap_or_default();

    let total_messages: u64 = ch
        .query("SELECT count() FROM slack_messages FINAL WHERE user_id = ?")
        .bind(&slack_id)
        .fetch_one()
        .await
        .unwrap_or(0);

    let coding_minutes: i64 = ch
        .query("SELECT sum(minutes) FROM coding_activity WHERE user_id = ?")
        .bind(&slack_id)
        .fetch_one()
        .await
        .unwrap_or(0);

    let channels: u64 = ch
        .query("SELECT uniqExact(channel_id) FROM slack_messages FINAL WHERE user_id = ?")
        .bind(&slack_id)
        .fetch_one()
        .await
        .unwrap_or(0);

    let last_ts: String = ch
        .query("SELECT max(message_ts) FROM slack_messages FINAL WHERE user_id = ?")
        .bind(&slack_id)
        .fetch_one()
        .await
        .unwrap_or_default();

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct ChannelCount {
        channel_id: String,
        messages: u64,
    }

    let counts: Vec<ChannelCount> = ch
        .query(
            "SELECT channel_id, count() as messages
             FROM slack_messages FINAL
             WHERE user_id = ?
             GROUP BY channel_id
             ORDER BY messages DESC
             LIMIT 10",
        )
        .bind(&slack_id)
        .fetch_all()
        .await
        .unwrap_or_default();

    let channel_names: std::collections::HashMap<String, String> = if counts.is_empty() {
        std::collections::HashMap::new()
    } else {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct ChannelName {
            channel_id: String,
            name: String,
        }
        let placeholders = counts.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let mut query = ch.query(&format!(
            "SELECT channel_id, name FROM slack_channels FINAL WHERE channel_id IN ({})",
            placeholders
        ));
        for c in &counts {
            query = query.bind(&c.channel_id);
        }
        query
            .fetch_all::<ChannelName>()
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|r| (r.channel_id, r.name))
            .collect()
    };

    let top_channels: Vec<ChannelStats> = counts
        .into_iter()
        .map(|c| ChannelStats {
            channel_name: channel_names
                .get(&c.channel_id)
                .cloned()
                .unwrap_or_else(|| c.channel_id.clone()),
            messages: fmt_thousands(c.messages),
        })
        .collect();

    let found = total_messages > 0 || coding_minutes > 0 || !display_name.is_empty();

    let template = UserTemplate {
        display_name: if display_name.is_empty() {
            slack_id.clone()
        } else {
            display_name
        },
        slack_id,
        total_messages: fmt_thousands(total_messages),
        coding_minutes: fmt_thousands(coding_minutes.max(0) as u64),
        channels: fmt_thousands(channels),
        last_msg: fmt_last_ts(&last_ts),
        top_channels,
        signed_in,
        found,
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

async fn load_stats(
    ch: &Client,
    headers: &HeaderMap,
    auth_config: &crate::auth::AuthConfig,
) -> Stats {
    let total_messages: u64 = ch
        .query("SELECT count() FROM slack_messages FINAL")
        .fetch_one()
        .await
        .unwrap_or(0);

    let active_users: u64 = ch
        .query("SELECT uniqExact(user_id) FROM slack_messages")
        .fetch_one()
        .await
        .unwrap_or(0);

    let channels_tracked: u64 = ch
        .query("SELECT uniqExact(channel_id) FROM slack_messages")
        .fetch_one()
        .await
        .unwrap_or(0);

    let total_channels: u64 = ch
        .query("SELECT count() FROM slack_channels FINAL")
        .fetch_one()
        .await
        .unwrap_or(0);

    let scraped_channels: u64 = ch
        .query("SELECT count() FROM scraped_channels")
        .fetch_one()
        .await
        .unwrap_or(0);

    let total_users: u64 = ch
        .query("SELECT count() FROM users FINAL")
        .fetch_one()
        .await
        .unwrap_or(0);

    let coding_minutes: u64 = ch
        .query("SELECT sum(minutes) FROM coding_activity")
        .fetch_one()
        .await
        .unwrap_or(0);

    let db_size_bytes: u64 = ch
        .query("SELECT sum(bytes_on_disk) as bytes FROM system.parts WHERE database = currentDatabase() AND active")
        .fetch_one()
        .await
        .unwrap_or(0);

    let db_size_gib = db_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
    let db_size_label = format!(
        "{:.prec$} GiB",
        db_size_gib,
        prec = if db_size_gib < 1.0 { 5 } else { 2 }
    );

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct LeaderboardRow {
        user_id: String,
        messages: u64,
    }

    let leaderboard: Vec<UserStats> = ch
        .query(
            "SELECT user_id, count() as messages
             FROM slack_messages FINAL
             GROUP BY user_id
             ORDER BY messages DESC
             LIMIT 20",
        )
        .fetch_all::<LeaderboardRow>()
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|r| UserStats {
                    user_id: r.user_id,
                    display_name: String::new(),
                    messages: fmt_thousands(r.messages),
                    coding_minutes: String::new(),
                })
                .collect()
        })
        .unwrap_or_default();

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct TimerRow {
        user_id: String,
        minutes: i64,
    }

    let timers: Vec<UserStats> = ch
        .query(
            "SELECT user_id, sum(minutes) as minutes
             FROM coding_activity
             GROUP BY user_id
             ORDER BY minutes DESC
             LIMIT 20",
        )
        .fetch_all::<TimerRow>()
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|r| UserStats {
                    user_id: r.user_id,
                    display_name: String::new(),
                    messages: String::new(),
                    coding_minutes: fmt_thousands(r.minutes.max(0) as u64),
                })
                .collect()
        })
        .unwrap_or_default();

    let mut name_ids: Vec<String> = leaderboard
        .iter()
        .chain(timers.iter())
        .map(|u| u.user_id.clone())
        .collect();
    name_ids.sort();
    name_ids.dedup();

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct NameRow {
        user_id: String,
        display_name: String,
    }

    let names: std::collections::HashMap<String, String> = if name_ids.is_empty() {
        std::collections::HashMap::new()
    } else {
        let in_list = name_ids.join("', '");
        ch.query(&format!(
            "SELECT user_id, display_name FROM users FINAL WHERE user_id IN ('{}')",
            in_list
        ))
        .fetch_all::<NameRow>()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|r| (r.user_id, r.display_name))
        .collect()
    };

    let display_name = |user_id: &str, fallback: String| -> String {
        match names.get(user_id) {
            Some(n) if !n.is_empty() => n.clone(),
            _ => fallback,
        }
    };

    let leaderboard: Vec<UserStats> = leaderboard
        .into_iter()
        .map(|mut u| {
            u.display_name = display_name(&u.user_id, u.user_id.clone());
            u
        })
        .collect();

    let timers: Vec<UserStats> = timers
        .into_iter()
        .map(|mut u| {
            u.display_name = display_name(&u.user_id, u.user_id.clone());
            u
        })
        .collect();

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct TopUserRow {
        user_id: String,
        messages: u64,
        last_ts: String,
    }

    let top_rows: Vec<TopUserRow> = ch
        .query(
            "SELECT user_id, count() as messages, max(message_ts) as last_ts
             FROM slack_messages FINAL
             GROUP BY user_id
             ORDER BY messages DESC
             LIMIT 3",
        )
        .fetch_all()
        .await
        .unwrap_or_default();

    let top_ids: Vec<String> = top_rows.iter().map(|r| r.user_id.clone()).collect();

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct TopUserNameRow {
        user_id: String,
        display_name: String,
    }

    let top_names: Vec<TopUserNameRow> = if top_ids.is_empty() {
        Vec::new()
    } else {
        let in_list = top_ids.join("', '");
        ch.query(&format!(
            "SELECT user_id, display_name FROM users FINAL WHERE user_id IN ('{}')",
            in_list
        ))
        .fetch_all()
        .await
        .unwrap_or_default()
    };

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct TopUserMinRow {
        user_id: String,
        minutes: i64,
    }

    let top_minutes: Vec<TopUserMinRow> = if top_ids.is_empty() {
        Vec::new()
    } else {
        let in_list = top_ids.join("', '");
        ch.query(&format!(
            "SELECT user_id, sum(minutes) as minutes FROM coding_activity
             WHERE user_id IN ('{}') GROUP BY user_id",
            in_list
        ))
        .fetch_all()
        .await
        .unwrap_or_default()
    };

    let top: Vec<TopUser> = top_rows
        .into_iter()
        .map(|r| {
            let minutes = top_minutes
                .iter()
                .find(|m| m.user_id == r.user_id)
                .map(|m| m.minutes)
                .unwrap_or(0);
            let name = top_names
                .iter()
                .find(|n| n.user_id == r.user_id)
                .map(|n| n.display_name.as_str())
                .unwrap_or("");
            TopUser {
                display_name: if name.is_empty() {
                    r.user_id.clone()
                } else {
                    name.to_string()
                },
                user_id: r.user_id,
                messages: fmt_thousands(r.messages),
                coding_minutes: fmt_thousands(minutes.max(0) as u64),
                last_msg: fmt_last_ts(&r.last_ts),
            }
        })
        .collect();

    Stats {
        total_messages: fmt_thousands(total_messages),
        active_users: fmt_thousands(active_users),
        channels_tracked: fmt_thousands(channels_tracked),
        total_channels: fmt_thousands(total_channels),
        scraped_channels: fmt_thousands(scraped_channels),
        scrape_in_progress: scraped_channels < total_channels,
        total_users: fmt_thousands(total_users),
        coding_minutes: fmt_thousands(coding_minutes),
        db_size_label,
        top,
        leaderboard,
        timers,
        signed_in: auth::session_from_request(headers, auth_config).is_some(),
    }
}

fn fmt_last_ts(ts: &str) -> String {
    let secs: i64 = ts
        .split('.')
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    if secs == 0 {
        return String::new();
    }
    let (year, month, day) = crate::auth::civil_from_days(secs / 86400);
    let hour = (secs % 86400) / 3600;
    let minute = (secs % 3600) / 60;
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02} UTC")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    #[tokio::test]
    async fn user_stats_route_matches() {
        let ch = Client::default()
            .with_url("http://localhost:8123")
            .with_user("ship_talkers")
            .with_password("ship_talkers")
            .with_database("ship_talkers");
        let auth_config = crate::auth::AuthConfig {
            hca_client_id: String::new(),
            hca_client_secret: String::new(),
            hackatime_client_id: String::new(),
            hackatime_client_secret: String::new(),
            base_url: "http://localhost:3000".into(),
            session_secret: String::new(),
        };
        let app = router(ch, auth_config);
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/stats/U01MPHKFZ7S")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = res.status();
        eprintln!("status for /stats/U01MPHKFZ7S: {}", status);
        assert_ne!(
            status,
            axum::http::StatusCode::NOT_FOUND,
            "route must match"
        );
    }
}
