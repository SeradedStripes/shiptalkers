use askama::Template;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::{Router, routing::get};
use clickhouse::Client;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tower_http::services::ServeDir;

pub mod auth;

struct TtlValue<T> {
    value: T,
    expires_at: Instant,
}

struct TtlCache<T> {
    inner: Mutex<Option<TtlValue<T>>>,
    ttl: Duration,
}

impl<T> TtlCache<T> {
    fn new(ttl: Duration) -> Self {
        Self {
            inner: Mutex::new(None),
            ttl,
        }
    }

    async fn get_or<F>(&self, compute: F) -> T
    where
        T: Clone,
        F: Future<Output = T>,
    {
        let mut guard = self.inner.lock().await;
        if let Some(cached) = guard.as_ref()
            && cached.expires_at > Instant::now()
        {
            return cached.value.clone();
        }
        let value = compute.await;
        *guard = Some(TtlValue {
            value: value.clone(),
            expires_at: Instant::now() + self.ttl,
        });
        value
    }
}

#[derive(Clone)]
pub struct AppCache {
    banner: Arc<TtlCache<String>>,
    stats: Arc<TtlCache<StatsSnapshot>>,
}

impl AppCache {
    fn new() -> Self {
        Self {
            banner: Arc::new(TtlCache::new(Duration::from_secs(60))),
            stats: Arc::new(TtlCache::new(Duration::from_secs(60))),
        }
    }
}

#[derive(Clone)]
struct StatsSnapshot {
    total_messages: u64,
    active_users: u64,
    channels_tracked: u64,
    total_channels: u64,
    total_users: u64,
    coding_minutes: u64,
    db_size_bytes: u64,
}

#[derive(Clone)]
pub struct AppState {
    pub clickhouse: Client,
    pub auth: crate::auth::AuthConfig,
    pub http: reqwest::Client,
    pub slack_time: crate::formula::Formula,
    pub auth_db: std::sync::Arc<crate::db::sqlite::AuthDb>,
    pub cache: AppCache,
}

#[derive(Template)]
#[template(path = "index.html")]
pub struct IndexTemplate {
    pub signed_in: bool,
    pub scrape_banner: String,
}

#[derive(Template)]
#[template(path = "stats.html")]
pub struct Stats {
    pub total_messages: String,
    pub active_users: String,
    pub channels_tracked: String,
    pub total_channels: String,
    pub total_users: String,
    pub coding_minutes: String,
    pub db_size_label: String,
    pub scrape_banner: String,
    pub signed_in: bool,
}

#[derive(Template)]
#[template(path = "user.html")]
pub struct UserTemplate {
    pub display_name: String,
    pub pfp: String,
    pub slack_id: String,
    pub total_messages: String,
    pub coding_minutes: String,
    pub channels: String,
    pub first_msg: String,
    pub last_msg: String,
    pub slack_time_total: String,
    pub slack_time_avg: String,
    pub slack_time_longest: String,
    pub slack_time_per_day: String,
    pub leaderboard_rank: String,
    pub active_hour: String,
    pub top_channels: Vec<ChannelStats>,
    pub signed_in: bool,
    pub scrape_banner: String,
    pub found: bool,
}

pub struct ChannelStats {
    pub user_id: String,
    pub channel_name: String,
    pub messages: String,
}

#[derive(Template)]
#[template(path = "channel.html")]
pub struct ChannelTemplate {
    pub channel_name: String,
    pub channel_id: String,
    pub total_messages: String,
    pub active_users: String,
    pub first_msg: String,
    pub last_msg: String,
    pub top_posters: Vec<UserStats>,
    pub signed_in: bool,
    pub scrape_banner: String,
    pub found: bool,
}

#[derive(Template)]
#[template(path = "search.html")]
pub struct SearchTemplate {
    pub query: String,
    pub results: Vec<SearchResult>,
    pub channels: Vec<SearchResult>,
    pub signed_in: bool,
    pub scrape_banner: String,
}

#[derive(Template)]
#[template(path = "leaderboard.html")]
pub struct LeaderboardTemplate {
    pub signed_in: bool,
    pub scrape_banner: String,
}

#[derive(Template)]
#[template(path = "leaderboard_category.html")]
pub struct LeaderboardCategoryTemplate {
    pub title: String,
    pub unit: String,
    pub extra_unit: Option<String>,
    pub rows: Vec<LeaderboardEntry>,
    pub coming_soon: bool,
    pub signed_in: bool,
    pub scrape_banner: String,
}

pub struct LeaderboardEntry {
    pub user_id: String,
    pub display_name: String,
    pub pfp: String,
    pub value: String,
    pub extra: String,
}

pub struct SearchResult {
    pub display_name: String,
    pub pfp: String,
    pub user_id: String,
}

pub struct UserStats {
    pub display_name: String,
    pub pfp: String,
    pub user_id: String,
    pub messages: String,
}

pub fn router(
    clickhouse: Client,
    auth_config: crate::auth::AuthConfig,
    slack_time: crate::formula::Formula,
    auth_db: std::sync::Arc<crate::db::sqlite::AuthDb>,
) -> Router {
    let state = AppState {
        clickhouse,
        auth: auth_config,
        http: reqwest::Client::new(),
        slack_time,
        auth_db,
        cache: AppCache::new(),
    };

    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/", get(get_index))
        .route("/link", get(auth::get_link))
        .route("/stats", get(get_stats_page))
        .route("/stats/:id", get(get_stats_for_id))
        .route("/leaderboard", get(get_leaderboard))
        .route("/leaderboard/:category", get(get_leaderboard_category))
        .route("/search", get(get_search))
        .route("/pfp/:id", get(get_pfp))
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
        .route(
            "/time.js",
            get(|| async {
                axum::response::Response::builder()
                    .header(axum::http::header::CONTENT_TYPE, "application/javascript")
                    .body(axum::body::Body::from(include_str!("static/time.js")))
                    .unwrap()
            }),
        )
        .fallback_service(ServeDir::new("static"))
        .with_state(state)
}

fn local_pfp(user_id: &str, pfp_url: &str) -> String {
    if pfp_url.is_empty() {
        String::new()
    } else {
        format!("/pfp/{}", user_id)
    }
}

async fn get_pfp(State(state): State<AppState>, Path(user_id): Path<String>) -> Response {
    let url: String = state
        .clickhouse
        .query("SELECT pfp FROM users FINAL WHERE user_id = ?")
        .bind(&user_id)
        .fetch_one()
        .await
        .unwrap_or_default();
    if url.is_empty() {
        return StatusCode::NOT_FOUND.into_response();
    }
    Redirect::temporary(&url).into_response()
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

async fn scrape_banner_html(state: &AppState) -> String {
    state
        .cache
        .banner
        .get_or(async {
            let ch = &state.clickhouse;
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
            if total_channels == 0 || scraped_channels >= total_channels {
                return String::new();
            }
            let total_messages: u64 = ch
                .query("SELECT count() FROM slack_messages")
                .fetch_one()
                .await
                .unwrap_or(0);
            let active_users: u64 = ch
                .query("SELECT uniqExact(user_id) FROM slack_messages")
                .fetch_one()
                .await
                .unwrap_or(0);
            let total_users: u64 = ch
                .query("SELECT count() FROM users FINAL")
                .fetch_one()
                .await
                .unwrap_or(0);

            let channel_frac = scraped_channels as f64 / total_channels as f64;
            let user_frac = if total_users > 0 {
                active_users as f64 / total_users as f64
            } else {
                1.0
            };
            let coverage = (channel_frac + user_frac) / 2.0;
            let scrape_pct_done = (coverage * 100.0).round().clamp(0.0, 100.0) as u64;
            let scrape_pct_left = 100 - scrape_pct_done;
            let messages_estimate = if coverage > 0.0 {
                (total_messages as f64 / coverage).round() as u64
            } else {
                total_messages
            };

            format!(
                r#"<div class="scrape-banner"><div>Scraping in progress: {}/{} channels ({}% complete, about {}% left, {}/{} users)</div><div class="scrape-progress"><div class="scrape-progress-fill" style="width: {}%"></div></div><div>{} of ~{} estimated messages</div></div>"#,
                fmt_thousands(scraped_channels),
                fmt_thousands(total_channels),
                scrape_pct_done,
                scrape_pct_left,
                fmt_thousands(active_users),
                fmt_thousands(total_users),
                scrape_pct_done,
                fmt_thousands(total_messages),
                fmt_thousands(messages_estimate),
            )
        })
        .await
}

async fn get_index(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Html<String>, StatusCode> {
    let signed_in = auth::session_from_request(&headers, &state.auth).is_some();
    let template = IndexTemplate {
        signed_in,
        scrape_banner: scrape_banner_html(&state).await,
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

async fn get_stats_page(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Html<String>, StatusCode> {
    let stats = load_stats(&state, &headers).await;
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
            pfp: String,
        }
        let pattern = format!("%{}%", query.trim());
        state
            .clickhouse
            .query(
                "SELECT user_id, display_name, pfp FROM users FINAL
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
                pfp: local_pfp(&r.user_id, &r.pfp),
                user_id: r.user_id,
            })
            .collect()
    };

    let channels = if query.trim().is_empty() {
        Vec::new()
    } else {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct ChannelRow {
            channel_id: String,
            name: String,
        }
        let pattern = format!("%{}%", query.trim());
        state
            .clickhouse
            .query(
                "SELECT channel_id, name FROM slack_channels FINAL
                 WHERE name ILIKE ?
                 ORDER BY name
                 LIMIT 25",
            )
            .bind(&pattern)
            .fetch_all::<ChannelRow>()
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|r| SearchResult {
                display_name: r.name,
                pfp: String::new(),
                user_id: r.channel_id,
            })
            .collect()
    };

    let template = SearchTemplate {
        query,
        results,
        channels,
        signed_in,
        scrape_banner: scrape_banner_html(&state).await,
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

async fn get_stats_for_id(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Html<String>, StatusCode> {
    if id.starts_with('C') {
        get_channel_stats(&state, &headers, &id).await
    } else {
        get_user_stats(&state, &headers, &id).await
    }
}

async fn get_leaderboard(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Html<String>, StatusCode> {
    let signed_in = auth::session_from_request(&headers, &state.auth).is_some();
    let template = LeaderboardTemplate {
        signed_in,
        scrape_banner: scrape_banner_html(&state).await,
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

async fn get_leaderboard_category(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(category): Path<String>,
) -> Result<Html<String>, StatusCode> {
    let ch = &state.clickhouse;
    let signed_in = auth::session_from_request(&headers, &state.auth).is_some();

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct LeaderboardRow {
        user_id: String,
        value: i64,
    }

    let (title, unit, extra_unit, coming_soon, rows): (
        String,
        String,
        Option<String>,
        bool,
        Vec<LeaderboardEntry>,
    ) = match category.as_str() {
        "talkers" => {
            #[derive(clickhouse::Row, serde::Deserialize)]
            struct ScoreRow {
                user_id: String,
                total_time: u64,
                messages: u64,
            }

            let rows: Vec<ScoreRow> = match ch
                .query(
                    "SELECT user_id, total_time, messages
                     FROM user_scores FINAL
                     ORDER BY score DESC
                     LIMIT 100",
                )
                .fetch_all()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!("talkers leaderboard query failed: {}", e);
                    Vec::new()
                }
            };
            let ranked: Vec<(String, i64, Option<i64>)> = rows
                .into_iter()
                .map(|r| (r.user_id, r.total_time as i64, Some(r.messages as i64)))
                .collect();
            (
                "Top Talkers".into(),
                "Slack Time".into(),
                Some("Messages".into()),
                false,
                leaderboard_entries(ch, ranked, fmt_duration, Some(fmt_thousands)).await,
            )
        }
        "coders" => {
            let rows: Vec<(String, i64)> = match ch
                .query(
                    "SELECT user_id, sum(m) as value FROM (
                         SELECT user_id, date, max(minutes) AS m
                         FROM coding_activity
                         GROUP BY user_id, date
                     )
                     GROUP BY user_id
                     ORDER BY value DESC
                     LIMIT 100",
                )
                .fetch_all::<LeaderboardRow>()
                .await
            {
                Ok(r) => r.into_iter().map(|r| (r.user_id, r.value)).collect(),
                Err(e) => {
                    tracing::error!("coders leaderboard query failed: {}", e);
                    Vec::new()
                }
            };
            let rows: Vec<(String, i64, Option<i64>)> =
                rows.into_iter().map(|(id, v)| (id, v, None)).collect();
            (
                "Top Coders".into(),
                "Coding Time".into(),
                None,
                false,
                leaderboard_entries(ch, rows, |v| format!("{} min", fmt_thousands(v)), None).await,
            )
        }
        "combined" => ("Top Combined".into(), String::new(), None, true, Vec::new()),
        _ => {
            return Err(StatusCode::NOT_FOUND);
        }
    };

    let template = LeaderboardCategoryTemplate {
        title,
        unit,
        extra_unit,
        rows,
        coming_soon,
        signed_in,
        scrape_banner: scrape_banner_html(&state).await,
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

async fn leaderboard_entries(
    ch: &Client,
    rows: Vec<(String, i64, Option<i64>)>,
    format_value: impl Fn(u64) -> String,
    format_extra: Option<fn(u64) -> String>,
) -> Vec<LeaderboardEntry> {
    let mut name_ids: Vec<String> = rows.iter().map(|(user_id, _, _)| user_id.clone()).collect();
    name_ids.sort();
    name_ids.dedup();

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct NameRow {
        user_id: String,
        display_name: String,
        pfp: String,
    }

    let names: std::collections::HashMap<String, (String, String)> = if name_ids.is_empty() {
        std::collections::HashMap::new()
    } else {
        let in_list = name_ids.join("', '");
        ch.query(&format!(
            "SELECT user_id, display_name, pfp FROM users FINAL WHERE user_id IN ('{}')",
            in_list
        ))
        .fetch_all::<NameRow>()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|r| (r.user_id, (r.display_name, r.pfp)))
        .collect()
    };

    rows.into_iter()
        .map(|(user_id, value, extra)| {
            let value = value.max(0) as u64;
            let (display_name, pfp) = names.get(&user_id).cloned().unwrap_or_default();
            LeaderboardEntry {
                user_id: user_id.clone(),
                display_name: if display_name.is_empty() {
                    user_id.clone()
                } else {
                    display_name
                },
                pfp: local_pfp(&user_id, &pfp),
                value: format_value(value),
                extra: extra
                    .map(|v| v.max(0) as u64)
                    .and_then(|v| format_extra.map(|f| f(v)))
                    .unwrap_or_default(),
            }
        })
        .collect()
}

fn fmt_duration(secs: u64) -> String {
    let hours = secs / 3600;
    let mins = (secs % 3600) / 60;
    if hours > 0 {
        format!("{}h {}m", hours, mins)
    } else {
        format!("{}m", mins)
    }
}

fn fmt_hour(hour: u8) -> String {
    let ampm = if hour < 12 { "AM" } else { "PM" };
    let mut hour = hour % 12;
    if hour == 0 {
        hour = 12;
    }
    format!("{} {}", hour, ampm)
}

async fn get_user_stats(
    state: &AppState,
    headers: &HeaderMap,
    slack_id: &str,
) -> Result<Html<String>, StatusCode> {
    let ch = &state.clickhouse;
    let signed_in = auth::session_from_request(headers, &state.auth).is_some();

    let display_name: String = ch
        .query("SELECT display_name FROM users FINAL WHERE user_id = ?")
        .bind(slack_id)
        .fetch_one()
        .await
        .unwrap_or_default();

    let pfp_url: String = ch
        .query("SELECT pfp FROM users FINAL WHERE user_id = ?")
        .bind(slack_id)
        .fetch_one()
        .await
        .unwrap_or_default();
    let pfp = local_pfp(slack_id, &pfp_url);

    let total_messages: u64 = ch
        .query("SELECT count() FROM slack_messages WHERE user_id = ?")
        .bind(slack_id)
        .fetch_one()
        .await
        .unwrap_or(0);

    let coding_minutes: i64 = ch
        .query(
            "SELECT sum(minutes) FROM (
                 SELECT max(minutes) AS minutes
                 FROM coding_activity
                 WHERE user_id = ?
                 GROUP BY date
             )",
        )
        .bind(slack_id)
        .fetch_one()
        .await
        .unwrap_or(0);

    let channels: u64 = ch
        .query("SELECT uniqExact(channel_id) FROM slack_messages WHERE user_id = ?")
        .bind(slack_id)
        .fetch_one()
        .await
        .unwrap_or(0);

    let total_chars: u64 = ch
        .query("SELECT sum(char_length(text)) FROM slack_messages WHERE user_id = ?")
        .bind(slack_id)
        .fetch_one()
        .await
        .unwrap_or(0);

    let last_ts: String = ch
        .query("SELECT max(message_ts) FROM slack_messages WHERE user_id = ?")
        .bind(slack_id)
        .fetch_one()
        .await
        .unwrap_or_default();

    let first_ts: String = ch
        .query("SELECT min(message_ts) FROM slack_messages WHERE user_id = ?")
        .bind(slack_id)
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
             FROM slack_messages
             WHERE user_id = ?
             GROUP BY channel_id
             ORDER BY messages DESC
             LIMIT 10",
        )
        .bind(slack_id)
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
            user_id: c.channel_id.clone(),
            channel_name: channel_names
                .get(&c.channel_id)
                .cloned()
                .unwrap_or_else(|| c.channel_id.clone()),
            messages: fmt_thousands(c.messages),
        })
        .collect();

    let found = total_messages > 0 || coding_minutes > 0 || !display_name.is_empty();

    let (slack_time_total, slack_time_avg, slack_time_longest, slack_time_per_day, active_hour) =
        if total_messages > 0 {
            #[derive(clickhouse::Row, serde::Deserialize)]
            struct SlackTimeRow {
                total_time: u64,
                longest: u64,
                sessions: u64,
                days: u64,
            }

            #[derive(clickhouse::Row, serde::Deserialize)]
            struct HourRow {
                hour: u8,
            }

            let report: SlackTimeRow = ch
                .query(
                    "WITH
                     msg AS (
                         SELECT toInt64(splitByChar('.', message_ts)[1]) AS ts
                         FROM slack_messages
                         WHERE user_id = ?
                     ),
                     flagged AS (
                         SELECT ts,
                             if(ts - lag(ts) OVER (ORDER BY ts) > 2100, 1, 0) AS boundary
                         FROM msg
                     ),
                     sess AS (
                         SELECT ts, sum(boundary) OVER (ORDER BY ts) AS sid
                         FROM flagged
                     ),
                     sessions AS (
                         SELECT min(ts) AS start_ts, max(ts) AS end_ts
                         FROM sess
                         GROUP BY sid
                     )
                     SELECT sum(least(end_ts + 300 - start_ts, 14400)) AS total_time,
                            max(least(end_ts + 300 - start_ts, 14400)) AS longest,
                            count() AS sessions,
                            greatest(dateDiff('day', toDateTime(min(start_ts)), toDateTime(max(start_ts))) + 1, 1) AS days
                     FROM sessions",
                )
                .bind(slack_id)
                .fetch_one()
                .await
                .unwrap_or(SlackTimeRow {
                    total_time: 0,
                    longest: 0,
                    sessions: 0,
                    days: 1,
                });

            let total = state
                .slack_time
                .eval(&crate::formula::Metrics {
                    message_count: total_messages,
                    session_seconds: report.total_time,
                    session_count: report.sessions,
                    avg_message_length: if total_messages > 0 {
                        total_chars as f64 / total_messages as f64
                    } else {
                        0.0
                    },
                    total_chars,
                })
                .max(0.0) as u64;
            let avg_session = total.checked_div(report.sessions).unwrap_or(0);
            let per_day = report.sessions as f64 / report.days as f64;

            let hour: HourRow = ch
                .query(
                    "SELECT toHour(toDateTime(toInt64(splitByChar('.', message_ts)[1]))) AS hour
                     FROM slack_messages
                     WHERE user_id = ?
                     GROUP BY hour
                     ORDER BY count() DESC
                     LIMIT 1",
                )
                .bind(slack_id)
                .fetch_one()
                .await
                .unwrap_or(HourRow { hour: 0 });

            (
                fmt_duration(total),
                fmt_duration(avg_session),
                fmt_duration(report.longest),
                format!("{:.1} / day", per_day),
                fmt_hour(hour.hour),
            )
        } else {
            (
                "0m".into(),
                "0m".into(),
                "0m".into(),
                "0 / day".into(),
                String::new(),
            )
        };

    let leaderboard_rank: String = {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct RankRow {
            rank: u64,
        }

        let score: Option<i64> = ch
            .query("SELECT score FROM user_scores FINAL WHERE user_id = ? LIMIT 1")
            .bind(slack_id)
            .fetch_optional()
            .await
            .unwrap_or(None);
        match score {
            Some(_) => ch
                .query(
                    "SELECT count() + 1
                     FROM (
                         SELECT user_id FROM user_scores FINAL
                         WHERE score > (
                             SELECT score FROM user_scores FINAL WHERE user_id = ? LIMIT 1
                         )
                     )",
                )
                .bind(slack_id)
                .fetch_one::<RankRow>()
                .await
                .map(|r| format!("#{}", fmt_thousands(r.rank)))
                .unwrap_or_default(),
            None => String::new(),
        }
    };

    let template = UserTemplate {
        display_name: if display_name.is_empty() {
            slack_id.to_string()
        } else {
            display_name
        },
        pfp,
        slack_id: slack_id.to_string(),
        total_messages: fmt_thousands(total_messages),
        coding_minutes: fmt_thousands(coding_minutes.max(0) as u64),
        channels: fmt_thousands(channels),
        first_msg: fmt_ts_local(&first_ts),
        last_msg: fmt_ts_local(&last_ts),
        slack_time_total,
        slack_time_avg,
        slack_time_longest,
        slack_time_per_day,
        leaderboard_rank,
        active_hour,
        top_channels,
        signed_in,
        scrape_banner: scrape_banner_html(state).await,
        found,
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

async fn get_channel_stats(
    state: &AppState,
    headers: &HeaderMap,
    channel_id: &str,
) -> Result<Html<String>, StatusCode> {
    let ch = &state.clickhouse;
    let signed_in = auth::session_from_request(headers, &state.auth).is_some();

    let channel_name: String = ch
        .query("SELECT name FROM slack_channels FINAL WHERE channel_id = ?")
        .bind(channel_id)
        .fetch_one()
        .await
        .unwrap_or_default();

    let total_messages: u64 = ch
        .query("SELECT count() FROM slack_messages WHERE channel_id = ?")
        .bind(channel_id)
        .fetch_one()
        .await
        .unwrap_or(0);

    let active_users: u64 = ch
        .query("SELECT uniqExact(user_id) FROM slack_messages WHERE channel_id = ?")
        .bind(channel_id)
        .fetch_one()
        .await
        .unwrap_or(0);

    let last_ts: String = ch
        .query("SELECT max(message_ts) FROM slack_messages WHERE channel_id = ?")
        .bind(channel_id)
        .fetch_one()
        .await
        .unwrap_or_default();

    let first_ts: String = ch
        .query("SELECT min(message_ts) FROM slack_messages WHERE channel_id = ?")
        .bind(channel_id)
        .fetch_one()
        .await
        .unwrap_or_default();

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct PosterRow {
        user_id: String,
        messages: u64,
    }

    let posters: Vec<PosterRow> = ch
        .query(
            "SELECT user_id, count() as messages
             FROM slack_messages
             WHERE channel_id = ?
             GROUP BY user_id
             ORDER BY messages DESC
             LIMIT 10",
        )
        .bind(channel_id)
        .fetch_all()
        .await
        .unwrap_or_default();

    let name_ids: Vec<String> = posters.iter().map(|p| p.user_id.clone()).collect();

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct PosterNameRow {
        user_id: String,
        display_name: String,
        pfp: String,
    }

    let poster_names: std::collections::HashMap<String, (String, String)> = if name_ids.is_empty() {
        std::collections::HashMap::new()
    } else {
        let in_list = name_ids.join("', '");
        ch.query(&format!(
            "SELECT user_id, display_name, pfp FROM users FINAL WHERE user_id IN ('{}')",
            in_list
        ))
        .fetch_all::<PosterNameRow>()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|r| (r.user_id, (r.display_name, r.pfp)))
        .collect()
    };

    let top_posters: Vec<UserStats> = posters
        .into_iter()
        .map(|p| {
            let (display_name, pfp) = poster_names.get(&p.user_id).cloned().unwrap_or_default();
            UserStats {
                user_id: p.user_id.clone(),
                display_name: if display_name.is_empty() {
                    p.user_id.clone()
                } else {
                    display_name
                },
                pfp: local_pfp(&p.user_id, &pfp),
                messages: fmt_thousands(p.messages),
            }
        })
        .collect();

    let found = total_messages > 0 || !channel_name.is_empty();

    let template = ChannelTemplate {
        channel_name: if channel_name.is_empty() {
            channel_id.to_string()
        } else {
            channel_name
        },
        channel_id: channel_id.to_string(),
        total_messages: fmt_thousands(total_messages),
        active_users: fmt_thousands(active_users),
        first_msg: fmt_ts_local(&first_ts),
        last_msg: fmt_ts_local(&last_ts),
        top_posters,
        signed_in,
        scrape_banner: scrape_banner_html(state).await,
        found,
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

async fn load_stats(state: &AppState, headers: &HeaderMap) -> Stats {
    let snapshot = state
        .cache
        .stats
        .get_or(async { compute_stats(state).await })
        .await;

    let db_size_gib = snapshot.db_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
    let db_size_label = format!(
        "{:.prec$} GiB",
        db_size_gib,
        prec = if db_size_gib < 1.0 { 5 } else { 2 }
    );

    Stats {
        total_messages: fmt_thousands(snapshot.total_messages),
        active_users: fmt_thousands(snapshot.active_users),
        channels_tracked: fmt_thousands(snapshot.channels_tracked),
        total_channels: fmt_thousands(snapshot.total_channels),
        total_users: fmt_thousands(snapshot.total_users),
        coding_minutes: fmt_thousands(snapshot.coding_minutes),
        db_size_label,
        scrape_banner: scrape_banner_html(state).await,
        signed_in: auth::session_from_request(headers, &state.auth).is_some(),
    }
}

async fn compute_stats(state: &AppState) -> StatsSnapshot {
    let ch = &state.clickhouse;
    let total_messages: u64 = ch
        .query("SELECT count() FROM slack_messages")
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

    let total_users: u64 = ch
        .query("SELECT count() FROM users FINAL")
        .fetch_one()
        .await
        .unwrap_or(0);

    let coding_minutes: u64 = ch
        .query(
            "SELECT sum(minutes) FROM (
                 SELECT max(minutes) AS minutes
                 FROM coding_activity
                 GROUP BY user_id, date
             )",
        )
        .fetch_one()
        .await
        .unwrap_or(0);

    let db_size_bytes: u64 = ch
        .query("SELECT sum(bytes_on_disk) as bytes FROM system.parts WHERE database = currentDatabase() AND active")
        .fetch_one()
        .await
        .unwrap_or(0);

    StatsSnapshot {
        total_messages,
        active_users,
        channels_tracked,
        total_channels,
        total_users,
        coding_minutes,
        db_size_bytes,
    }
}

fn parse_ts(ts: &str) -> Option<(u32, u32, u32, u32, u32)> {
    let secs: i64 = ts.split('.').next()?.parse().ok()?;
    if secs == 0 {
        return None;
    }
    let (year, month, day) = crate::auth::civil_from_days(secs / 86400);
    let hour = ((secs % 86400) / 3600) as u32;
    let minute = ((secs % 3600) / 60) as u32;
    Some((year, month, day, hour, minute))
}

fn fmt_ts_local(ts: &str) -> String {
    match parse_ts(ts) {
        Some((year, month, day, hour, minute)) => format!(
            "<time datetime=\"{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:00Z\">\
             {year:04}-{month:02}-{day:02} {hour:02}:{minute:02} UTC</time>"
        ),
        None => String::new(),
    }
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
        let app = router(
            ch,
            auth_config,
            crate::formula::Formula::parse(crate::formula::SLACK_TIME_CALCULATION_FORMULA).unwrap(),
            std::sync::Arc::new(
                crate::db::sqlite::AuthDb::open(":memory:").expect("open in-memory auth db"),
            ),
        );
        for uri in ["/stats/U01MPHKFZ7S", "/stats/C0123456789"] {
            let res = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            let status = res.status();
            eprintln!("status for {}: {}", uri, status);
            assert_ne!(
                status,
                axum::http::StatusCode::NOT_FOUND,
                "route must match"
            );
        }
    }
}
