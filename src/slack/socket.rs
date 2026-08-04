use futures::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::bot_image;
use crate::db::clickhouse_db::{self, SlackChannelRow};
use crate::db::sqlite::AuthDb;

#[derive(Debug, Deserialize)]
struct SocketMessage {
    #[serde(rename = "type")]
    msg_type: String,
    payload: Option<serde_json::Value>,
    envelope_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChannelCreated {
    channel: ChannelInfo,
}

#[derive(Debug, Deserialize)]
struct ChannelInfo {
    id: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct MessageEvent {
    channel: String,
    ts: String,
    user: Option<String>,
    text: Option<String>,
    thread_ts: Option<String>,
    channel_type: Option<String>,
    subtype: Option<String>,
    bot_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConnectionsOpenResponse {
    ok: bool,
    url: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PostMessageResponse {
    ok: bool,
    error: Option<String>,
}

#[derive(Deserialize)]
struct GetUploadUrlResponse {
    ok: bool,
    error: Option<String>,
    upload_url: Option<String>,
    file_id: Option<String>,
}

#[derive(Clone)]
pub struct SocketConfig {
    pub app_tokens: Vec<String>,
    pub bot_tokens: Vec<String>,
    pub main_channel: Option<String>,
    pub base_url: String,
}

impl SocketConfig {
    pub fn new(
        app_tokens: Vec<String>,
        bot_tokens: Vec<String>,
        main_channel: Option<String>,
        base_url: String,
    ) -> Self {
        Self {
            app_tokens,
            bot_tokens,
            main_channel,
            base_url,
        }
    }
}

pub async fn start_socket_mode(
    config: SocketConfig,
    clickhouse: clickhouse::Client,
    auth_db: std::sync::Arc<AuthDb>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if config.app_tokens.is_empty() {
        return Err("No SLACK_APP_TOKENS set, Socket Mode disabled".into());
    }

    if let Some(channel) = &config.main_channel {
        tracing::info!("Stats bot watching channel {}", channel);
    } else {
        tracing::warn!("SLACK_MAIN_CHANNEL not set, stats bot disabled");
    }

    let num_sockets = config.app_tokens.len();
    let mut sockets = Vec::with_capacity(num_sockets);
    for (socket_idx, app_token) in config.app_tokens.iter().enumerate() {
        let config = config.clone();
        let clickhouse = clickhouse.clone();
        let auth_db = auth_db.clone();
        sockets.push(run_socket(
            socket_idx,
            num_sockets,
            app_token.clone(),
            config,
            clickhouse,
            auth_db,
        ));
    }

    let results = futures::future::join_all(sockets).await;
    let errors: Vec<String> = results
        .into_iter()
        .filter_map(|r| r.err())
        .map(|e| e.to_string())
        .collect();

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; ").into())
    }
}

async fn run_socket(
    socket_idx: usize,
    num_sockets: usize,
    app_token: String,
    config: SocketConfig,
    clickhouse: clickhouse::Client,
    auth_db: std::sync::Arc<AuthDb>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = Client::new();
    let mut failures = 0u32;

    loop {
        match serve_socket(
            &client,
            socket_idx,
            num_sockets,
            &app_token,
            &config,
            &clickhouse,
            &auth_db,
        )
        .await
        {
            Ok(()) => {
                tracing::warn!(
                    "Socket Mode connection ended (app {}/{}), reconnecting",
                    socket_idx + 1,
                    num_sockets
                );
                failures = 0;
            }
            Err(e) => {
                tracing::error!(
                    "Socket Mode error (app {}/{}): {}",
                    socket_idx + 1,
                    num_sockets,
                    e
                );
                failures += 1;
            }
        }

        let delay = (1u64 << failures.min(6)).min(60);
        tracing::info!(
            "Reconnecting Socket Mode in {}s (app {}/{})",
            delay,
            socket_idx + 1,
            num_sockets
        );
        tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
    }
}

async fn serve_socket(
    client: &Client,
    socket_idx: usize,
    num_sockets: usize,
    app_token: &str,
    config: &SocketConfig,
    clickhouse: &clickhouse::Client,
    auth_db: &std::sync::Arc<AuthDb>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let resp: ConnectionsOpenResponse = client
        .post("https://slack.com/api/apps.connections.open")
        .header("Authorization", format!("Bearer {}", app_token))
        .send()
        .await?
        .json()
        .await?;

    if !resp.ok {
        return Err(format!("Slack API error: {}", resp.error.unwrap_or_default()).into());
    }

    let ws_url = resp.url.ok_or("No WebSocket URL returned")?;
    tracing::info!(
        "Connecting to Socket Mode (app {}/{})...",
        socket_idx + 1,
        num_sockets
    );

    let (mut ws_stream, _) = connect_async(&ws_url).await?;
    tracing::info!(
        "Connected to Slack Socket Mode (app {}/{})",
        socket_idx + 1,
        num_sockets
    );

    while let Some(msg) = ws_stream.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Ok(socket_msg) = serde_json::from_str::<SocketMessage>(&text) {
                    match socket_msg.msg_type.as_str() {
                        "hello" => {
                            tracing::info!("Socket Mode handshake complete");
                        }
                        "events_api" => {
                            if let Some(envelope_id) = &socket_msg.envelope_id {
                                let ack = serde_json::json!({
                                    "envelope_id": envelope_id
                                });
                                let _ = ws_stream.send(Message::Text(ack.to_string())).await;
                            }

                            if let Some(payload) = &socket_msg.payload
                                && let Some(event) = payload.get("event")
                                && let Some(event_type) = event.get("type").and_then(|v| v.as_str())
                            {
                                match event_type {
                                    "channel_created" => {
                                        handle_channel_created(client, event, clickhouse).await;
                                    }
                                    "message" => {
                                        handle_message(
                                            client,
                                            config,
                                            socket_idx,
                                            num_sockets,
                                            auth_db,
                                            clickhouse,
                                            event,
                                        )
                                        .await;
                                    }
                                    _ => {}
                                }
                            }
                        }
                        "disconnect" => {
                            tracing::warn!("Socket Mode disconnected");
                            break;
                        }
                        _ => {}
                    }
                }
            }
            Ok(Message::Ping(_)) => {
                let _ = ws_stream.send(Message::Pong(vec![])).await;
            }
            Err(e) => {
                tracing::error!("WebSocket error: {}", e);
                break;
            }
            _ => {}
        }
    }

    Ok(())
}

fn shard_for_ts(ts: &str, num_sockets: usize) -> usize {
    if num_sockets <= 1 {
        return 0;
    }
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in ts.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash % num_sockets as u64) as usize
}

async fn handle_channel_created(
    _client: &Client,
    event: &serde_json::Value,
    clickhouse: &clickhouse::Client,
) {
    let Ok(created) = serde_json::from_value::<ChannelCreated>(event.clone()) else {
        return;
    };

    tracing::info!(
        "New channel created: #{} ({})",
        created.channel.name,
        created.channel.id
    );

    let row = SlackChannelRow {
        channel_id: created.channel.id,
        name: created.channel.name,
    };

    if let Err(e) = clickhouse_db::insert_new_channels(clickhouse, &[row]).await {
        tracing::error!("Failed to insert new channel: {}", e);
    }
}

async fn handle_message(
    client: &Client,
    config: &SocketConfig,
    socket_idx: usize,
    num_sockets: usize,
    auth_db: &std::sync::Arc<AuthDb>,
    clickhouse: &clickhouse::Client,
    event: &serde_json::Value,
) {
    let Some(main_channel) = &config.main_channel else {
        return;
    };

    let Ok(msg) = serde_json::from_value::<MessageEvent>(event.clone()) else {
        return;
    };

    if msg.channel != *main_channel {
        return;
    }
    if shard_for_ts(&msg.ts, num_sockets) != socket_idx {
        return;
    }
    if msg.channel_type.as_deref() != Some("channel") {
        return;
    }
    if msg.subtype.is_some() || msg.bot_id.is_some() {
        return;
    }
    if msg.user.is_none() || msg.text.is_none() {
        return;
    }
    if let Some(thread_ts) = &msg.thread_ts
        && thread_ts != &msg.ts
    {
        return;
    }

    let user = msg.user.unwrap_or_default();
    let text = msg.text.unwrap_or_default();

    let Some(range) = parse_time_range(&text) else {
        return;
    };
    tracing::info!(
        "Stats bot: stats request from {} in {} ({:?})",
        user,
        msg.channel,
        text
    );

    let Some(bot_token) = config.bot_tokens.first() else {
        tracing::warn!("Stats bot: no bot tokens configured, skipping reply");
        return;
    };

    if !auth_db.is_linked(&user).await {
        tracing::info!("Stats bot: {} is not linked, sending link prompt", user);
        let reply = format!(
            "You aren't linked yet. Link your account here to get your stats: {}/link",
            config.base_url.trim_end_matches('/')
        );
        if let Err(e) = post_reply(client, bot_token, &msg.channel, &msg.ts, &reply).await {
            tracing::error!("Stats bot: failed to post reply: {}", e);
        }
        return;
    }

    let (slack_seconds, coding_seconds) = query_stats(clickhouse, &user, &range).await;
    let user_name = user_display_name(clickhouse, &user).await;

    let (percent, more, other) = if slack_seconds >= coding_seconds {
        let percent = if coding_seconds > 0 {
            ((slack_seconds as f64 / coding_seconds as f64 - 1.0) * 100.0).round() as u64
        } else {
            100
        };
        (percent, "Slack", "Coding")
    } else {
        let percent = if slack_seconds > 0 {
            ((coding_seconds as f64 / slack_seconds as f64 - 1.0) * 100.0).round() as u64
        } else {
            100
        };
        (percent, "Coding", "Slack")
    };

    let slack_time = fmt_span(slack_seconds);
    let coding_time = fmt_span(coding_seconds);
    tracing::info!(
        "Stats bot: {} spent {} on Slack vs {} on Coding ({}% more {})",
        user,
        slack_time,
        coding_time,
        percent,
        more
    );

    let image = bot_image::StatsImage {
        user: &user_name,
        percent,
        more,
        other,
        slack_time: &slack_time,
        coding_time: &coding_time,
    };
    let png = match bot_image::render_stats_image(&image) {
        Ok(png) => png,
        Err(e) => {
            tracing::error!("Stats bot: failed to render stats image: {}", e);
            return;
        }
    };

    if let Err(e) = upload_image(client, bot_token, &msg.channel, &msg.ts, png).await {
        tracing::error!("Stats bot: failed to upload stats image: {}", e);
    }
}

async fn query_stats(clickhouse: &clickhouse::Client, user: &str, range: &TimeRange) -> (u64, u64) {
    let slack = query_slack_seconds(clickhouse, user, range).await;
    let coding = query_coding_seconds(clickhouse, user, range).await;
    (slack, coding)
}

async fn query_slack_seconds(
    clickhouse: &clickhouse::Client,
    user: &str,
    range: &TimeRange,
) -> u64 {
    let mut sql = String::from(
        "WITH
         msg AS (
             SELECT toInt64(splitByChar('.', message_ts)[1]) AS ts
             FROM slack_messages
             WHERE user_id = ?",
    );
    if range.start_ts().is_some() {
        sql.push_str(" AND ts >= ?");
    }
    sql.push_str(
        "),
         flagged AS (
             SELECT ts, if(ts - lag(ts) OVER (ORDER BY ts) > 2100, 1, 0) AS boundary
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
         SELECT sum(least(end_ts + 300 - start_ts, 14400)) AS total_time
         FROM sessions",
    );

    let mut query = clickhouse.query(&sql);
    query = query.bind(user);
    if let Some(start_ts) = range.start_ts() {
        query = query.bind(start_ts);
    }
    query.fetch_one::<u64>().await.unwrap_or(0)
}

async fn query_coding_seconds(
    clickhouse: &clickhouse::Client,
    user: &str,
    range: &TimeRange,
) -> u64 {
    let mut sql = String::from(
        "SELECT sum(minutes) FROM (
             SELECT max(minutes) AS minutes
             FROM coding_activity
             WHERE user_id = ?",
    );
    if range.start_date().is_some() {
        sql.push_str(" AND date >= ?");
    }
    sql.push_str(" GROUP BY date )");
    let mut query = clickhouse.query(&sql);
    query = query.bind(user);
    if let Some(start_date) = range.start_date() {
        query = query.bind(&start_date);
    }
    let minutes: i64 = query.fetch_one().await.unwrap_or(0);
    minutes.max(0) as u64 * 60
}

async fn user_display_name(clickhouse: &clickhouse::Client, user: &str) -> String {
    let name: String = clickhouse
        .query("SELECT display_name FROM users FINAL WHERE user_id = ?")
        .bind(user)
        .fetch_one()
        .await
        .unwrap_or_default();
    if name.is_empty() {
        user.to_string()
    } else {
        name
    }
}

async fn upload_image(
    client: &Client,
    bot_token: &str,
    channel: &str,
    thread_ts: &str,
    png: Vec<u8>,
) -> Result<(), String> {
    let response = client
        .post("https://slack.com/api/files.getUploadURLExternal")
        .header("Authorization", format!("Bearer {}", bot_token))
        .form(&[
            ("filename", "stats.png"),
            ("length", &png.len().to_string()),
        ])
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let parsed: GetUploadUrlResponse = serde_json::from_str(&body).map_err(|e| {
        format!("files.getUploadURLExternal returned bad JSON ({status}, {body:?}): {e}")
    })?;
    if !parsed.ok {
        return Err(format!(
            "Slack API error: {} ({})",
            parsed.error.unwrap_or_default(),
            status
        ));
    }
    let upload_url = parsed.upload_url.unwrap_or_default();
    let file_id = parsed.file_id.unwrap_or_default();

    let response = client
        .post(&upload_url)
        .header("Content-Type", "application/octet-stream")
        .body(png)
        .send()
        .await
        .map_err(|e| format!("failed to upload file to upload URL: {e}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "file upload to upload URL failed ({})",
            response.status()
        ));
    }

    let files = serde_json::json!([{ "id": file_id }]).to_string();
    let response = client
        .post("https://slack.com/api/files.completeUploadExternal")
        .header("Authorization", format!("Bearer {}", bot_token))
        .form(&[
            ("files", files.as_str()),
            ("channel_id", channel),
            ("thread_ts", thread_ts),
        ])
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let parsed: PostMessageResponse = serde_json::from_str(&body).map_err(|e| {
        format!("files.completeUploadExternal returned bad JSON ({status}, {body:?}): {e}")
    })?;
    if !parsed.ok {
        return Err(format!(
            "Slack API error: {} ({})",
            parsed.error.unwrap_or_default(),
            status
        ));
    }
    Ok(())
}

fn fmt_span(secs: u64) -> String {
    let hours = secs / 3600;
    let mins = (secs % 3600) / 60;
    if hours > 0 {
        format!("{}h {}m", hours, mins)
    } else if mins > 0 {
        format!("{}m", mins)
    } else {
        format!("{}s", secs)
    }
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn start_of_day(ts: i64) -> i64 {
    ts / 86400 * 86400
}

fn start_of_week(ts: i64) -> i64 {
    let day = ts / 86400;
    let offset = (day + 3).rem_euclid(7);
    (day - offset) * 86400
}

fn start_of_month(ts: i64) -> i64 {
    let days = ts / 86400;
    let (year, month, _) = crate::auth::civil_from_days(days);
    days_from_civil(year as i64, month as i64, 1) * 86400
}

fn start_of_year(ts: i64) -> i64 {
    let days = ts / 86400;
    let (year, _, _) = crate::auth::civil_from_days(days);
    days_from_civil(year as i64, 1, 1) * 86400
}

enum TimeRange {
    AllTime,
    Since(i64),
}

impl TimeRange {
    fn start_ts(&self) -> Option<i64> {
        match self {
            TimeRange::AllTime => None,
            TimeRange::Since(ts) => Some(*ts),
        }
    }

    fn start_date(&self) -> Option<String> {
        match self {
            TimeRange::AllTime => None,
            TimeRange::Since(ts) => {
                let (year, month, day) = crate::auth::civil_from_days(ts / 86400);
                Some(format!("{year:04}-{month:02}-{day:02}"))
            }
        }
    }
}

fn parse_time_range(text: &str) -> Option<TimeRange> {
    let normalized: String = text
        .to_lowercase()
        .chars()
        .filter(|c| !c.is_ascii_punctuation())
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    let now = now_unix();
    let ranges: Vec<(&str, TimeRange)> = vec![
        ("all time", TimeRange::AllTime),
        ("alltime", TimeRange::AllTime),
        ("last 24 hours", TimeRange::Since(now - 86400)),
        ("last 7 days", TimeRange::Since(now - 7 * 86400)),
        ("last 14 days", TimeRange::Since(now - 14 * 86400)),
        ("last 2 weeks", TimeRange::Since(now - 14 * 86400)),
        ("last 30 days", TimeRange::Since(now - 30 * 86400)),
        ("last 90 days", TimeRange::Since(now - 90 * 86400)),
        ("last 3 months", TimeRange::Since(now - 90 * 86400)),
        ("last 365 days", TimeRange::Since(now - 365 * 86400)),
        ("last year", TimeRange::Since(now - 365 * 86400)),
        ("last month", TimeRange::Since(now - 30 * 86400)),
        ("last week", TimeRange::Since(now - 7 * 86400)),
        ("this year", TimeRange::Since(start_of_year(now))),
        ("this month", TimeRange::Since(start_of_month(now))),
        ("this week", TimeRange::Since(start_of_week(now))),
        ("yesterday", TimeRange::Since(start_of_day(now - 86400))),
        ("today", TimeRange::Since(start_of_day(now))),
    ];

    ranges
        .into_iter()
        .find(|(phrase, _)| normalized.contains(phrase))
        .map(|(_, range)| range)
}

async fn post_reply(
    client: &Client,
    bot_token: &str,
    channel: &str,
    thread_ts: &str,
    text: &str,
) -> Result<(), String> {
    let response = client
        .post("https://slack.com/api/chat.postMessage")
        .header("Authorization", format!("Bearer {}", bot_token))
        .json(&serde_json::json!({
            "channel": channel,
            "text": text,
            "thread_ts": thread_ts,
        }))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let parsed: PostMessageResponse = serde_json::from_str(&body)
        .map_err(|e| format!("chat.postMessage returned bad JSON ({status}, {body:?}): {e}"))?;
    if !parsed.ok {
        return Err(format!(
            "Slack API error: {} ({})",
            parsed.error.unwrap_or_default(),
            status
        ));
    }
    Ok(())
}
