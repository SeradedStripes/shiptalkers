use std::collections::VecDeque;
use std::sync::Mutex;

use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::bot_image;
use crate::db::postgres_db::{self, SlackChannelRow};
use crate::settings::RuntimeSettings;
use crate::slack::time_range::{self, TimeRange, now_unix};

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
}

impl SocketConfig {
    pub fn new(app_tokens: Vec<String>) -> Self {
        Self { app_tokens }
    }
}

pub async fn start_socket_mode(
    config: SocketConfig,
    pool: sqlx::PgPool,
    settings: RuntimeSettings,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if config.app_tokens.is_empty() {
        return Err("No SLACK_APP_TOKENS set, Socket Mode disabled".into());
    }

    let main_channel = settings.get("SLACK_MAIN_CHANNEL");
    if main_channel.is_empty() {
        tracing::warn!("SLACK_MAIN_CHANNEL not set, stats bot disabled");
    } else {
        tracing::info!("Stats bot watching channel {}", main_channel);
    }

    let num_sockets = config.app_tokens.len();
    let mut sockets = Vec::with_capacity(num_sockets);
    for (socket_idx, app_token) in config.app_tokens.iter().enumerate() {
        let pool = pool.clone();
        let settings = settings.clone();
        sockets.push(run_socket(
            socket_idx,
            num_sockets,
            app_token.clone(),
            pool,
            settings,
        ));
    }

    let results = futures_util::future::join_all(sockets).await;
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
    pool: sqlx::PgPool,
    settings: RuntimeSettings,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = Client::new();
    let mut failures = 0u32;

    // Track recently-processed (channel, ts) so an event Slack redelivers is only handled once.
    let seen: Mutex<VecDeque<(String, String, i64)>> = Mutex::new(VecDeque::new());

    loop {
        match serve_socket(
            &client,
            socket_idx,
            num_sockets,
            &app_token,
            &pool,
            &settings,
            &seen,
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
    pool: &sqlx::PgPool,
    settings: &RuntimeSettings,
    seen: &Mutex<VecDeque<(String, String, i64)>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let resp: ConnectionsOpenResponse = client
        .post("https://slack.com/api/apps.connections.open")
        .header("Authorization", format!("Bearer {}", app_token))
        .timeout(std::time::Duration::from_secs(30))
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

    let (mut ws_stream, _) =
        tokio::time::timeout(std::time::Duration::from_secs(30), connect_async(&ws_url))
            .await
            .map_err(|_| "Timed out connecting to Socket Mode WebSocket".to_string())??;
    tracing::info!(
        "Connected to Slack Socket Mode (app {}/{})",
        socket_idx + 1,
        num_sockets
    );

    loop {
        let received =
            tokio::time::timeout(std::time::Duration::from_secs(60), ws_stream.next()).await;
        match received {
            Err(_) => {
                tracing::warn!(
                    "Socket Mode connection idle, reconnecting (app {}/{})",
                    socket_idx + 1,
                    num_sockets
                );
                break;
            }
            Ok(None) => break,
            Ok(Some(Err(e))) => {
                tracing::error!("WebSocket error: {}", e);
                break;
            }
            Ok(Some(Ok(Message::Text(text)))) => {
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
                                let _ = ws_stream.send(Message::Text(ack.to_string().into())).await;
                            }

                            if let Some(payload) = &socket_msg.payload
                                && let Some(event) = payload.get("event")
                                && let Some(event_type) = event.get("type").and_then(|v| v.as_str())
                            {
                                match event_type {
                                    "channel_created" => {
                                        handle_channel_created(client, event, pool).await;
                                    }
                                    "message" => {
                                        handle_message(
                                            client,
                                            socket_idx,
                                            num_sockets,
                                            pool,
                                            settings,
                                            event,
                                            seen,
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
            Ok(Some(Ok(Message::Ping(_)))) => {
                let _ = ws_stream.send(Message::Pong(vec![].into())).await;
            }
            Ok(Some(Ok(_))) => {}
        }
    }

    Ok(())
}

const SEEN_TTL_SECS: i64 = 600;
const SEEN_MAX: usize = 512;

/// Marks a (channel, ts) as handled. Returns true if this is the first time we have seen it, false for any redelivery. Prunes stale entries on insert.
fn mark_seen(seen: &Mutex<VecDeque<(String, String, i64)>>, channel: &str, ts: &str) -> bool {
    let now = now_unix();
    let mut guard = seen.lock().unwrap_or_else(|p| p.into_inner());
    guard.retain(|(_, _, at)| now.saturating_sub(*at) < SEEN_TTL_SECS);
    if guard.iter().any(|(c, t, _)| c == channel && t == ts) {
        return false;
    }
    guard.push_back((channel.to_string(), ts.to_string(), now));
    if guard.len() > SEEN_MAX {
        guard.pop_front();
    }
    true
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

async fn handle_channel_created(_client: &Client, event: &serde_json::Value, pool: &sqlx::PgPool) {
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

    if let Err(e) =
        postgres_db::insert_new_channels(pool, &[row], &mut std::collections::HashSet::new()).await
    {
        tracing::error!("Failed to insert new channel: {}", e);
    }
}

async fn handle_message(
    client: &Client,
    socket_idx: usize,
    num_sockets: usize,
    pool: &sqlx::PgPool,
    settings: &RuntimeSettings,
    event: &serde_json::Value,
    seen: &Mutex<VecDeque<(String, String, i64)>>,
) {
    let main_channel = settings.get("SLACK_MAIN_CHANNEL");
    if main_channel.is_empty() {
        return;
    };

    let Ok(msg) = serde_json::from_value::<MessageEvent>(event.clone()) else {
        return;
    };

    // Skip events we already answered: Socket Mode can redeliver the same message (e.g. on reconnect), and every delivery would post a reply.
    if !mark_seen(seen, &msg.channel, &msg.ts) {
        return;
    }

    if msg.channel != main_channel {
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

    let sender = msg.user.unwrap_or_default();
    let text = msg.text.unwrap_or_default();

    let Some(range) = time_range::parse_time_range_at(&text, now_unix()) else {
        return;
    };
    let user = extract_mentioned_user(&text).unwrap_or_else(|| sender.clone());
    tracing::info!(
        "Stats bot: stats request for {} (from {}) in {} ({:?})",
        user,
        sender,
        msg.channel,
        text
    );

    let Some(bot_token) = settings.get_list("SLACK_BOT_TOKENS").first().cloned() else {
        tracing::warn!("Stats bot: no bot tokens configured, skipping reply");
        return;
    };
    let base_url = settings.get("BASE_URL");

    // If we have no usable coding data on the user (private or no-account
    // profile, never synced, or a genuine zero total), still show their
    // slack time as a card and explain the situation.
    if !has_coding_data(pool, &user).await {
        tracing::info!(
            user,
            range = range.label().as_str(),
            message = text.as_str(),
            "Stats bot: no coding data for {}, sending slack-only card",
            user
        );
        let slack_seconds = query_slack_seconds(pool, &user, &range).await;
        let user_name = user_display_name(pool, &user).await;
        let slack_time = fmt_span(slack_seconds);

        let image = bot_image::SlackOnlyImage {
            user: &user_name,
            slack_time: &slack_time,
        };
        match bot_image::render_slack_only_image(&image) {
            Ok(png) => {
                if let Err(e) = upload_image(client, &bot_token, &msg.channel, &msg.ts, png).await {
                    tracing::error!("Stats bot: failed to upload slack-only image: {}", e);
                }
            }
            Err(e) => {
                tracing::error!("Stats bot: failed to render slack-only image: {}", e);
            }
        }

        let link = format!("{}/link", base_url.trim_end_matches('/'));
        let reply = format!(
            "No Hackatime Data available, your coding time is either private or you have none. \
             If it is private link your account here to see your stats: {link}, \
             if you have no coding time then get coding :thumbs-up:. \
             For now here's just your slack time data"
        );
        if let Err(e) = post_reply(client, &bot_token, &msg.channel, &msg.ts, &reply).await {
            tracing::error!("Stats bot: failed to post reply: {}", e);
        }
        return;
    }

    let (slack_seconds, coding_seconds) = query_stats(pool, &user, &range, &text).await;
    let user_name = user_display_name(pool, &user).await;

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
        user,
        range = range.label().as_str(),
        message = text.as_str(),
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

    if let Err(e) = upload_image(client, &bot_token, &msg.channel, &msg.ts, png).await {
        tracing::error!("Stats bot: failed to upload stats image: {}", e);
    }
}

async fn query_stats(
    pool: &sqlx::PgPool,
    user: &str,
    range: &TimeRange,
    message: &str,
) -> (u64, u64) {
    let slack = query_slack_seconds(pool, user, range).await;
    let coding = query_coding_seconds(pool, user, range, message).await;
    (slack, coding)
}

async fn query_slack_seconds(pool: &sqlx::PgPool, user: &str, range: &TimeRange) -> u64 {
    let boundary = crate::sessionize::SESSION_GAP_BOUNDARY_SECS;
    let rate = crate::sessionize::MESSAGE_TYPING_CHARS_PER_SEC;
    let overhead = crate::sessionize::MESSAGE_READ_OVERHEAD_SECS;
    let max_secs = crate::sessionize::SESSION_MAX_SECS;
    let mut session_sql = String::from(
        "WITH
         msg AS (
             SELECT message_ts / 1000000 AS ts,
                    sum(char_length(text)) AS chars,
                    count(*) AS msgs
             FROM slack_messages_by_user
             WHERE user_id = $1",
    );
    if range.start_ts().is_some() {
        session_sql.push_str(" AND message_ts / 1000000 >= $2");
    }
    if range.end_ts().is_some() {
        session_sql.push_str(&format!(
            " AND message_ts / 1000000 < ${}",
            if range.start_ts().is_some() { 3 } else { 2 }
        ));
    }
    session_sql.push_str(&format!(
        " GROUP BY ts
         ),
         flagged AS (
             SELECT ts, chars, msgs,
                    CASE WHEN ts - lag(ts) OVER (ORDER BY ts) > {boundary} THEN 1 ELSE 0 END AS boundary
             FROM msg
         ),
         sess AS (
             SELECT ts, chars, msgs,
                    sum(boundary) OVER (ORDER BY ts) AS sid
             FROM flagged
         ),
         sessions AS (
             SELECT min(ts) AS start_ts, max(ts) AS end_ts,
                    (array_agg(chars ORDER BY ts))[1] AS first_chars,
                    (array_agg(msgs ORDER BY ts))[1] AS first_msgs
             FROM sess
             GROUP BY sid
         )
         SELECT sum(least(end_ts - start_ts + (first_chars + {rate} - 1) / {rate} + first_msgs * {overhead}, {max_secs}))::bigint AS total_time
         FROM sessions"
    ));

    let mut session_query = sqlx::query_scalar::<_, Option<i64>>(&session_sql);
    session_query = session_query.bind(user);
    if let Some(start_ts) = range.start_ts() {
        session_query = session_query.bind(start_ts);
    }
    if let Some(end_ts) = range.end_ts() {
        session_query = session_query.bind(end_ts);
    }

    session_query
        .fetch_one(pool)
        .await
        .ok()
        .flatten()
        .unwrap_or(0)
        .max(0) as u64
}

/// Coding seconds for a user within the requested range, summed as the exact
/// overlap of the `hackatime_spans` rows with the range (spans crossing the
/// boundaries only count the part inside). When the user has no span rows yet
/// (never synced), falls back to the all-time `total_minutes` so the card
/// still shows a number.
async fn query_coding_seconds(
    pool: &sqlx::PgPool,
    user: &str,
    range: &TimeRange,
    message: &str,
) -> u64 {
    let has_rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM hackatime_spans WHERE slack_id = $1")
            .bind(user)
            .fetch_one(pool)
            .await
            .unwrap_or(0);
    if has_rows == 0 {
        tracing::info!(
            user,
            range = range.label().as_str(),
            message,
            "query_coding_seconds: no hackatime_spans rows, falling back to total_minutes"
        );
        return query_total_minutes(pool, user).await * 60;
    }
    let (sql, binds) = build_coding_query(range);
    let start_date = range.start_date().unwrap_or_default();
    let end_date = range.end_date().unwrap_or_default();
    let mut query = sqlx::query_scalar::<_, Option<i64>>(&sql);
    for b in &binds {
        query = query.bind(*b);
    }
    query = query.bind(user);
    match query.fetch_one(pool).await {
        Ok(secs) => {
            let secs = secs.unwrap_or(0).max(0) as u64;
            tracing::info!(
                user,
                range = range.label().as_str(),
                message,
                start_date,
                end_date,
                secs,
                "query_coding_seconds: result"
            );
            secs
        }
        Err(e) => {
            tracing::info!(
                user,
                range = range.label().as_str(),
                message,
                start_date,
                end_date,
                error = %e,
                sql = sql.as_str(),
                ?binds,
                "query_coding_seconds: fetch failed, returning 0"
            );
            0
        }
    }
}

/// Builds the SQL expression and bind values for the coding-overlap query.
/// The returned `sql` always ends with a final placeholder bound to the
/// `slack_id`, which the caller must bind after the range values in `binds`.
pub fn build_coding_query(range: &TimeRange) -> (String, Vec<i64>) {
    let span_start = "start_ts";
    let span_end = "(start_ts + duration)";
    let (expr, binds) = match (range.start_ts(), range.end_ts()) {
        (Some(start), Some(end)) => (
            format!(
                "sum(CASE WHEN {span_end} > $1 AND {span_start} < $2 \
                 THEN least({span_end}, $2) - greatest({span_start}, $1) ELSE 0 END)"
            ),
            vec![start, end],
        ),
        (Some(start), None) => (
            format!(
                "sum(CASE WHEN {span_end} > $1 THEN {span_end} - greatest({span_start}, $1) ELSE 0 END)"
            ),
            vec![start],
        ),
        (None, _) => (String::from("sum(duration)"), Vec::new()),
    };
    let next = binds.len() + 1;
    let sql = format!("SELECT {expr}::bigint FROM hackatime_spans WHERE slack_id = ${next}");
    (sql, binds)
}

async fn query_total_minutes(pool: &sqlx::PgPool, user: &str) -> u64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT total_minutes FROM hackatime_connections WHERE slack_id = $1",
    )
    .bind(user)
    .fetch_one(pool)
    .await
    .unwrap_or(0)
    .max(0) as u64
}

/// Whether the stats bot has usable all-time coding data on a user: a synced
/// `hackatime_connections` row (empty `status`; `private`/`no_account` rows
/// carry none) with a nonzero total. The 30m resync loop fills this in, so a
/// fresh user may get the prompt until their first sync lands.
async fn has_coding_data(pool: &sqlx::PgPool, user: &str) -> bool {
    let row: Option<(String, i64)> = sqlx::query_as(
        "SELECT status, total_minutes FROM hackatime_connections WHERE slack_id = $1",
    )
    .bind(user)
    .fetch_optional(pool)
    .await
    .unwrap_or(None);
    match row {
        Some((status, total_minutes)) => status.is_empty() && total_minutes > 0,
        None => false,
    }
}

async fn user_display_name(pool: &sqlx::PgPool, user: &str) -> String {
    let row: Option<(String, i16)> =
        sqlx::query_as("SELECT display_name, is_deleted FROM users WHERE user_id = $1")
            .bind(user)
            .fetch_optional(pool)
            .await
            .unwrap_or(None);
    match row {
        Some((_, 1)) => "Deleted account".to_string(),
        Some((display_name, _)) if !display_name.is_empty() => display_name,
        _ => user.to_string(),
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

/// Extracts the first user ID from Slack mention syntax (`<@U123456>` or
/// `<@U123456|display_name>`) in the message text.
fn extract_mentioned_user(text: &str) -> Option<String> {
    let start = text.find("<@")?;
    let after_at = start + 2;
    let id_end = text[after_at..].find('>')? + after_at;
    let id = &text[after_at..id_end];
    let id = id.split('|').next()?;
    if id.is_empty() {
        return None;
    }
    Some(id.to_string())
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
