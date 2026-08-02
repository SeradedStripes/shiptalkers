use futures::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use tokio_tungstenite::{connect_async, tungstenite::Message};

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

pub struct SocketConfig {
    pub app_token: String,
    pub bot_token: String,
    pub main_channel: Option<String>,
    pub base_url: String,
}

pub async fn start_socket_mode(
    config: SocketConfig,
    clickhouse: clickhouse::Client,
    auth_db: std::sync::Arc<AuthDb>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = Client::new();

    let resp: ConnectionsOpenResponse = client
        .post("https://slack.com/api/apps.connections.open")
        .header("Authorization", format!("Bearer {}", config.app_token))
        .send()
        .await?
        .json()
        .await?;

    if !resp.ok {
        return Err(format!("Slack API error: {}", resp.error.unwrap_or_default()).into());
    }

    let ws_url = resp.url.ok_or("No WebSocket URL returned")?;
    tracing::info!("Connecting to Socket Mode...");

    let (mut ws_stream, _) = connect_async(&ws_url).await?;
    tracing::info!("Connected to Slack Socket Mode");

    if let Some(channel) = &config.main_channel {
        tracing::info!("Stats bot watching channel {}", channel);
    } else {
        tracing::warn!("SLACK_MAIN_CHANNEL not set, stats bot disabled");
    }

    while let Some(msg) = ws_stream.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Ok(socket_msg) = serde_json::from_str::<SocketMessage>(&text) {
                    match socket_msg.msg_type.as_str() {
                        "hello" => {
                            tracing::info!("Socket Mode handshake complete");
                        }
                        "events_api" => {
                            if let Some(payload) = &socket_msg.payload {
                                if let Some(event) = payload.get("event")
                                    && let Some(event_type) =
                                        event.get("type").and_then(|v| v.as_str())
                                {
                                    match event_type {
                                        "channel_created" => {
                                            handle_channel_created(&client, event, &clickhouse)
                                                .await;
                                        }
                                        "message" => {
                                            handle_message(&client, &config, &auth_db, event).await;
                                        }
                                        _ => {}
                                    }
                                }

                                if let Some(envelope_id) = &socket_msg.envelope_id {
                                    let ack = serde_json::json!({
                                        "envelope_id": envelope_id
                                    });
                                    let _ = ws_stream.send(Message::Text(ack.to_string())).await;
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
    auth_db: &std::sync::Arc<AuthDb>,
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
    tracing::info!(
        "Stats bot: message from {} in {} (thread reply to {})",
        user,
        msg.channel,
        msg.ts
    );
    tracing::debug!("Stats bot: message text: {:?}", text);

    let reply = if auth_db.is_linked(&user).await {
        tracing::info!("Stats bot: {} is linked, sending placeholder", user);
        "nah".to_string()
    } else {
        format!(
            "You aren't linked yet. Link your account here to get your stats: {}/link",
            config.base_url.trim_end_matches('/')
        )
    };

    if let Err(e) = post_reply(client, &config.bot_token, &msg.channel, &msg.ts, &reply).await {
        tracing::error!("Stats bot: failed to post reply: {}", e);
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
