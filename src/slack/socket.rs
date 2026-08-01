use futures::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::db::clickhouse_db::{self, SlackChannelRow};

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
struct ConnectionsOpenResponse {
    ok: bool,
    url: Option<String>,
    error: Option<String>,
}

pub async fn start_socket_mode(
    app_token: String,
    clickhouse: clickhouse::Client,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = Client::new();

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
    tracing::info!("Connecting to Socket Mode...");

    let (mut ws_stream, _) = connect_async(&ws_url).await?;
    tracing::info!("Connected to Slack Socket Mode");

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
                                    && event_type == "channel_created"
                                    && let Ok(created) =
                                        serde_json::from_value::<ChannelCreated>(event.clone())
                                {
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
                                        clickhouse_db::insert_new_channels(&clickhouse, &[row])
                                            .await
                                    {
                                        tracing::error!("Failed to insert new channel: {}", e);
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
