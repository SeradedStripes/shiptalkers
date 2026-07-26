use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize)]
pub struct SlackMessage {
    pub user: String,
    pub text: String,
    pub ts: String,
    pub channel: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlackChannel {
    pub id: String,
    pub name: String,
}

pub struct SlackClient {
    client: Client,
    token: String,
    base_url: String,
}

impl SlackClient {
    pub fn new(token: String) -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .unwrap(),
            token,
            base_url: "https://slack.com/api".to_string(),
        }
    }

    async fn get(&self, method: &str, params: &[(String, String)]) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}/{}", self.base_url, method);

        loop {
            let response = self.client
                .get(&url)
                .header("Authorization", format!("Bearer {}", self.token))
                .query(params)
                .send()
                .await?;

            if response.status().as_u16() == 429 {
                let retry_after = response.headers()
                    .get("Retry-After")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(5);
                tracing::warn!("Rate limited on {}, waiting {} seconds", method, retry_after);
                tokio::time::sleep(Duration::from_secs(retry_after)).await;
                continue;
            }

            let parsed: serde_json::Value = response.json().await?;

            if let Some(error) = parsed.get("error") {
                if error.as_str() != Some("ok") {
                    return Err(format!("Slack API error: {}", error).into());
                }
            }

            return Ok(parsed);
        }
    }

    pub async fn fetch_channels_paginated<F>(&self, mut on_page: F) -> Result<usize, Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnMut(Vec<SlackChannel>) -> Pin<Box<dyn Future<Output = ()> + Send>>,
    {
        let mut total = 0;
        let mut cursor: Option<String> = None;

        loop {
            let mut params = vec![
                ("types".to_string(), "public_channel".to_string()),
                ("limit".to_string(), "200".to_string()),
            ];
            if let Some(ref c) = cursor {
                params.push(("cursor".to_string(), c.clone()));
            }

            let resp = self.get("conversations.list", &params).await?;

            let mut page_channels = Vec::new();
            if let Some(channels_arr) = resp.get("channels").and_then(|v| v.as_array()) {
                for ch in channels_arr {
                    if let (Some(id), Some(name)) = (ch.get("id"), ch.get("name")) {
                        page_channels.push(SlackChannel {
                            id: id.as_str().unwrap_or_default().to_string(),
                            name: name.as_str().unwrap_or_default().to_string(),
                        });
                    }
                }
            }

            total += page_channels.len();
            tracing::info!("Fetched {} channels ({} total)", page_channels.len(), total);

            on_page(page_channels).await;

            cursor = resp
                .get("response_metadata")
                .and_then(|m| m.get("next_cursor"))
                .and_then(|c| c.as_str())
                .filter(|c| !c.is_empty())
                .map(|c| c.to_string());

            match &cursor {
                Some(c) if !c.is_empty() => {}
                _ => break,
            }
        }

        Ok(total)
    }

    pub async fn get_channel_history(&self, channel_id: &str) -> Result<Vec<SlackMessage>, Box<dyn std::error::Error + Send + Sync>> {
        let mut messages = Vec::new();
        let mut cursor: Option<String> = None;

        for _page in 0..500 {
            let mut params = vec![
                ("channel".to_string(), channel_id.to_string()),
                ("limit".to_string(), "100".to_string()),
            ];
            if let Some(c) = &cursor {
                params.push(("cursor".to_string(), c.clone()));
            }

            let resp = self.get("conversations.history", &params).await?;

            if let Some(msgs) = resp.get("messages").and_then(|v| v.as_array()) {
                for msg in msgs {
                    if let (Some(user), Some(text), Some(ts)) = (
                        msg.get("user").and_then(|v| v.as_str()),
                        msg.get("text").and_then(|v| v.as_str()),
                        msg.get("ts").and_then(|v| v.as_str()),
                    ) {
                        messages.push(SlackMessage {
                            user: user.to_string(),
                            text: text.to_string(),
                            ts: ts.to_string(),
                            channel: channel_id.to_string(),
                        });
                    }
                }
            }

            let has_more = resp.get("has_more").and_then(|v| v.as_bool()).unwrap_or(false);
            if !has_more {
                break;
            }

            cursor = resp
                .get("response_metadata")
                .and_then(|m| m.get("next_cursor"))
                .and_then(|c| c.as_str())
                .filter(|c| !c.is_empty())
                .map(|c| c.to_string());

            if cursor.is_none() {
                break;
            }
        }

        Ok(messages)
    }
}
