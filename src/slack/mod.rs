mod socket;

pub use socket::start_socket_mode;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Serialize, Deserialize)]
pub struct SlackMessage {
    pub user: String,
    pub text: String,
    pub ts: String,
    pub channel: String,
    pub thread_ts: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlackChannel {
    pub id: String,
    pub name: String,
}

#[derive(Clone)]
pub struct SlackClient {
    client: Client,
    token: String,
    base_url: String,
    delay_between_requests: Duration,
    last_request_time: Arc<Mutex<Instant>>,
}

impl SlackClient {
    pub fn new(token: String, delay_between_requests: Duration) -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .unwrap(),
            token,
            base_url: "https://slack.com/api".to_string(),
            delay_between_requests,
            last_request_time: Arc::new(Mutex::new(Instant::now())),
        }
    }

    async fn get(&self, method: &str, params: &[(String, String)]) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}/{}", self.base_url, method);
        let mut retry_count = 0u32;

        loop {
            // Enforce minimum delay between requests
            let elapsed = self.last_request_time.lock().unwrap().elapsed();
            if elapsed < self.delay_between_requests {
                tokio::time::sleep(self.delay_between_requests - elapsed).await;
            }

            let response = self.client
                .get(&url)
                .header("Authorization", format!("Bearer {}", self.token))
                .query(params)
                .send()
                .await?;

            *self.last_request_time.lock().unwrap() = Instant::now();

            if response.status().as_u16() == 429 {
                let retry_after = response.headers()
                    .get("Retry-After")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<f64>().ok())
                    .unwrap_or(5.0);
                let backoff = (retry_after as u64).saturating_mul(2u64.saturating_pow(retry_count));
                let wait = backoff.min(60);
                tracing::warn!("Rate limited on {}, attempt {}, waiting {}s (retry_after={}s)", method, retry_count + 1, wait, retry_after);
                tokio::time::sleep(Duration::from_secs(wait)).await;
                retry_count += 1;
                continue;
            }

            let parsed: serde_json::Value = response.json().await?;

            if let Some(error) = parsed.get("error").and_then(|v| v.as_str()) {
                if error == "ratelimited" {
                    let retry_after = parsed.get("retry_after")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(5.0);
                    let backoff = (retry_after as u64).saturating_mul(2u64.saturating_pow(retry_count));
                    let wait = backoff.min(60);
                    tracing::warn!("Rate limited on {}, attempt {}, waiting {}s (retry_after={}s)", method, retry_count + 1, wait, retry_after);
                    tokio::time::sleep(Duration::from_secs(wait)).await;
                    retry_count += 1;
                    continue;
                }
                return Err(format!("Slack API error: {}", error).into());
            }

            return Ok(parsed);
        }
    }

    pub async fn fetch_channels_paginated<F>(&self, mut on_page: F, max_pages: Option<usize>) -> Result<usize, Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnMut(Vec<SlackChannel>) -> Pin<Box<dyn Future<Output = ()> + Send>>,
    {
        let mut total = 0;
        let mut cursor: Option<String> = None;
        let mut page_count = 0;

        loop {
            if let Some(max) = max_pages {
                if page_count >= max {
                    break;
                }
            }

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
            page_count += 1;
            tracing::info!("Fetched {} channels ({} total)", page_channels.len(), total);

            if total > 0 && page_channels.is_empty() {
                break;
            }

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

    pub async fn fetch_first_page(&self) -> Result<Vec<SlackChannel>, Box<dyn std::error::Error + Send + Sync>> {
        let params = vec![
            ("types".to_string(), "public_channel".to_string()),
            ("limit".to_string(), "200".to_string()),
        ];

        let resp = self.get("conversations.list", &params).await?;

        let mut channels = Vec::new();
        if let Some(channels_arr) = resp.get("channels").and_then(|v| v.as_array()) {
            for ch in channels_arr {
                if let (Some(id), Some(name)) = (ch.get("id"), ch.get("name")) {
                    channels.push(SlackChannel {
                        id: id.as_str().unwrap_or_default().to_string(),
                        name: name.as_str().unwrap_or_default().to_string(),
                    });
                }
            }
        }

        Ok(channels)
    }

    pub async fn get_channel_history(&self, channel_id: &str, oldest: Option<&str>) -> Result<Vec<SlackMessage>, Box<dyn std::error::Error + Send + Sync>> {
        self.fetch_paginated("conversations.history", channel_id, None, oldest).await
    }

    pub async fn fetch_thread_replies(&self, channel_id: &str, thread_ts: &str, oldest: Option<&str>) -> Result<Vec<SlackMessage>, Box<dyn std::error::Error + Send + Sync>> {
        self.fetch_paginated("conversations.replies", channel_id, Some(thread_ts), oldest).await
    }

    async fn fetch_paginated(
        &self,
        method: &str,
        channel_id: &str,
        thread_ts: Option<&str>,
        oldest: Option<&str>,
    ) -> Result<Vec<SlackMessage>, Box<dyn std::error::Error + Send + Sync>> {
        let mut messages = Vec::new();
        let mut cursor: Option<String> = None;
        let mut page = 0u32;

        loop {
            page += 1;
            let mut params = vec![
                ("channel".to_string(), channel_id.to_string()),
                ("limit".to_string(), "100".to_string()),
            ];
            if let Some(c) = &cursor {
                params.push(("cursor".to_string(), c.clone()));
            }
            if let Some(ts) = thread_ts {
                params.push(("ts".to_string(), ts.to_string()));
            }
            if let Some(o) = oldest {
                params.push(("oldest".to_string(), o.to_string()));
            }

            let resp = self.get(method, &params).await?;

            if let Some(msgs) = resp.get("messages").and_then(|v| v.as_array()) {
                for msg in msgs {
                    if let (Some(user), Some(text), Some(ts)) = (
                        msg.get("user").and_then(|v| v.as_str()),
                        msg.get("text").and_then(|v| v.as_str()),
                        msg.get("ts").and_then(|v| v.as_str()),
                    ) {
                        let thread = msg.get("thread_ts").and_then(|v| v.as_str());
                        messages.push(SlackMessage {
                            user: user.to_string(),
                            text: text.to_string(),
                            ts: ts.to_string(),
                            channel: channel_id.to_string(),
                            thread_ts: thread.map(|t| t.to_string()),
                        });
                    }
                }
            }

            let has_more = resp.get("has_more").and_then(|v| v.as_bool()).unwrap_or(false);
            if !has_more {
                if page > 1 && method != "conversations.replies" {
                    tracing::info!("Scraped {} ({} pages, {} messages)", channel_id, page, messages.len());
                }
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
