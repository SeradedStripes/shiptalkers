mod socket;

pub use socket::start_socket_mode;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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

pub struct TokenBucket {
    tokens: f64,
    last: Instant,
    rate: f64,
    burst: f64,
}

pub struct RateLimiter {
    bucket: tokio::sync::Mutex<TokenBucket>,
}

impl RateLimiter {
    pub fn new(rate_per_sec: f64, burst: f64) -> Self {
        Self {
            bucket: tokio::sync::Mutex::new(TokenBucket {
                tokens: burst,
                last: Instant::now(),
                rate: rate_per_sec,
                burst,
            }),
        }
    }

    pub async fn acquire(&self) {
        loop {
            let wait = {
                let mut b = self.bucket.lock().await;
                let now = Instant::now();
                b.tokens = (b.tokens + now.duration_since(b.last).as_secs_f64() * b.rate).min(b.burst);
                b.last = now;
                if b.tokens >= 1.0 {
                    b.tokens -= 1.0;
                    None
                } else {
                    Some((1.0 - b.tokens) / b.rate)
                }
            };
            match wait {
                None => return,
                Some(w) => tokio::time::sleep(Duration::from_secs_f64(w)).await,
            }
        }
    }
}

#[derive(Clone)]
pub struct SlackClient {
    client: Client,
    token: String,
    base_url: String,
    delay_between_requests: Duration,
    max_inflight: usize,
    limiters: Arc<Mutex<HashMap<String, Arc<RateLimiter>>>>,
}

impl SlackClient {
    pub fn new(token: String, delay_between_requests: Duration, max_inflight: usize) -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .unwrap(),
            token,
            base_url: "https://slack.com/api".to_string(),
            delay_between_requests,
            max_inflight,
            limiters: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn limiter_for(&self, method: &str) -> Arc<RateLimiter> {
        let rate = 1.0 / self.delay_between_requests.as_secs_f64().max(0.001);
        let mut map = self.limiters.lock().unwrap();
        map.entry(method.to_string())
            .or_insert_with(|| Arc::new(RateLimiter::new(rate, self.max_inflight as f64)))
            .clone()
    }

    async fn get(&self, method: &str, params: &[(String, String)]) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}/{}", self.base_url, method);
        let mut retry_count = 0u32;

        loop {
            self.limiter_for(method).acquire().await;

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
        let start = Instant::now();

        loop {
            page += 1;
            if page == 1 {
                match oldest {
                    Some(ts) if !ts.is_empty() => tracing::debug!("Fetching {} for {} (incremental, oldest={})", method, channel_id, ts),
                    _ => tracing::debug!("Fetching {} for {} (full)", method, channel_id),
                }
            }
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
                if !o.is_empty() {
                    params.push(("oldest".to_string(), o.to_string()));
                }
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

            if page % 10 == 0 {
                tracing::info!("{}: fetched page {} of {} ({} messages so far, {:.0}s)", channel_id, page, if thread_ts.is_some() { format!("thread {}", thread_ts.unwrap()) } else { "channel".to_string() }, messages.len(), start.elapsed().as_secs_f64());
            }

            let has_more = resp.get("has_more").and_then(|v| v.as_bool()).unwrap_or(false);
            if !has_more {
                if page > 1 {
                    tracing::info!("{}: {} complete ({} pages, {} messages, {:.0}s)", channel_id, if thread_ts.is_some() { format!("thread {}", thread_ts.unwrap()) } else { "channel".to_string() }, page, messages.len(), start.elapsed().as_secs_f64());
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
