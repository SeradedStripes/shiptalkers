mod socket;

pub use socket::SocketConfig;
pub use socket::start_socket_mode;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlackUser {
    pub id: String,
    pub display_name: String,

    pub pfp: String,
    pub updated: u64,
    pub is_bot: bool,
    pub is_deleted: bool,
}

struct Inner {
    tokens: f64,
    last: Instant,
    rate: f64,
    burst: f64,
    queue: VecDeque<u64>,
    next_ticket: u64,
}

pub struct RateLimiter {
    state: tokio::sync::Mutex<Inner>,
    notify: Notify,
}

impl RateLimiter {
    pub fn new(rate_per_sec: f64, burst: f64) -> Self {
        Self {
            state: tokio::sync::Mutex::new(Inner {
                tokens: burst,
                last: Instant::now(),
                rate: rate_per_sec,
                burst,
                queue: VecDeque::new(),
                next_ticket: 0,
            }),
            notify: Notify::new(),
        }
    }

    pub async fn acquire(&self) {
        let ticket = {
            let mut s = self.state.lock().await;
            let t = s.next_ticket;
            s.next_ticket += 1;
            s.queue.push_back(t);
            t
        };

        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let wait = {
                let mut s = self.state.lock().await;
                let now = Instant::now();
                s.tokens =
                    (s.tokens + now.duration_since(s.last).as_secs_f64() * s.rate).min(s.burst);
                s.last = now;

                if s.queue.front() == Some(&ticket) && s.tokens >= 1.0 {
                    s.tokens -= 1.0;
                    s.queue.pop_front();
                    self.notify.notify_waiters();
                    return;
                }

                if s.queue.front() == Some(&ticket) {
                    Some((1.0 - s.tokens) / s.rate)
                } else {
                    None
                }
            };

            match wait {
                Some(w) => {
                    tokio::select! {
                        _ = &mut notified => {}
                        _ = tokio::time::sleep(Duration::from_secs_f64(w)) => {}
                    }
                }
                None => {
                    notified.await;
                }
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

    async fn get(
        &self,
        method: &str,
        params: &[(String, String)],
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}/{}", self.base_url, method);
        let mut retry_count = 0u32;

        loop {
            self.limiter_for(method).acquire().await;

            let response = self
                .client
                .get(&url)
                .header("Authorization", format!("Bearer {}", self.token))
                .query(params)
                .send()
                .await?;

            if response.status().as_u16() == 429 {
                let retry_after = response
                    .headers()
                    .get("Retry-After")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<f64>().ok())
                    .unwrap_or(5.0);
                let backoff = (retry_after as u64).saturating_mul(2u64.saturating_pow(retry_count));
                let wait = backoff.min(60);
                tracing::warn!(
                    "Rate limited on {}, attempt {}, waiting {}s (retry_after={}s)",
                    method,
                    retry_count + 1,
                    wait,
                    retry_after
                );
                tokio::time::sleep(Duration::from_secs(wait)).await;
                retry_count += 1;
                continue;
            }

            let parsed: serde_json::Value = response.json().await?;

            if let Some(error) = parsed.get("error").and_then(|v| v.as_str()) {
                if error == "ratelimited" {
                    let retry_after = parsed
                        .get("retry_after")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(5.0);
                    let backoff =
                        (retry_after as u64).saturating_mul(2u64.saturating_pow(retry_count));
                    let wait = backoff.min(60);
                    tracing::warn!(
                        "Rate limited on {}, attempt {}, waiting {}s (retry_after={}s)",
                        method,
                        retry_count + 1,
                        wait,
                        retry_after
                    );
                    tokio::time::sleep(Duration::from_secs(wait)).await;
                    retry_count += 1;
                    continue;
                }
                return Err(format!("Slack API error: {}", error).into());
            }

            return Ok(parsed);
        }
    }

    pub async fn get_channel_history(
        &self,
        channel_id: &str,
        oldest: Option<&str>,
    ) -> Result<Vec<SlackMessage>, Box<dyn std::error::Error + Send + Sync>> {
        self.fetch_paginated("conversations.history", channel_id, None, oldest)
            .await
    }

    pub async fn fetch_thread_replies(
        &self,
        channel_id: &str,
        thread_ts: &str,
        oldest: Option<&str>,
    ) -> Result<Vec<SlackMessage>, Box<dyn std::error::Error + Send + Sync>> {
        self.fetch_paginated("conversations.replies", channel_id, Some(thread_ts), oldest)
            .await
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
        let mut seen_cursors = std::collections::HashSet::new();
        let mut page = 0u32;
        let start = Instant::now();

        loop {
            if let Some(c) = &cursor
                && !seen_cursors.insert(c.clone())
            {
                tracing::warn!(
                    "{}: pagination cursor {} repeated, stopping to avoid a loop",
                    channel_id,
                    c
                );
                break;
            }
            page += 1;
            if page == 1 {
                match oldest {
                    Some(ts) if !ts.is_empty() => tracing::debug!(
                        "Fetching {} for {} (incremental, oldest={})",
                        method,
                        channel_id,
                        ts
                    ),
                    _ => tracing::debug!("Fetching {} for {} (full)", method, channel_id),
                }
            }
            let mut params = vec![
                ("channel".to_string(), channel_id.to_string()),
                ("limit".to_string(), "999".to_string()),
            ];
            if let Some(c) = &cursor {
                params.push(("cursor".to_string(), c.clone()));
            }
            if let Some(ts) = thread_ts {
                params.push(("ts".to_string(), ts.to_string()));
            }
            if let Some(o) = oldest
                && !o.is_empty()
            {
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

            let what = thread_ts.map_or("channel".to_string(), |ts| format!("thread {}", ts));
            if page.is_multiple_of(10) {
                tracing::info!(
                    "{}: fetched page {} of {} ({} messages so far, {:.0}s)",
                    channel_id,
                    page,
                    what,
                    messages.len(),
                    start.elapsed().as_secs_f64()
                );
            }

            let has_more = resp
                .get("has_more")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if !has_more {
                if page > 1 {
                    tracing::info!(
                        "{}: {} complete ({} pages, {} messages, {:.0}s)",
                        channel_id,
                        what,
                        page,
                        messages.len(),
                        start.elapsed().as_secs_f64()
                    );
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

#[derive(Clone)]
pub struct SlackClientPool {
    clients: Vec<SlackClient>,
}

impl SlackClientPool {
    pub fn new(tokens: Vec<String>, delay_between_requests: Duration, max_inflight: usize) -> Self {
        Self {
            clients: tokens
                .into_iter()
                .map(|token| SlackClient::new(token, delay_between_requests, max_inflight))
                .collect(),
        }
    }

    fn client_for_page(&self, page: usize) -> &SlackClient {
        &self.clients[page % self.clients.len()]
    }

    pub async fn fetch_channels_paginated<F>(
        &self,
        mut on_page: F,
        max_pages: Option<usize>,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnMut(Vec<SlackChannel>) -> Pin<Box<dyn Future<Output = ()> + Send>>,
    {
        if self.clients.is_empty() {
            return Err("no bot tokens configured".into());
        }

        let mut total = 0;
        let mut cursor: Option<String> = None;
        let mut page_count = 0;

        loop {
            if max_pages.is_some_and(|max| page_count >= max) {
                break;
            }

            let client = self.client_for_page(page_count);

            let mut params = vec![
                ("types".to_string(), "public_channel".to_string()),
                ("limit".to_string(), "200".to_string()),
            ];
            if let Some(ref c) = cursor {
                params.push(("cursor".to_string(), c.clone()));
            }

            let resp = client.get("conversations.list", &params).await?;

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

    pub async fn fetch_users<F>(
        &self,
        mut on_page: F,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnMut(Vec<SlackUser>) -> Pin<Box<dyn Future<Output = ()> + Send>>,
    {
        if self.clients.is_empty() {
            return Err("no bot tokens configured".into());
        }

        let mut total = 0;
        let mut cursor: Option<String> = None;
        let mut page = 0u32;
        let mut batch = Vec::new();

        loop {
            page += 1;
            let client = self.client_for_page((page - 1) as usize);
            let mut params = vec![("limit".to_string(), "1000".to_string())];
            if let Some(ref c) = cursor {
                params.push(("cursor".to_string(), c.clone()));
            }

            let resp = client.get("users.list", &params).await?;

            let mut page_users = Vec::new();
            if let Some(members) = resp.get("members").and_then(|v| v.as_array()) {
                for m in members {
                    let Some(id) = m.get("id").and_then(|v| v.as_str()) else {
                        continue;
                    };
                    let is_bot = m.get("is_bot").and_then(|v| v.as_bool()).unwrap_or(false);
                    let deleted = m.get("deleted").and_then(|v| v.as_bool()).unwrap_or(false);
                    let profile = m.get("profile");
                    let display_name = profile
                        .and_then(|p| p.get("display_name"))
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .or_else(|| {
                            profile
                                .and_then(|p| p.get("real_name"))
                                .and_then(|v| v.as_str())
                                .filter(|s| !s.is_empty())
                        })
                        .unwrap_or("")
                        .to_string();
                    let pfp = ["image_192", "image_72", "image_48", "image_32", "image_24"]
                        .into_iter()
                        .find_map(|key| {
                            profile
                                .and_then(|p| p.get(key))
                                .and_then(|v| v.as_str())
                                .filter(|s| !s.is_empty())
                                .map(|s| s.to_string())
                        })
                        .unwrap_or_default();
                    let updated = m
                        .get("updated")
                        .and_then(|v| v.as_u64().or_else(|| v.as_f64().map(|f| f as u64)))
                        .unwrap_or(0);
                    let is_deleted = deleted || (display_name.is_empty() && !is_bot);
                    page_users.push(SlackUser {
                        id: id.to_string(),
                        display_name,
                        pfp,
                        updated,
                        is_bot,
                        is_deleted,
                    });
                }
            }

            total += page_users.len();
            batch.extend(page_users);
            if page.is_multiple_of(10) {
                on_page(std::mem::take(&mut batch)).await;
            }

            if page.is_multiple_of(25) {
                tracing::info!("users.list: fetched {} users so far (page {})", total, page);
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

        if !batch.is_empty() {
            on_page(batch).await;
        }

        tracing::info!("Fetched {} users from Slack", total);
        Ok(total)
    }
}
