mod db;
mod slack;
mod hackatime;
mod api;
mod website;

use dotenvy::dotenv;
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::future::Future;
use std::pin::Pin;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let slack_token = env::var("SLACK_BOT_TOKEN")?;
    let clickhouse_url = env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".into());
    let clickhouse_user = env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".into());
    let clickhouse_password = env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();
    let clickhouse_db = env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "default".into());
    let sqlite_path = env::var("SQLITE_PATH").unwrap_or_else(|_| "ship-talkers.db".into());

    let database = db::Database::new(&sqlite_path, &clickhouse_url, &clickhouse_user, &clickhouse_password, &clickhouse_db)?;
    database.init_sqlite()?;

    tracing::info!("Initializing ClickHouse tables...");
    db::clickhouse_db::init_tables(&database.clickhouse).await?;

    let clickhouse_for_scraper = database.clickhouse.clone();
    let slack_token_clone = slack_token.clone();

    tokio::spawn(async move {
        if let Err(e) = run_scraper(clickhouse_for_scraper, slack_token_clone).await {
            tracing::error!("Scraper error: {}", e);
        }
    });

    if let Ok(app_token) = env::var("SLACK_APP_TOKEN") {
        let clickhouse_for_socket = database.clickhouse.clone();
        tokio::spawn(async move {
            if let Err(e) = slack::start_socket_mode(app_token, clickhouse_for_socket).await {
                tracing::error!("Socket Mode error: {}", e);
            }
        });
    } else {
        tracing::warn!("SLACK_APP_TOKEN not set, Socket Mode disabled");
    }

    let host = env::var("HOST").unwrap_or_else(|_| "0.0.0.0".into());
    let port = env::var("PORT").unwrap_or_else(|_| "3000".into());
    let addr = format!("{}:{}", host, port);

    tracing::info!("Starting web server on {}", addr);
    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, website::router(database.clickhouse)).await?;

    Ok(())
}

fn insert_page(clickhouse: clickhouse::Client, page: Vec<slack::SlackChannel>) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async move {
        let rows: Vec<db::clickhouse_db::SlackChannelRow> = page
            .iter()
            .map(|ch| db::clickhouse_db::SlackChannelRow {
                channel_id: ch.id.clone(),
                name: ch.name.clone(),
                is_archived: ch.is_archived,
            })
            .collect();

        if let Err(e) = db::clickhouse_db::insert_new_channels(&clickhouse, &rows).await {
            tracing::error!("Failed to insert channels: {}", e);
        }
    })
}

async fn run_scraper(clickhouse: clickhouse::Client, slack_token: String) -> Result<(), String> {
    let slack_client = slack::SlackClient::new(slack_token);

    let existing = db::clickhouse_db::get_known_channel_ids(&clickhouse)
        .await
        .map_err(|e| e.to_string())?;

    if existing.is_empty() {
        tracing::info!("No channels in ClickHouse, fetching all from Slack...");
        full_fetch(&slack_client, &clickhouse).await?;
    } else {
        tracing::info!("{} channels already in ClickHouse, skipping initial fetch", existing.len());
    }

    tracing::info!("Starting message scraper...");
    scrape_all_messages(&slack_client, &clickhouse).await;

    tracing::info!("Will do a full rescan every 24 hours");
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(86400)).await;
        tracing::info!("24hr rescan starting...");
        full_fetch(&slack_client, &clickhouse).await?;
        scrape_all_messages(&slack_client, &clickhouse).await;
    }
}

async fn scrape_all_messages(slack_client: &slack::SlackClient, clickhouse: &clickhouse::Client) {
    let channels = match db::clickhouse_db::get_active_channel_ids(clickhouse).await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("Failed to get active channel IDs: {}", e);
            return;
        }
    };

    tracing::info!("Scraping messages for {} channels...", channels.len());
    let total_messages = Arc::new(AtomicU64::new(0));
    let scraped = Arc::new(AtomicU64::new(0));
    let concurrency = 3;
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let num_channels = channels.len();

    let mut handles = Vec::new();
    for channel_id in channels {
        let slack_client = slack_client.clone();
        let clickhouse = clickhouse.clone();
        let total_messages = total_messages.clone();
        let scraped = scraped.clone();
        let semaphore = semaphore.clone();

        let handle = tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();

            let oldest = db::clickhouse_db::get_scrape_state(&clickhouse, &channel_id).await
                .ok()
                .flatten()
                .map(|(ts, _)| ts);

            let oldest_ref = oldest.as_deref();
            let messages = match slack_client.get_channel_history(&channel_id, oldest_ref).await {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!("Failed to scrape channel {}: {}", channel_id, e);
                    return;
                }
            };

            if !messages.is_empty() {
                let rows: Vec<db::clickhouse_db::SlackMessageRow> = messages.iter().map(|m| {
                    db::clickhouse_db::SlackMessageRow {
                        user_id: m.user.clone(),
                        channel_id: m.channel.clone(),
                        message_ts: m.ts.clone(),
                        text: m.text.clone(),
                    }
                }).collect();

                let count = db::clickhouse_db::insert_messages(&clickhouse, &rows).await.unwrap_or(0);
                total_messages.fetch_add(count, Ordering::Relaxed);

                if let Some(last) = messages.last() {
                    let _ = db::clickhouse_db::update_scrape_state(
                        &clickhouse, &channel_id, &last.ts, rows.len() as u64
                    ).await;
                }
            }

            let done = scraped.fetch_add(1, Ordering::Relaxed) + 1;
            if done % 100 == 0 || done == num_channels as u64 {
                tracing::info!("Scraped {}/{} channels ({} messages so far)", done, num_channels, total_messages.load(Ordering::Relaxed));
            }

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    tracing::info!("Message scrape complete! {} total messages inserted", total_messages.load(Ordering::Relaxed));
}

async fn full_fetch(slack_client: &slack::SlackClient, clickhouse: &clickhouse::Client) -> Result<(), String> {
    let ch = clickhouse.clone();
    let total = slack_client
        .fetch_channels_paginated(move |page| insert_page(ch.clone(), page), None)
        .await
        .map_err(|e| e.to_string())?;

    tracing::info!("Fetching archived channel count...");
    match slack_client.get_archived_channel_count().await {
        Ok(count) => {
            if let Err(e) = db::clickhouse_db::set_metric(clickhouse, "archived_channels", count as u64).await {
                tracing::error!("Failed to store archived count: {}", e);
            } else {
                tracing::info!("Archived channels: {}", count);
            }
        }
        Err(e) => {
            tracing::warn!("Failed to get archived count: {}", e);
        }
    }

    tracing::info!("Full rescan done! {} total channels", total);
    Ok(())
}
