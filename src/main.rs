mod db;
mod slack;
mod hackatime;
mod api;
mod website;

use dotenvy::dotenv;
use std::env;
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
    let slack_user_token = env::var("SLACK_USER_TOKEN").ok();
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
        if let Err(e) = run_scraper(clickhouse_for_scraper, slack_token_clone, slack_user_token).await {
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
            })
            .collect();

        if let Err(e) = db::clickhouse_db::insert_new_channels(&clickhouse, &rows).await {
            tracing::error!("Failed to insert channels: {}", e);
        }
    })
}

async fn run_scraper(clickhouse: clickhouse::Client, slack_token: String, slack_user_token: Option<String>) -> Result<(), String> {
    let slack_client = slack::SlackClient::new(slack_token, std::time::Duration::from_millis(200));
    let user_client = slack_user_token.map(|t| slack::SlackClient::new(t, std::time::Duration::from_secs(1)));

    let existing = db::clickhouse_db::get_known_channel_ids(&clickhouse)
        .await
        .map_err(|e| e.to_string())?;

    if existing.is_empty() {
        tracing::info!("No channels in ClickHouse, fetching all from Slack...");
        full_fetch(&slack_client, &clickhouse).await?;
    } else {
        tracing::info!("{} channels already in ClickHouse, skipping initial fetch", existing.len());
    }

    if let Some(ref user_client) = user_client {
        tracing::info!("Starting message scraper with user token...");
        scrape_all_messages(user_client, &clickhouse).await;
    } else {
        tracing::warn!("SLACK_USER_TOKEN not set, message scraping disabled");
    }

    tracing::info!("Will do a full rescan and rescrape every 24 hours");
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(86400)).await;
        tracing::info!("24hr rescan starting...");
        full_fetch(&slack_client, &clickhouse).await?;
        if let Some(ref user_client) = user_client {
            scrape_all_messages(user_client, &clickhouse).await;
        }
    }
}

async fn scrape_all_messages(user_client: &slack::SlackClient, clickhouse: &clickhouse::Client) {
    let channels = match db::clickhouse_db::get_known_channel_ids(clickhouse).await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("Failed to get channel IDs: {}", e);
            return;
        }
    };

    tracing::info!("Scraping messages for {} channels...", channels.len());
    let mut total = 0u64;

    for (i, channel_id) in channels.iter().enumerate() {
        let fully_scraped = db::clickhouse_db::is_fully_scraped(clickhouse, channel_id).await.unwrap_or(false);

        let oldest = match db::clickhouse_db::get_max_message_ts(clickhouse, channel_id).await {
            Ok(Some(ts)) => {
                tracing::info!("[{}/{}] Scraping channel {} (fully={}, oldest={})", i + 1, channels.len(), channel_id, fully_scraped, ts);
                Some(ts)
            }
            Ok(None) => {
                tracing::info!("[{}/{}] Scraping channel {} (fully={}, oldest=None, fresh)", i + 1, channels.len(), channel_id, fully_scraped);
                None
            }
            Err(e) => {
                tracing::warn!("[{}/{}] Failed to get max ts for {}: {}, doing full scrape", i + 1, channels.len(), channel_id, e);
                None
            }
        };

        let messages = match user_client.get_channel_history(channel_id, oldest.as_deref()).await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("[{}/{}] Failed to scrape {}: {}", i + 1, channels.len(), channel_id, e);
                continue;
            }
        };

        // Filter out messages at or before the oldest timestamp to avoid duplicates
        let messages: Vec<_> = if let Some(ref oldest_ts) = oldest {
            messages.into_iter().filter(|m| m.ts > *oldest_ts).collect()
        } else {
            messages
        };

        // Collect unique thread parents from replies and fetch full thread replies
        let mut thread_parents: Vec<String> = Vec::new();
        for msg in &messages {
            if let Some(ref t) = msg.thread_ts {
                if t != &msg.ts && !thread_parents.contains(t) {
                    thread_parents.push(t.clone());
                }
            }
        }

        if !thread_parents.is_empty() {
            tracing::info!("[{}/{}] Found {} threads to scrape in {}", i + 1, channels.len(), thread_parents.len(), channel_id);
        }

        for thread_ts in &thread_parents {
            let thread_fully = db::clickhouse_db::is_thread_fully_scraped(clickhouse, channel_id, thread_ts).await.unwrap_or(false);
            let thread_oldest = if thread_fully {
                db::clickhouse_db::get_max_thread_reply_ts(clickhouse, channel_id, thread_ts).await
                    .ok()
                    .flatten()
            } else {
                None
            };

            match user_client.fetch_thread_replies(channel_id, thread_ts, thread_oldest.as_deref()).await {
                Ok(replies) => {
                    let replies: Vec<_> = if let Some(ref o) = thread_oldest {
                        replies.into_iter().filter(|m| m.ts > *o).collect()
                    } else {
                        replies
                    };

                    if !replies.is_empty() {
                        let rows: Vec<db::clickhouse_db::SlackMessageRow> = replies.iter().map(|m| {
                            db::clickhouse_db::SlackMessageRow {
                                user_id: m.user.clone(),
                                channel_id: m.channel.clone(),
                                message_ts: m.ts.clone(),
                                text: m.text.clone(),
                                thread_ts: m.thread_ts.clone(),
                            }
                        }).collect();

                        let count = db::clickhouse_db::insert_messages(clickhouse, &rows).await.unwrap_or(0);
                        total += count;
                        tracing::info!("[{}/{}] Inserted {} thread replies from thread {} in {}", i + 1, channels.len(), count, thread_ts, channel_id);
                    }

                    if thread_oldest.is_none() {
                        if let Err(e) = db::clickhouse_db::mark_thread_fully_scraped(clickhouse, channel_id, thread_ts).await {
                            tracing::warn!("[{}/{}] Failed to mark thread {} as scraped: {}", i + 1, channels.len(), thread_ts, e);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("[{}/{}] Failed to scrape thread {} in {}: {}", i + 1, channels.len(), thread_ts, channel_id, e);
                }
            }
        }

        let rows: Vec<db::clickhouse_db::SlackMessageRow> = messages.iter().map(|m| {
            db::clickhouse_db::SlackMessageRow {
                user_id: m.user.clone(),
                channel_id: m.channel.clone(),
                message_ts: m.ts.clone(),
                text: m.text.clone(),
                thread_ts: m.thread_ts.clone(),
            }
        }).collect();

        if !rows.is_empty() {
            let count = db::clickhouse_db::insert_messages(clickhouse, &rows).await.unwrap_or(0);
            total += count;
            tracing::info!("[{}/{}] Inserted {} messages from {}", i + 1, channels.len(), count, channel_id);
        } else {
            tracing::info!("[{}/{}] No new messages from {}", i + 1, channels.len(), channel_id);
        }

        if !fully_scraped && oldest.is_none() {
            if let Err(e) = db::clickhouse_db::mark_fully_scraped(clickhouse, channel_id).await {
                tracing::warn!("[{}/{}] Failed to mark {} as fully scraped: {}", i + 1, channels.len(), channel_id, e);
            }
        }
    }

    tracing::info!("Message scrape complete! {} new messages", total);
}

async fn full_fetch(slack_client: &slack::SlackClient, clickhouse: &clickhouse::Client) -> Result<(), String> {
    let ch = clickhouse.clone();
    let total = slack_client
        .fetch_channels_paginated(move |page| insert_page(ch.clone(), page), None)
        .await
        .map_err(|e| e.to_string())?;

    tracing::info!("Full rescan done! {} total channels", total);
    Ok(())
}
