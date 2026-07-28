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

    tracing::info!("Will do a full rescan every 24 hours");
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(86400)).await;
        tracing::info!("24hr rescan starting...");
        full_fetch(&slack_client, &clickhouse).await?;
    }
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
