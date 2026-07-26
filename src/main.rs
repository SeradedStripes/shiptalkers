mod db;
mod slack;
mod hackatime;
mod api;
mod website;

use dotenvy::dotenv;
use std::env;
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

    let db_for_scraper = database.clickhouse.clone();
    let slack_token_clone = slack_token.clone();

    tokio::spawn(async move {
        if let Err(e) = run_scraper(db_for_scraper, slack_token_clone).await {
            tracing::error!("Scraper error: {}", e);
        }
    });

    let host = env::var("HOST").unwrap_or_else(|_| "0.0.0.0".into());
    let port = env::var("PORT").unwrap_or_else(|_| "3000".into());
    let addr = format!("{}:{}", host, port);

    tracing::info!("Starting web server on {}", addr);
    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, website::router(database.clickhouse)).await?;

    Ok(())
}

async fn run_scraper(clickhouse: clickhouse::Client, slack_token: String) -> Result<(), String> {
    let slack_client = slack::SlackClient::new(slack_token);

    tracing::info!("Fetching all public channels...");
    let channels = slack_client.get_all_channels().await.map_err(|e| e.to_string())?;

    tracing::info!("Scraping message history from {} channels...", channels.len());
    let mut total_messages = 0;

    for (i, channel) in channels.iter().enumerate() {
        tracing::info!("[{}/{}] #{}", i + 1, channels.len(), channel.name);

        match slack_client.get_channel_history(&channel.id).await {
            Ok(messages) => {
                tracing::info!("  {} messages", messages.len());

                let rows: Vec<db::clickhouse_db::SlackMessageRow> = messages
                    .iter()
                    .map(|m| db::clickhouse_db::SlackMessageRow {
                        user_id: m.user.clone(),
                        channel_id: m.channel.clone(),
                        message_ts: m.ts.clone(),
                        text: m.text.clone(),
                    })
                    .collect();

                if !rows.is_empty() {
                    db::clickhouse_db::insert_messages(&clickhouse, &rows)
                        .await
                        .map_err(|e| e.to_string())?;
                }

                total_messages += messages.len();
            }
            Err(e) => {
                tracing::warn!("  failed: {}", e);
            }
        }
    }

    tracing::info!("Done scraping! Total messages: {}", total_messages);
    Ok(())
}
