mod db;
mod slack;
mod hackatime;
mod api;

use dotenvy::dotenv;
use std::env;

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
    let sqlite_path = env::var("SQLITE_PATH").unwrap_or_else(|_| "ship-talkers.db".into());

    let database = db::Database::new(&sqlite_path, &clickhouse_url)?;
    database.init_sqlite()?;

    let slack_client = slack::SlackClient::new(slack_token);

    tracing::info!("Fetching all public channels...");
    let channels = slack_client.get_all_channels().await?;

    tracing::info!("Scraping message history from {} channels...", channels.len());
    let mut total_messages = 0;

    for (i, channel) in channels.iter().enumerate() {
        tracing::info!("[{}/{}] #{}", i + 1, channels.len(), channel.name);

        match slack_client.get_channel_history(&channel.id).await {
            Ok(messages) => {
                tracing::info!("  {} messages", messages.len());
                total_messages += messages.len();
            }
            Err(e) => {
                tracing::warn!("  failed: {}", e);
            }
        }
    }

    tracing::info!("Done! Total messages: {}", total_messages);
    Ok(())
}
