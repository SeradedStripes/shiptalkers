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
    let hackatime_client = hackatime::HackatimeClient::new(
        env::var("HACKATIME_URL").unwrap_or_else(|_| "https://hackatime.hackclub.com".into())
    );

    tracing::info!("Ship Talkers starting up...");

    // TODO: Set up scheduler to periodically sync data
    // TODO: Set up HTTP server for OAuth callbacks

    Ok(())
}
