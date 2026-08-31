use ship_talkers::{db, settings, slack, website};

use dotenvy::dotenv;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    ship_talkers::init_tls();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let settings = settings::RuntimeSettings::load();

    let database_url = settings.get("DATABASE_URL");
    if database_url.is_empty() {
        return Err("DATABASE_URL is not set. Point it at your Postgres instance.".into());
    }
    tracing::info!("Connecting to Postgres...");
    let pool = db::postgres_db::connect(&database_url).await?;
    db::postgres_db::init_tables(&pool).await?;
    let auth_db = std::sync::Arc::new(db::postgres_db::AuthDb::new(pool.clone()));

    let has_app_tokens = !settings.get_list("SLACK_APP_TOKENS").is_empty();

    {
        let pool_for_words = pool.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = db::refresh::refresh_word_totals(&pool_for_words).await {
                    tracing::warn!("Failed to refresh word totals: {}", e);
                }
                tokio::time::sleep(std::time::Duration::from_secs(30 * 60)).await;
            }
        });
    }

    {
        let pool_for_daily = pool.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = db::refresh::refresh_daily_stats(&pool_for_daily).await {
                    tracing::warn!("Failed to refresh daily stats: {}", e);
                }
                tokio::time::sleep(std::time::Duration::from_secs(30 * 60)).await;
            }
        });
    }

    {
        let pool_for_stats = pool.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = db::refresh::refresh_page_stats(&pool_for_stats).await {
                    tracing::warn!("Failed to refresh page stats: {}", e);
                }
                tokio::time::sleep(std::time::Duration::from_secs(30 * 60)).await;
            }
        });
    }

    if has_app_tokens {
        let socket_config = slack::SocketConfig::new(settings.get_list("SLACK_APP_TOKENS"));
        let pool_for_socket = pool.clone();
        let settings_for_socket = settings.clone();
        tokio::spawn(async move {
            if let Err(e) =
                slack::start_socket_mode(socket_config, pool_for_socket, settings_for_socket).await
            {
                tracing::error!("Socket Mode error: {}", e);
            }
        });
    } else {
        tracing::warn!("SLACK_APP_TOKENS not set, Socket Mode disabled");
    }

    let addr = format!("{}:{}", settings.get("HOST"), settings.get("PORT"));

    tracing::info!("Starting web server on {}", addr);
    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, website::router(pool, settings, auth_db))
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("Received shutdown signal, stopping");
}
