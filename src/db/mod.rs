use rusqlite::Connection;
use clickhouse::Client;

pub mod sqlite;
pub mod clickhouse_db;

pub struct Database {
    pub sqlite: Connection,
    pub clickhouse: Client,
}

impl Database {
    pub fn new(
        sqlite_path: &str,
        clickhouse_url: &str,
        clickhouse_user: &str,
        clickhouse_password: &str,
        clickhouse_db: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let sqlite = Connection::open(sqlite_path)?;
        let clickhouse = Client::default()
            .with_url(clickhouse_url)
            .with_user(clickhouse_user)
            .with_password(clickhouse_password)
            .with_database(clickhouse_db);

        Ok(Self { sqlite, clickhouse })
    }

    pub fn init_sqlite(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.sqlite.execute_batch(
            "CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                slack_id TEXT NOT NULL UNIQUE,
                hackatime_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS oauth_tokens (
                id INTEGER PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                provider TEXT NOT NULL,
                access_token TEXT NOT NULL,
                refresh_token TEXT,
                expires_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS sync_state (
                id INTEGER PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                last_sync_at TIMESTAMP,
                channel_id TEXT
            );"
        )?;
        Ok(())
    }
}
