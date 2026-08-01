use clickhouse::Client;

pub mod clickhouse_db;

pub struct Database {
    pub clickhouse: Client,
}

impl Database {
    pub fn new(
        clickhouse_url: &str,
        clickhouse_user: &str,
        clickhouse_password: &str,
        clickhouse_db: &str,
    ) -> Self {
        let clickhouse = Client::default()
            .with_url(clickhouse_url)
            .with_user(clickhouse_user)
            .with_password(clickhouse_password)
            .with_database(clickhouse_db);

        Self { clickhouse }
    }
}
