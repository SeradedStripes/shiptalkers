use crate::db::sqlite::AuthDb;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub const SETTING_KEYS: &[&str] = &[
    "ADMIN_SLACK_IDS",
    "BASE_URL",
    "CLICKHOUSE_DB",
    "CLICKHOUSE_PASSWORD",
    "CLICKHOUSE_URL",
    "CLICKHOUSE_USER",
    "HACKATIME_CLIENT_ID",
    "HACKATIME_CLIENT_SECRET",
    "HCA_CLIENT_ID",
    "HCA_CLIENT_SECRET",
    "HOST",
    "PORT",
    "SESSION_SECRET",
    "SLACK_APP_TOKENS",
    "SLACK_BOT_TOKENS",
    "SLACK_CHANNEL_CONCURRENCY",
    "SLACK_MAIN_CHANNEL",
    "SLACK_MAX_INFLIGHT",
    "SLACK_REQUEST_DELAY_MS",
    "SLACK_THREAD_RESCAN_HOURS",
    "SLACK_THREAD_RESCAN_INTERVAL_HOURS",
    "SLACK_USER_SYNC_DELAY_MS",
    "SLACK_USER_TOKENS",
    "SQLITE_DB_PATH",
];

const SECRET_KEYS: &[&str] = &[
    "CLICKHOUSE_PASSWORD",
    "HACKATIME_CLIENT_SECRET",
    "HCA_CLIENT_SECRET",
    "SESSION_SECRET",
    "SLACK_APP_TOKENS",
    "SLACK_BOT_TOKENS",
    "SLACK_USER_TOKENS",
];

const RESTART_KEYS: &[&str] = &[
    "CLICKHOUSE_DB",
    "CLICKHOUSE_PASSWORD",
    "CLICKHOUSE_URL",
    "CLICKHOUSE_USER",
    "HOST",
    "PORT",
    "SLACK_APP_TOKENS",
    "SQLITE_DB_PATH",
];

const READONLY_KEYS: &[&str] = &["SQLITE_DB_PATH"];

fn default_value(key: &str) -> &str {
    match key {
        "SLACK_REQUEST_DELAY_MS" => "1200",
        "SLACK_MAX_INFLIGHT" => "8",
        "SLACK_CHANNEL_CONCURRENCY" => "8",
        "SLACK_USER_SYNC_DELAY_MS" => "3000",
        "SLACK_THREAD_RESCAN_HOURS" => "720",
        "SLACK_THREAD_RESCAN_INTERVAL_HOURS" => "6",
        "BASE_URL" => "http://localhost:3000",
        "CLICKHOUSE_URL" => "http://localhost:8123",
        "CLICKHOUSE_USER" => "default",
        "CLICKHOUSE_DB" => "default",
        "HOST" => "0.0.0.0",
        "PORT" => "3000",
        "SQLITE_DB_PATH" => "data/auth.db",
        _ => "",
    }
}

pub fn is_secret(key: &str) -> bool {
    SECRET_KEYS.contains(&key)
}

pub fn is_restart(key: &str) -> bool {
    RESTART_KEYS.contains(&key)
}

pub fn is_readonly(key: &str) -> bool {
    READONLY_KEYS.contains(&key)
}

/// Runtime-editable settings. Seeded from the SQLite settings table, falling
/// back to environment variables (with defaults) for keys never saved before.
/// All runtime subsystems read their knobs from here so admin edits apply
/// without a restart.
#[derive(Clone)]
pub struct RuntimeSettings {
    inner: Arc<RwLock<HashMap<String, String>>>,
}

impl RuntimeSettings {
    pub async fn load(auth_db: &AuthDb) -> Self {
        let mut map = HashMap::new();
        for key in SETTING_KEYS {
            let value = auth_db
                .get_setting(key)
                .await
                .or_else(|| std::env::var(key).ok())
                .unwrap_or_else(|| default_value(key).to_string());
            map.insert((*key).to_string(), value);
        }
        Self {
            inner: Arc::new(RwLock::new(map)),
        }
    }

    pub fn get(&self, key: &str) -> String {
        self.inner
            .read()
            .unwrap()
            .get(key)
            .cloned()
            .unwrap_or_default()
    }

    pub fn get_u64(&self, key: &str) -> u64 {
        self.get(key).parse().unwrap_or(0)
    }

    pub fn get_list(&self, key: &str) -> Vec<String> {
        self.get(key)
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }

    pub fn auth_config(&self) -> crate::auth::AuthConfig {
        crate::auth::AuthConfig {
            hca_client_id: self.get("HCA_CLIENT_ID"),
            hca_client_secret: self.get("HCA_CLIENT_SECRET"),
            hackatime_client_id: self.get("HACKATIME_CLIENT_ID"),
            hackatime_client_secret: self.get("HACKATIME_CLIENT_SECRET"),
            base_url: self.get("BASE_URL"),
            session_secret: self.get("SESSION_SECRET"),
        }
    }

    pub fn all(&self) -> Vec<(String, String)> {
        let mut rows: Vec<(String, String)> = self
            .inner
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows
    }

    pub async fn update(
        &self,
        auth_db: &AuthDb,
        entries: &[(String, String)],
    ) -> Result<(), String> {
        for (key, value) in entries {
            if is_readonly(key) {
                continue;
            }
            auth_db.set_setting(key, value).await?;
        }
        let mut guard = self.inner.write().unwrap();
        for (key, value) in entries {
            if !is_readonly(key) {
                guard.insert(key.clone(), value.clone());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn update_applies_immediately() {
        let db = Arc::new(crate::db::sqlite::AuthDb::open(":memory:").unwrap());
        let settings = RuntimeSettings::load(&db).await;
        settings
            .update(&db, &[("SLACK_REQUEST_DELAY_MS".into(), "777".into())])
            .await
            .unwrap();
        assert_eq!(settings.get("SLACK_REQUEST_DELAY_MS"), "777");
    }

    #[tokio::test]
    async fn readonly_keys_are_ignored() {
        let db = Arc::new(crate::db::sqlite::AuthDb::open(":memory:").unwrap());
        let settings = RuntimeSettings::load(&db).await;
        settings
            .update(&db, &[("SQLITE_DB_PATH".into(), "changed".into())])
            .await
            .unwrap();
        assert_eq!(settings.get("SQLITE_DB_PATH"), "data/auth.db");
    }
}
