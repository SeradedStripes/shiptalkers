use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub const SETTING_KEYS: &[&str] = &[
    "BASE_URL",
    "DATABASE_URL",
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
];

fn default_value(key: &str) -> &str {
    match key {
        "SLACK_REQUEST_DELAY_MS" => "1200",
        "SLACK_MAX_INFLIGHT" => "8",
        "SLACK_CHANNEL_CONCURRENCY" => "8",
        "SLACK_USER_SYNC_DELAY_MS" => "3000",
        "SLACK_THREAD_RESCAN_HOURS" => "720",
        "SLACK_THREAD_RESCAN_INTERVAL_HOURS" => "6",
        "BASE_URL" => "http://localhost:3000",
        "HOST" => "0.0.0.0",
        "PORT" => "3000",
        _ => "",
    }
}

/// Keys that hold comma-separated lists. These also accept numbered variants
/// (`SLACK_BOT_TOKENS_1`, `SLACK_BOT_TOKENS_2`, ...) so long tokens can be
/// added as short separate lines instead of one giant line.
const LIST_KEYS: &[&str] = &["SLACK_APP_TOKENS", "SLACK_BOT_TOKENS", "SLACK_USER_TOKENS"];
const MAX_LIST_VARIANTS: u32 = 64;

/// Settings read from environment variables at startup, with defaults for keys
/// that are unset. All subsystems read their knobs from here.
#[derive(Clone)]
pub struct RuntimeSettings {
    inner: Arc<RwLock<HashMap<String, String>>>,
    set_keys: Arc<RwLock<std::collections::HashSet<String>>>,
}

impl RuntimeSettings {
    pub fn load() -> Self {
        Self::from_env(|key| std::env::var(key).ok())
    }

    pub fn from_env(env: impl Fn(&str) -> Option<String>) -> Self {
        let mut map = HashMap::new();
        let mut set_keys = std::collections::HashSet::new();
        for key in SETTING_KEYS {
            let value = match env(key) {
                Some(v) => {
                    set_keys.insert((*key).to_string());
                    v
                }
                None => default_value(key).to_string(),
            };
            map.insert((*key).to_string(), value);
            if LIST_KEYS.contains(key) {
                for i in 1..=MAX_LIST_VARIANTS {
                    let variant = format!("{key}_{i}");
                    if let Some(v) = env(&variant) {
                        set_keys.insert(variant.clone());
                        map.insert(variant, v);
                    }
                }
            }
        }
        Self {
            inner: Arc::new(RwLock::new(map)),
            set_keys: Arc::new(RwLock::new(set_keys)),
        }
    }

    /// Whether the key was explicitly provided by the environment (not a default).
    pub fn was_set(&self, key: &str) -> bool {
        self.set_keys.read().unwrap().contains(key)
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
        let read = self.inner.read().unwrap();
        let mut out = Vec::new();
        let mut push = |v: &str| {
            for s in v.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
                if !out.contains(&s.to_string()) {
                    out.push(s.to_string());
                }
            }
        };
        if let Some(base) = read.get(key) {
            push(base);
        }
        for i in 1..=MAX_LIST_VARIANTS {
            if let Some(v) = read.get(&format!("{key}_{i}")) {
                push(v);
            }
        }
        out
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
}
