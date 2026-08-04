use rusqlite::Connection;
use std::path::Path;
use tokio::sync::Mutex;

pub struct AuthDb {
    conn: Mutex<Connection>,
}

impl AuthDb {
    pub fn open(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        if let Some(parent) = Path::new(path).parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             CREATE TABLE IF NOT EXISTS linked_users (
                slack_id TEXT PRIMARY KEY,
                display_name TEXT NOT NULL,
                linked_at TEXT NOT NULL DEFAULT (datetime('now'))
             );
             CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
             );",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub async fn mark_linked(&self, slack_id: &str, display_name: &str) -> Result<(), String> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO linked_users (slack_id, display_name) VALUES (?1, ?2)
             ON CONFLICT(slack_id) DO UPDATE SET display_name = excluded.display_name",
            rusqlite::params![slack_id, display_name],
        )
        .map(|_| ())
        .map_err(|e| e.to_string())
    }

    pub async fn is_linked(&self, slack_id: &str) -> bool {
        let conn = self.conn.lock().await;
        conn.query_row(
            "SELECT 1 FROM linked_users WHERE slack_id = ?1",
            rusqlite::params![slack_id],
            |_| Ok(()),
        )
        .is_ok()
    }

    pub async fn get_setting(&self, key: &str) -> Option<String> {
        let conn = self.conn.lock().await;
        conn.query_row(
            "SELECT value FROM settings WHERE key = ?1",
            rusqlite::params![key],
            |row| row.get(0),
        )
        .ok()
    }

    pub async fn set_setting(&self, key: &str, value: &str) -> Result<(), String> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            rusqlite::params![key, value],
        )
        .map(|_| ())
        .map_err(|e| e.to_string())
    }
}
