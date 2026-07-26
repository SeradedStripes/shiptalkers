use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct User {
    pub id: i64,
    pub slack_id: String,
    pub hackatime_id: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UserData {
    pub slack_id: String,
    pub slack_messages: i64,
    pub coding_minutes: i64,
}

pub fn create_user(conn: &Connection, slack_id: &str) -> Result<i64, Box<dyn std::error::Error>> {
    conn.execute(
        "INSERT INTO users (slack_id) VALUES (?1)",
        params![slack_id],
    )?;
    Ok(conn.last_insert_rowid())
}

pub fn get_user(conn: &Connection, slack_id: &str) -> Result<Option<User>, Box<dyn std::error::Error>> {
    let mut stmt = conn.prepare(
        "SELECT id, slack_id, hackatime_id, created_at FROM users WHERE slack_id = ?1"
    )?;

    let mut rows = stmt.query_map(params![slack_id], |row| {
        Ok(User {
            id: row.get(0)?,
            slack_id: row.get(1)?,
            hackatime_id: row.get(2)?,
            created_at: row.get(3)?,
        })
    })?;

    match rows.next() {
        Some(row) => Ok(Some(row?)),
        None => Ok(None),
    }
}

pub fn get_user_data(conn: &Connection, slack_id: &str) -> Result<UserData, Box<dyn std::error::Error>> {
    todo!()
}
