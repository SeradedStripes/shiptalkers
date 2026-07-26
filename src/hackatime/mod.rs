use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct CodingActivity {
    pub user_id: String,
    pub date: String,
    pub minutes: i64,
    pub language: Option<String>,
}

pub struct HackatimeClient {
    client: Client,
    base_url: String,
}

impl HackatimeClient {
    pub fn new(base_url: String) -> Self {
        Self {
            client: Client::new(),
            base_url,
        }
    }

    pub async fn get_user_activity(&self, user_id: &str, access_token: &str) -> Result<Vec<CodingActivity>, Box<dyn std::error::Error>> {
        todo!()
    }

    pub fn get_oauth_url(client_id: &str, redirect_uri: &str) -> String {
        format!(
            "{}/oauth/authorize?client_id={}&redirect_uri={}&response_type=code",
            "https://hackatime.hackclub.com", client_id, redirect_uri
        )
    }
}
