use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct SlackMessage {
    pub user: String,
    pub text: String,
    pub ts: String,
    pub channel: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SlackChannel {
    pub id: String,
    pub name: String,
}

pub struct SlackClient {
    client: Client,
    token: String,
}

impl SlackClient {
    pub fn new(token: String) -> Self {
        Self {
            client: Client::new(),
            token,
        }
    }

    pub async fn get_channels(&self) -> Result<Vec<SlackChannel>, Box<dyn std::error::Error>> {
        todo!()
    }

    pub async fn get_channel_history(&self, channel_id: &str) -> Result<Vec<SlackMessage>, Box<dyn std::error::Error>> {
        todo!()
    }
}
