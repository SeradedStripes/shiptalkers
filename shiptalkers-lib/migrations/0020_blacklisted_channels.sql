CREATE TABLE IF NOT EXISTS blacklisted_channels (
    slack_channel_id TEXT PRIMARY KEY,
    ship_talkers_id TEXT UNIQUE NOT NULL,
    channel_name TEXT NOT NULL DEFAULT '',
    blacklisted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
