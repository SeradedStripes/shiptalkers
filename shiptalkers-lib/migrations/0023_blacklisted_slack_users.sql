CREATE TABLE IF NOT EXISTS blacklisted_slack_users (
    slack_user_id TEXT PRIMARY KEY,
    blacklisted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
