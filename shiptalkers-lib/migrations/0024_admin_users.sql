CREATE TABLE IF NOT EXISTS admin_users (
    slack_id TEXT PRIMARY KEY,
    added_by TEXT NOT NULL,
    added_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
