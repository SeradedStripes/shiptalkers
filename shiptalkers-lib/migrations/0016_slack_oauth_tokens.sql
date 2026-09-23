CREATE TABLE IF NOT EXISTS slack_oauth_tokens (
    slack_id TEXT PRIMARY KEY,
    team_id TEXT NOT NULL DEFAULT '',
    access_token TEXT NOT NULL,
    scopes TEXT NOT NULL DEFAULT '',
    connected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    disabled_at TIMESTAMPTZ
);
