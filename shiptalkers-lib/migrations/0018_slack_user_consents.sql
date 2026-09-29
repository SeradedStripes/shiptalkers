CREATE TABLE IF NOT EXISTS slack_user_consents (
    slack_user_id TEXT PRIMARY KEY,
    ship_talkers_id TEXT UNIQUE NOT NULL,
    consented_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ,
    consent_source TEXT NOT NULL DEFAULT '',
    backfill_started_at TIMESTAMPTZ,
    backfill_completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS slack_user_consents_active_idx
    ON slack_user_consents (ship_talkers_id)
    WHERE revoked_at IS NULL;
