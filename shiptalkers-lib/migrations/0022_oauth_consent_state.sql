ALTER TABLE slack_user_consents
    ADD COLUMN IF NOT EXISTS oauth_active BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS manual_revoked_at TIMESTAMPTZ;
