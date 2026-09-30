ALTER TABLE slack_user_consents
    ADD COLUMN IF NOT EXISTS opt_out_locked BOOLEAN NOT NULL DEFAULT false;
