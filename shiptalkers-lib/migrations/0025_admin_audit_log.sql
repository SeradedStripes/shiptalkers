CREATE TABLE IF NOT EXISTS admin_audit_log (
    id BIGSERIAL PRIMARY KEY,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    action TEXT NOT NULL,
    actor_slack_id TEXT NOT NULL,
    data_deleted BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS admin_audit_log_created_idx
    ON admin_audit_log (created_at DESC, id DESC);
