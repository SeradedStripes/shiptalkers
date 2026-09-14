CREATE INDEX IF NOT EXISTS slack_messages_channel_thread_ts_idx
    ON slack_messages (channel_id, thread_ts, message_ts)
    WHERE thread_ts IS NOT NULL;
