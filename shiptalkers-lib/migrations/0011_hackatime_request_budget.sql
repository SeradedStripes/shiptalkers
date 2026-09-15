CREATE TABLE IF NOT EXISTS hackatime_request_budget (
    request_date DATE PRIMARY KEY,
    request_count BIGINT NOT NULL DEFAULT 0
);
