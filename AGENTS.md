# Agent Instructions

Use this file as the default guide for AI agents working in the repository.

## Human in the Loop

- Always keep a human in the loop. Present your work for review, and do not deploy, push, or run anything that changes shared state without explicit approval.
- Do not test with Docker. Building images, starting the dev server, and running containers is the user's job.

## Quick Rules

- Run `just lint` and `just fmt` before marking code as ready for review.
- Do not use emdashes in anything.
- Use proper markdown syntax.
- Follow existing code style and conventions.
- Stay minimalistic in your code and documentation.
- Do not divert from your active task unless explicitly instructed to do so.
- If you change something that affects anything in the "Overview" section of this file, update it accordingly.

## Commands

- `just check` - cargo check for all targets and features
- `just lint` - clippy with `-D warnings`
- `just fmt` - rustfmt
- `just test` - cargo test
- `just formula-test` - sessionizer tests with `--nocapture` so the demo tests print the computed numbers

## Overview

Scrapes every public channel and thread reply (plus their reactions) from Hack Club Slack into ClickHouse and serves a stats website with live leaderboards. Emoji names stay in the raw message text and in `slack_reactions` for later statistics; there is no emoji catalog yet.

Coding time for every non-bot, non-deleted user is pulled from hackatime on a 30-minute cycle: the public spans endpoint (`/api/v1/users/{uid}/heartbeats/spans`) is called for an incremental window and the returned spans are stored with exact timestamps in `hackatime_spans`, with `hackatime_connections.total_minutes` derived from the span sum. A first sync backfills every day since `2024-01-01`. Most users sync through the public endpoint (keyed by Slack UID, no OAuth); a user whose hackatime profile is not public gets a `private` flag and only syncs once they link via OAuth. Missing profiles are recorded as `no_account` and retried every 30 days. A user with a saved OAuth token always syncs through it.

## Repository Layout

- `Cargo.toml` (root) - Cargo workspace with a single member, `shiptalkers-app`; run `cargo`/`just` from the repo root
- `shiptalkers-app/` - the Rust app crate: `src/`, `templates/`, `tests/`, `Cargo.toml`, `Dockerfile`, `docker-entrypoint.sh`, `.env.example`; built from the workspace root (single `Cargo.lock`, context is the repo root) so the app's `Dockerfile` is used as `shiptalkers-app/Dockerfile` from `docker-compose.yml` and the CI build workflow
- `scripts/slack_app_creation/` - standalone Rust CLI (not a workspace member) for creating the Slack app and installing tokens
- `.github/` - `workflows/` (CI, build/deploy) and `actions/setup-rust`
- `docker-compose.yml` - local dev setup (app + ClickHouse)

## Architecture

- `src/main.rs` - entry point, settings + env parsing, spawns scraper/user sync/socket mode/web server tasks. When no Slack tokens are set, serves existing ClickHouse data read-only.
- `src/scraper.rs` - all scrape logic: `run_scraper` (30m cycle), `scrape_all_messages`, `scrape_channel_list` (shared work queue across user tokens), `scrape_one_channel` (incremental + full-scrape modes, thread re-scan), `scrape_thread`, `process_channel_page` / `process_thread_page` (per-page inserts of messages, reactions, word counts, bot users). Touched users/channels are batched for score recomputation at the end of each pass. `sync_users` runs `users.list` every 2h.
- `src/slack/mod.rs` - `SlackClient` (per-method FIFO token-bucket rate limiter, 429 backoff, page-by-page streaming pagination so whole channels/threads never sit in memory), `SlackClientPool` (round-robins pages across bot tokens).
- `src/slack/socket.rs` - Slack Socket Mode via tokio-tungstenite; one connection per `SLACK_APP_TOKENS` app, events sharded by FNV hash of `ts`. Stats bot replies to messages in `SLACK_MAIN_CHANNEL` with a PNG card. Reconnects forever with exponential backoff (1s-60s); stale connections (no frames for 60s) and connect timeouts (30s) are handled.
- `src/slack/time_range.rs` - `TimeRange` enum (`AllTime` / `Since` / `Between`) and `parse_time_range_at` which matches keywords like `today`, `yesterday`, `last week`, `7 days`, `3 months`, `all time`, etc.
- `src/bot_image.rs` - renders the stats card SVG to PNG via resvg/usvg.
- `src/sessionize.rs` - Slack Time sessionizer constants (`SESSION_GAP_BOUNDARY_SECS`, `MESSAGE_TYPING_CHARS_PER_SEC`, `MESSAGE_READ_OVERHEAD_SECS`, `SESSION_MAX_SECS`) and the Rust reference `sessionize`. Edit here to change the algorithm.
- `src/settings.rs` - env var parsing with defaults, exposed as `RuntimeSettings`.
- `src/auth/mod.rs` - hackatime spans fetching (`fetch_coding_spans`), span-overlap math for range-scoped queries (`span_overlap_seconds`).

### `src/db/`

- `clickhouse_db.rs` - schema DDL, `init_tables` (startup migrations, `ReplacingMergeTree`), message/channel/reaction/word-count inserts, checkpoint queries (`get_max_message_ts`, `mark_channel_scraped`, `mark_fully_scraped`, thread equivalents), `optimize_slack_messages` (24h). Row structs: `SlackMessageRow`, `SlackChannelRow`, `SlackReactionRow`, `WordCountRow`.
- `hackatime.rs` - `hackatime_connections` and `hackatime_spans` CRUD: upsert, get, delete, `insert_hackatime_spans`, `get_hackatime_total_seconds`, `get_coding_user_ids`.
- `scores.rs` - `sessionizer_changed` (checks `score_meta` fingerprint), `backfill_stale_user_scores` / `backfill_stale_channel_scores` (incremental or full on sessionizer change), `recompute_user_scores` / `recompute_channel_scores` (batches of 50, sessionizer SQL in ClickHouse).
- `refresh.rs` - background tasks: `refresh_word_totals` (incremental fold with daily full rebuild, watermark tracked in `word_refresh_meta`), `refresh_daily_stats` (sessionizer pass over every message, replaces `daily_stats`).
- `sqlite.rs` - SQLite auth DB (`linked_users`).

### `src/website/`

- `mod.rs` - axum router, server-rendered `/stats`, `/stats/:id` (user or channel, `U`/`C` prefix), `/leaderboard`, `/search` via askama. `/pfp/:id` redirects to stored Slack pfp URL.
- `auth.rs` - hackatime OAuth login/callback/disconnect, `sync_coding_activity` (fetches spans, stores in `hackatime_spans`, rewrites `total_minutes`), `resync_all` (30m loop over every non-bot user).

## Slack Time Formula

Slack Time is the sessionizer output (`user_scores.total_time`, ranked by `score`). To change the algorithm edit the constants in `src/sessionize.rs`, which are shared across all ClickHouse sessionizer queries and the Rust reference. A change to any constant flips `score_meta`'s stored fingerprint, so the next restart full-recomputes all user and channel scores. Tests: `tests/sessionizer.rs`.

## Conventions

- ClickHouse is the only analytics datastore. SQLite holds auth/linked-user state only.
- `slack_messages.message_ts` is `UInt64` microseconds; `thread_ts` stays `String` for Slack pagination compatibility.
- Logging is `tracing` only. Per-channel work logs at debug; inserts at info; progress at info only when new messages were inserted.
- The website has exactly one JS file (`time.js`) for UTC-to-local timezone conversion. Everything else is server-rendered askama with `<meta http-equiv="refresh">`.
- ClickHouse row structs use `#[derive(clickhouse::Row, serde::Deserialize)]`, plus `Serialize` when inserting.
- Tests live in `shiptalkers-app/tests/` (one file per area) and only reach `pub` items. The crate is lib + bin (`lib.rs` declares modules, `main.rs` imports them).
- Queries that must survive transient DB issues fall back with `unwrap_or` / `unwrap_or_default`, never panic.
- Errors use `Box<dyn std::error::Error>` (plus `Send + Sync` across await points) or `String` in scraper tasks.

## Environment Variables

All settings below are read from environment variables at startup (with the defaults noted); edit `shiptalkers-app/.env` and restart to change them.

- `SLACK_BOT_TOKENS` - required, comma-separated bot tokens (one per Slack app), or numbered variants `SLACK_BOT_TOKENS_1`, `SLACK_BOT_TOKENS_2`, ...; `conversations.list` / `users.list` pages round-robin across them, stats bot replies always use the first entry.
- `SLACK_USER_TOKENS` - comma-separated user tokens or numbered variants, one SlackClient per token pulling from the shared channel work queue.
- `SLACK_APP_TOKENS` - optional, comma-separated app tokens or numbered variants; each opens its own Socket Mode connection and events are sharded across them.
- `SLACK_MAIN_CHANNEL` - channel ID the stats bot watches; users posting a time range there get a threaded reply. Optional, disables the bot when unset.
- `SQLITE_DB_PATH` - SQLite auth DB path, default `data/auth.db`.
- `SLACK_REQUEST_DELAY_MS` - request pacing per method per token, default 1200 (tier 3, 50 req/min).
- `SLACK_MAX_INFLIGHT` - burst per method per token, default 8.
- `SLACK_CHANNEL_CONCURRENCY` - channels scraped concurrently per token, default 8.
- `SLACK_THREAD_RESCAN_HOURS` - thread rescan history window, default 720 (30 days).
- `SLACK_THREAD_RESCAN_INTERVAL_HOURS` - how often fully-scraped channels are re-scanned for threads, default 6.
- `CLICKHOUSE_URL` - ClickHouse HTTP endpoint, default `http://clickhouse:8123`. Coolify internal URLs (`clickhouse://user:pass@host:9000/db`) are auto-converted to `http://host:8123`, and credentials/database from the URL are used unless `CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD`/`CLICKHOUSE_DB` are explicitly set.
- `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DB` - ClickHouse credentials and database, default `ship_talkers`.
- `HOST`, `PORT` - web server bind, default 0.0.0.0:3000.

## Gotchas

- Slack rate limits are per (token, method). `conversations.history` and `conversations.replies` have separate budgets.
- Every token gets its own rate-limiter budget: bot tokens pages rotate one token per page, user tokens workers each get their own SlackClient (pulling from the shared queue), and stats bot replies use the first bot token.
- Socket Mode opens one connection per app. Events are sharded (FNV hash of `ts`) so only one bot replies. Duplicate `channel_created` events are harmless because `insert_new_channels` is idempotent.
- Scrape passes split into full-scrape (new channels) and incremental check (already-scraped channels) using `scraped_channels`.
- Reactions are whatever the fetch returned at that moment, so only re-fetched messages get their reactions refreshed. Slack truncates the `users` list of very popular reactions, so per-user reaction stats may undercount.
- Coding time is stored per span in `hackatime_spans` with one total per user in `hackatime_connections.total_minutes`. The stats bot card sums each span's exact overlap with the requested range, falling back to the all-time total before a user's first sync. Coding syncs are serialized per user. A 401/403 only deletes the connection if the `me` endpoint confirms the token is dead; hackatime outages never strip links. A 403 on the unauthenticated path means `private` state; a 404 means `no_account` (both written with empty `access_token` so the resync loop skips them).
- `slack_messages_by_user` is created with `max_suspicious_broken_parts = 1000` because it is fully derived from `slack_messages` and a power loss can break it. `init_tables` probes this table first and drops it (and its materialized view) whenever it cannot load; the startup backfill then rebuilds it.

## Finally

Thanks for your help! (To you the AI agent reading this or a human looking at this file) <3
