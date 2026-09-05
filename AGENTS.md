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
- `just formula-test` - sessionizer tests with `--nocapture` so the demo tests print the computed numbers (lives in `shiptalkers-lib`)

## Overview

Scrapes every public channel and thread reply (plus their reactions) from Hack Club Slack into PostgreSQL and serves a stats website with live leaderboards. Emoji names stay in the raw message text and in `slack_reactions` for later statistics; there is no emoji catalog yet.

Scraping runs in its own binary/container (`shiptalkers-scraper`, depends only on Postgres) so it keeps running while the app container restarts; the app serves the website, refresh loops, and the Socket Mode stats bot.

Coding time for every non-bot, non-deleted user is pulled from hackatime on a 30-minute cycle (run by the `shiptalkers-scraper` container so it survives app restarts): the public spans endpoint (`/api/v1/users/{uid}/heartbeats/spans`) is called for an incremental window and the returned spans are stored with exact timestamps in `hackatime_spans`, with `hackatime_connections.total_minutes` derived from the span sum. A first sync backfills every day since `2024-01-01`. Most users sync through the public endpoint (keyed by Slack UID, no OAuth); a user whose hackatime profile is not public gets a `private` flag and only syncs once they link via OAuth. Missing profiles are recorded as `no_account` and retried every 30 days. A user with a saved OAuth token always syncs through it.

## Repository Layout

- `Cargo.toml` (root) - Cargo workspace with three members, `shiptalkers-app`, `shiptalkers-lib`, and `shiptalkers-scraper`; run `cargo`/`just` from the repo root
- `shiptalkers-lib/` - the Rust shared library crate (`Cargo.toml`, `src/lib.rs`, `src/sessionize.rs`, `src/hackatime.rs`, `src/db.rs`, `tests/sessionizer.rs`) holding code used by both app and scraper: the sessionizer, the shared db primitives (`connect`, `init_tables`, `placeholders`, `INSERT_CHUNK`, `SlackChannelRow`, `insert_new_channels_rows`), and the hackatime access layer (`fetch_coding_spans`, `fetch_hackatime_me`, `span_overlap_seconds`, date helpers, `hackatime_connections`/`hackatime_spans` CRUD). It is the only crate that depends on sqlx directly (0.9, features `runtime-tokio`, `tls-rustls-ring`, `postgres`, `time`) and re-exports it (`pub use sqlx;`); both app and scraper re-export it again as `crate::sqlx` so they only use sqlx through the lib, and dynamic SQL strings are wrapped in `sqlx::AssertSqlSafe`.
- `shiptalkers-app/` - the Rust app crate: `src/`, `templates/`, `tests/`, `Cargo.toml`, `Dockerfile`, `docker-entrypoint.sh`, `.env.example`; built from the workspace root (single `Cargo.lock`, context is the repo root) so the app's `Dockerfile` is used as `shiptalkers-app/Dockerfile` from `docker-compose.yml` and the CI build workflow
- `shiptalkers-scraper/` - the Rust scraper crate (`Cargo.toml`, `src/`, `Dockerfile`, `.env.example`), its own Docker image so scraping keeps running while the app container restarts; only depends on Postgres (not on the app)
- `scripts/slack_app_creation/` - standalone Rust CLI (not a workspace member) for creating the Slack app and installing tokens
- `.github/` - `workflows/` (CI, build/deploy) and `actions/setup-rust`
- `docker-compose.yml` - local dev setup (app + scraper + Postgres)

## Architecture

- `src/main.rs` (app) - entry point, settings + env parsing, connects to `DATABASE_URL`, spawns the refresh loops, the Socket Mode task, and the web server. When no Slack tokens are set, serves existing data read-only. When `DATABASE_URL` is unset it still boots without a database for dev; DB-backed routes return 503 and static pages work.
- `shiptalkers-scraper/src/main.rs` (scraper) - separate binary that connects to the same `DATABASE_URL`, runs `init_tables`, spawns `run_scraper`, the `users.list` user-sync loop, and the hackatime `resync_all` loop (every 30m, regardless of Slack tokens), then idles until a shutdown signal.
- `shiptalkers-scraper/src/hackatime.rs` - the coding-time sync engine: `sync_coding_activity` (one user, incremental window or full backfill, token-death check via the `me` endpoint), `resync_all` (30m pass over every non-bot user, records `private`/`no_account` states), `SyncFailure`, `record_hackatime_status`, per-user `coding_sync_lock`.
- `shiptalkers-scraper/src/scraper.rs` - all scrape logic: `run_scraper` (30m cycle), `scrape_all_messages`, `scrape_channel_list` (shared work queue across user tokens), `scrape_one_channel` (incremental + full-scrape modes, thread re-scan), `scrape_thread`, `process_channel_page` / `process_thread_page` (per-page inserts of messages, reactions, word counts, bot users). Touched users/channels are batched for score recomputation at the end of each pass. `sync_users` runs `users.list` every 2h. The incremental already-scraped-channel sweep (`scrape_channel_list` + `ScrapeSweep`) resumes where the last pass left off instead of restarting at the list start, persisting its position in `scrape_sweep` and clearing it when a full pass completes.
- `shiptalkers-scraper/src/slack/mod.rs` - `SlackClient` (per-method FIFO token-bucket rate limiter, 429 backoff, page-by-page streaming pagination so whole channels/threads never sit in memory), `SlackClientPool` (round-robins pages across bot tokens).
- `src/slack/mod.rs` (app) - only re-exports `socket` and `time_range`; the Slack client lives in the scraper crate.
- `src/slack/socket.rs` - Slack Socket Mode via tokio-tungstenite; one connection per `SLACK_APP_TOKENS` app, events sharded by FNV hash of `ts`. Stats bot replies to messages in `SLACK_MAIN_CHANNEL` with a PNG card. Reconnects forever with exponential backoff (1s-60s); stale connections (no frames for 60s) and connect timeouts (30s) are handled.
- `src/slack/time_range.rs` - `TimeRange` enum (`AllTime` / `Since` / `Between`) and `parse_time_range_at` which matches keywords like `today`, `yesterday`, `last week`, `7 days`, `3 months`, `all time`, etc.
- `src/bot_image.rs` - renders the stats card SVG to PNG via resvg/usvg.
- `src/lib.rs` (app) - re-exports `ship_talkers_lib::sessionize` and `ship_talkers_lib::sqlx` (as `crate::sqlx`) so the crate-wide sessionizer references and the socket/refresh queries stay in lockstep with the scraper. Slack Time sessionizer constants (`SESSION_GAP_BOUNDARY_SECS`, `MESSAGE_TYPING_CHARS_PER_SEC`, `MESSAGE_READ_OVERHEAD_SECS`, `SESSION_MAX_SECS`) and the Rust reference `sessionize` live in the shared `shiptalkers-lib` crate; edit `shiptalkers-lib/src/sessionize.rs` to change the algorithm.
- `src/settings.rs` (app) - env var parsing with defaults, exposed as `RuntimeSettings`, including the auth/website keys. `shiptalkers-scraper/src/settings.rs` is a trimmed copy for the scraper's own knobs.
- `src/auth/mod.rs` - OAuth login/token exchange for HCA and hackatime, signed session cookies (`Session`, `issue_session`, `parse_session`), authorize-URL helpers. The hackatime fetch helpers and date math live in `shiptalkers-lib/src/hackatime.rs` and are re-exported here (`fetch_hackatime_me`, `span_overlap_seconds`, `civil_from_days`, `date_plus_days`).

### `src/db/` (app)

- `postgres_db.rs` - `AuthDb` (linked-user upsert/lookup) plus the shared db primitives re-exported from `shiptalkers-lib` (`connect`, `init_tables`, `placeholders`, `INSERT_CHUNK`, `SlackChannelRow`, `insert_new_channels_rows`). The scraper crate keeps its own copy with the scrape inserts/checkpoints and timestamp helpers (`slack_ts_to_micros`, `micros_to_slack_ts`, `parse_date`). Only the scraper runs `init_tables`; the app connects and reads existing tables so it can run with a read-only DB role.
- `refresh.rs` - background tasks: `refresh_word_totals` (incremental fold with daily full rebuild, watermark tracked in `word_refresh_meta`), `refresh_daily_stats` (sessionizer pass over every message, replaces `daily_stats`).

### `shiptalkers-scraper/src/db/`

- `postgres_db.rs` - scraper shares the schema DDL, row structs, and insert helpers with the app via `shiptalkers-lib`, and adds its scrape-only pieces: message/channel/reaction/word-count inserts (multi-row `$n` placeholders, chunks of 500), checkpoint queries (`get_max_message_ts`, `get_scraped_channel_ids`, `mark_channels_scraped`, `mark_fully_scraped`, thread equivalents), startup backfills (`word_counts` server-side rebuild), timestamp helpers (`slack_ts_to_micros`, `micros_to_slack_ts`, `parse_date`). Scrape-only row structs: `SlackMessageRow`, `SlackReactionRow`, `SlackUserRow`, `WordCountRow`.
- `scores.rs` - `sessionizer_changed` (checks `score_meta` fingerprint), `backfill_stale_user_scores` / `backfill_stale_channel_scores` (incremental or full on sessionizer change), `recompute_user_scores` / `recompute_channel_scores` (batches of 50, sessionizer SQL in Postgres).

### `src/website/`

- `mod.rs` - axum router, server-rendered `/stats`, `/stats/:id` (user or channel, `U`/`C` prefix), `/leaderboard`, `/search` via askama. `/pfp/:id` redirects to stored Slack pfp URL.
- `auth.rs` - hackatime OAuth login/callback/disconnect. The callback validates the token via the shared `fetch_hackatime_me` and stores it; the scraper's `resync_all` loop picks it up on its next 30m pass.

## Slack Time Formula

Slack Time is the sessionizer output (`user_scores.total_time`, ranked by `score`). To change the algorithm edit the constants in `shiptalkers-lib/src/sessionize.rs`, which is shared across all sessionizer queries and the Rust reference (both crates re-export it). A change to any constant flips `score_meta`'s stored fingerprint, so the next restart full-recomputes all user and channel scores. Tests: `shiptalkers-lib/tests/sessionizer.rs`.

## Conventions

- PostgreSQL is the only datastore (sqlx, runtime queries with `$n` placeholders; no query macros).
- `slack_messages.message_ts` is `BIGINT` microseconds; Rust row structs keep it as `u64` and bind `as i64`. `thread_ts` stays `TEXT` for Slack pagination compatibility.
- Logging is `tracing` only. Per-channel work logs at debug; inserts at info; progress at info only when new messages were inserted.
- The website has exactly one JS file (`time.js`) for UTC-to-local timezone conversion. Everything else is server-rendered askama with `<meta http-equiv="refresh">`.
- Tests live in `shiptalkers-app/tests/`, `shiptalkers-scraper/tests/`, and `shiptalkers-lib/tests/` (one file per area) and only reach `pub` items. Both app and scraper are lib + bin (`lib.rs` declares modules, `main.rs` imports them).
- Queries that must survive transient DB issues fall back with `unwrap_or` / `unwrap_or_default`, never panic.
- Errors use `Box<dyn std::error::Error>` (plus `Send + Sync` across await points) or `String` in scraper tasks.

## Environment Variables

All settings below are read from environment variables at startup (with the defaults noted); edit `shiptalkers-app/.env` / `shiptalkers-scraper/.env` and restart to change them.

App (`shiptalkers-app/.env`):

- `SLACK_BOT_TOKENS` - required, comma-separated bot tokens (one per Slack app), or numbered variants `SLACK_BOT_TOKENS_1`, `SLACK_BOT_TOKENS_2`, ...; stats bot replies always use the first entry.
- `SLACK_APP_TOKENS` - optional, comma-separated app tokens or numbered variants; each opens its own Socket Mode connection and events are sharded across them.
- `SLACK_MAIN_CHANNEL` - channel ID the stats bot watches; users posting a time range there get a threaded reply. Optional, disables the bot when unset.
- `DATABASE_URL` - required Postgres connection string, e.g. `postgres://ship_talkers:ship_talkers@localhost:5432/ship_talkers`.
- `HOST`, `PORT` - web server bind, default 0.0.0.0:3000.
- Auth/website keys: `BASE_URL`, `SESSION_SECRET`, `HCA_CLIENT_ID`, `HCA_CLIENT_SECRET`, `HACKATIME_CLIENT_ID`, `HACKATIME_CLIENT_SECRET`.

Scraper (`shiptalkers-scraper/.env`):

- `SLACK_BOT_TOKENS` - optional, comma-separated bot tokens (one per Slack app), or numbered variants; `conversations.list` / `users.list` pages round-robin across them. Unset, the scraper still runs the hackatime resync loop.
- `SLACK_USER_TOKENS` - comma-separated user tokens or numbered variants, one SlackClient per token pulling from the shared channel work queue.
- `SLACK_REQUEST_DELAY_MS` - request pacing per method per token, default 1200 (tier 3, 50 req/min).
- `SLACK_MAX_INFLIGHT` - burst per method per token, default 8.
- `SLACK_CHANNEL_CONCURRENCY` - channels scraped concurrently per token, default 8.
- `SLACK_THREAD_RESCAN_HOURS` - thread rescan history window, default 720 (30 days).
- `SLACK_THREAD_RESCAN_INTERVAL_HOURS` - how often fully-scraped channels are re-scanned for threads, default 6.
- `DATABASE_URL` - required Postgres connection string, e.g. `postgres://ship_talkers:ship_talkers@localhost:5432/ship_talkers`.

## Gotchas

- Slack rate limits are per (token, method). `conversations.history` and `conversations.replies` have separate budgets.
- Every token gets its own rate-limiter budget (scraper crate): bot tokens pages rotate one token per page, user tokens workers each get their own SlackClient (pulling from the shared queue). Stats bot replies (app crate) use the first bot token.
- Socket Mode opens one connection per app. Events are sharded (FNV hash of `ts`) so only one bot replies. Duplicate `channel_created` events are harmless because `insert_new_channels` is idempotent.
- Scrape passes split into full-scrape (new channels) and incremental check (already-scraped channels) using `scraped_channels`.
- Reactions are whatever the fetch returned at that moment, so only re-fetched messages get their reactions refreshed. Slack truncates the `users` list of very popular reactions, so per-user reaction stats may undercount.
- Coding time is stored per span in `hackatime_spans` with one total per user in `hackatime_connections.total_minutes`. The stats bot card sums each span's exact overlap with the requested range, falling back to the all-time total before a user's first sync. Coding syncs are serialized per user. A 401/403 only deletes the connection if the `me` endpoint confirms the token is dead; hackatime outages never strip links. A 403 on the unauthenticated path means `private` state; a 404 means `no_account` (both written with empty `access_token` so the resync loop skips them).
- They are same-row views: the per-user Slack Time queries read `slack_messages` directly, served by the `slack_messages_user_ts_idx (user_id, message_ts)` index, so no denormalized per-user copy of every message is kept. `init_tables` runs a one-time cleanup that drops the old `slack_messages_by_user` table (and its trigger/function/index) plus any stale `toast_compress` flag left from the removed compaction task.

## Finally

Thanks for your help! (To you the AI agent reading this or a human looking at this file) <3
