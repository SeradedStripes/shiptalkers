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

## Overview

Scrapes every public channel and thread reply from Hack Club Slack into ClickHouse and serves a stats website with live leaderboards.

## Architecture

- `src/main.rs` - entry point, settings + env parsing, scraper orchestration; message scrape cycles run every 30 minutes, pacing a full pass over every channel (round-robin across user tokens) to the 30m boundary before repeating. Per-channel tasks are wrapped in a 25m timeout and `conversations.list` page inserts in a 2m timeout, so a stalled ClickHouse call can never wedge a pass or cycle. After a channel's messages and thread replies are inserted, the touched users' Slack Time scores are recomputed into `user_scores`; a startup backfill recomputes stale users (a full recompute only when the formula changed). Fully-scraped channels are also re-scanned for threads: every `SLACK_THREAD_RESCAN_INTERVAL_HOURS` (default 6h) a recent `SLACK_THREAD_RESCAN_HOURS` (default 30-day) window of history is fetched (throttled per channel in memory) so messages that gained a thread after their first scrape get their replies fetched too. `sync_users` runs `users.list` every 2h (retry after 5m on failure), upserts only users whose `updated` changed, who lack a pfp URL, or who just became deleted, so avatar URLs stay current. `users` keeps every account (including bots and deleted ones) with `is_bot`/`is_deleted` flags; leaderboards, rankings, and stats counts exclude `is_bot = 1 OR is_deleted = 1`, but such users stay searchable and deleted accounts render as "Deleted account".
- `src/slack/mod.rs` - SlackClient, per-method FIFO token-bucket rate limiter, 429 backoff; SlackClientPool round-robins `conversations.list` / `users.list` pages across bot tokens (one SlackClient per token)
- `src/slack/socket.rs` - Slack Socket Mode (app events) via tokio-tungstenite; one connection per `SLACK_APP_TOKENS` app, message events sharded across apps so only one replies; each connection reconnects forever with exponential backoff (fresh `apps.connections.open` URL per attempt, 1s to 60s) since Slack recycles connections; a connection with no frames for 60s is treated as stale and reconnected, and the connect phase itself times out after 30s; stats bot replies to top-level messages in `SLACK_MAIN_CHANNEL` in a thread, via `chat.postMessage`, with a PNG card uploaded via `files.getUploadURLExternal` + `files.completeUploadExternal`. Replies always use the first `SLACK_BOT_TOKENS` entry (the main bot)
- `src/bot_image.rs` - renders the stats card SVG (`templates/slack_image.html` + `src/website/static/slack_image_stats.css`) to PNG via resvg/usvg, with bundled DejaVu fonts
- `src/db/clickhouse_db.rs` - ClickHouse schema, inserts, checkpoint queries, startup migrations (ReplacingMergeTree engine, `message_ts` String to UInt64 micros, `coding_activity.date` String to Date), periodic `OPTIMIZE TABLE slack_messages` (24h, no startup dedup); `user_scores` (per-user Slack Time score and metrics: session time, longest session, sessions, days, active hour, message/channel counts, first/last message ts; `ReplacingMergeTree(updated)`) refreshed by `recompute_user_scores` whenever a user's messages change (batches of 50 users) and read by the per-user stats pages
- `src/db/sqlite.rs` - SQLite auth DB (`linked_users`), the only non-ClickHouse datastore
- `src/settings.rs` - settings read from environment variables at startup (with defaults), exposed as `RuntimeSettings`; `SETTING_KEYS` and `default_value` drive the env parsing
- `src/website/mod.rs` - axum router, server-rendered `/stats`, `/stats/:id` (user or channel, dispatched by `U`/`C` prefix), `/leaderboard` and `/search` via askama; `/pfp/:id` looks up the stored Slack pfp URL and redirects to it
- `templates/stats.html` - askama template for the stats page

- `templates/user.html` - askama template for the per-user stats page
- `templates/channel.html` - askama template for the per-channel stats page
- `templates/leaderboard.html` - askama template for the leaderboard page
- `templates/search.html` - askama template for user and channel search results
- `templates/search_form.html` - shared inline search form partial
- `templates/slack_image.html` - askama SVG template for the stats bot card (CSS inlined from `slack_image_stats.css`)
- `src/website/static/` - style.css, time.js, slack_image_stats.css
- `src/formula.rs` - Slack Time formula evaluator and the `SLACK_TIME_CALCULATION_FORMULA` code constant (edit here to change the algorithm)
- `scripts/slack_app_creation/` - standalone Rust CLI that creates a Slack app from a manifest (default `manifest.yml` in the same directory as `.env.example`, or one passed as an argument) via an app configuration token and runs a one-shot OAuth install to print the bot and user tokens; configured through env vars (`SLACK_CONFIG_TOKEN`, `SLACK_CONFIG_REFRESH_TOKEN`, `SLACK_INSTALL_PORT`), see `.env.example`

## Slack Time Formula

`SLACK_TIME_CALCULATION_FORMULA` in `src/formula.rs` drives Top Talkers ranking (computed per user into `user_scores`, ranked by `score`), the per-user Slack Time report, and the Slack stats bot card (`src/slack/socket.rs` evaluates the same formula over the same per-user metrics). Variables: `SESSION_SECONDS` (sessionizer output, 5 min windows split after 30 min inactivity, capped at 4 h), `MESSAGE_COUNT`, `SESSION_COUNT`, `TOTAL_CHARS`, `AVG_MESSAGE_LENGTH`. Functions: `log10`, `ln`, `sqrt`, `exp`, `abs`, `pow`. Supports `+ - * / ()` and implicit multiplication like `2MESSAGE_COUNT`. Invalid formulas fail at startup. Comments above the constant document each variable's source.

## Conventions

- ClickHouse is the only analytics datastore. The stats page reads `slack_messages`, `slack_channels`, `coding_activity`, and `user_scores`; per-user pages read `user_scores` (plus `slack_messages_by_user` for the top-channels list). SQLite (`src/db/sqlite.rs`) holds auth/linked-user state.
- Insert data before marking any checkpoint complete. Main channel messages are inserted before thread replies.
- Progress tracking uses `max(message_ts)` per channel and `max(thread reply ts)` per thread. `slack_messages.message_ts` is `UInt64` microseconds (Slack sends "seconds.microseconds" strings, converted via `slack_ts_to_micros`/`micros_to_slack_ts` in `src/db/clickhouse_db.rs`); `thread_ts` stays `String` because Slack pagination uses it verbatim. `coding_activity.date` is `Date` (`time::Date` in Rust via `clickhouse::serde::time::date`).
- Logging is `tracing` only. Per-channel, per-thread, and per-fetch work logs at debug; inserts and page progress log at info; `Progress:` lines log at info but only when a run actually inserts new messages, never on an idle tick.
- Multi-token scraping round-robins channel shards across tokens and prefixes log lines with `[token k]`.
- The website has exactly one public JavaScript file (`src/website/static/time.js`, loaded via `header.html`), which converts UTC `<time>` elements to the visitor's local timezone. Everything else renders server side with askama and auto refreshes via `<meta http-equiv="refresh">`. Number formatting lives in Rust (`fmt_thousands`).
- ClickHouse row structs use `#[derive(clickhouse::Row, serde::Deserialize)]`, plus `Serialize` when inserting.
- Queries that must survive transient DB issues fall back with `unwrap_or` / `unwrap_or_default`, never panic.
- Errors use `Box<dyn std::error::Error>` (plus `Send + Sync` across await points) or `String` in scraper tasks.

## Environment Variables

All settings below are read from environment variables at startup (with the defaults noted); edit `.env` and restart to change them.

- `SLACK_BOT_TOKENS` - required, comma-separated bot tokens (one per Slack app); `conversations.list` / `users.list` pages round-robin across them, stats bot replies always use the first entry (the main bot).
- `SLACK_USER_TOKENS` - comma-separated user tokens, sharded round-robin per channel.
- `SLACK_APP_TOKENS` - optional, comma-separated app tokens; each opens its own Socket Mode connection and message events are sharded across them so only one bot replies.
- `SLACK_MAIN_CHANNEL` - channel ID the stats bot watches; users posting a time range there get a threaded reply. Optional, disables the bot when unset.
- `SQLITE_DB_PATH` - SQLite auth DB path (linked users), default `data/auth.db`.
- `SLACK_REQUEST_DELAY_MS` - request pacing per method per token, default 1200 (tier 3, 50 req/min)
- `SLACK_MAX_INFLIGHT` - burst per method per token, default 8
- `SLACK_CHANNEL_CONCURRENCY` - channels scraped concurrently per token, default 8
- `SLACK_THREAD_RESCAN_HOURS` - thread rescan history window, default 720 (30 days)
- `SLACK_THREAD_RESCAN_INTERVAL_HOURS` - how often fully-scraped channels are re-scanned for threads, default 6
- `CLICKHOUSE_URL`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DB`
- `HOST`, `PORT` - web server bind, default 0.0.0.0:3000.

## Gotchas

- Slack rate limits are per (token, method). `conversations.history` and `conversations.replies` have separate budgets, so the rate limiter stays per method.
- Every token gets its own rate-limiter budget: `SLACK_BOT_TOKENS` pages rotate one token per page, `SLACK_USER_TOKENS` channel shards each get their own SlackClient, and stats bot replies use the first bot token (the main bot).
- Socket Mode opens one connection per `SLACK_APP_TOKENS` app. Slack delivers every event to every app, so message events are sharded (FNV hash of `ts`) and only the owning socket replies. Duplicate `channel_created` events are harmless because `insert_new_channels` is idempotent.
- The rate limiter is a FIFO ticket queue that paces at exactly 1 request per delay, so one huge channel cannot stall the pass.
- Scrape passes split into full-scrape (new channels) and incremental check (already-scraped channels) using `scraped_channels`.
- `coding_activity` is `ReplacingMergeTree` on new deployments but reads must not rely on `FINAL` (it errors on tables still created as plain `MergeTree`). Reads dedup with `max(minutes)` per `(user_id, date)` in SQL. Coding syncs are serialized per user (`CODING_SYNC_LOCKS` in `auth.rs`) and the clear-then-insert uses `SETTINGS mutations_sync = 2`, because concurrent syncs used to insert duplicate day rows that inflated coding time sums. Fetches run before any DB write, so a failed sync leaves the old rows intact; a 401/403 from the hours endpoint means the stored hackatime token died, so the connection is deleted and the user re-links from `/link`.

## Finally

Thanks for your help! (To you the AI agent reading this or a human looking at this file) <3