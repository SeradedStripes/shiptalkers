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

## Commands

Run all of these from the repo root.

- `just check` - cargo check for all targets and features
- `just lint` - clippy with `-D warnings`
- `just fmt` - rustfmt
- `just test` - cargo test
- `just formula-test` - sessionizer tests with `--nocapture` so the demo tests print the computed numbers

## Repository Layout

- `Cargo.toml` (root) - Cargo workspace with three members: `shiptalkers-app`, `shiptalkers-lib`, `shiptalkers-scraper`.
- `shiptalkers-lib/` - shared library: sessionizer, db primitives, hackatime access layer. The only crate that depends on sqlx directly.
- `shiptalkers-app/` - website, refresh loops, and the Socket Mode stats bot. Askama templates in `templates/`.
- `shiptalkers-scraper/` - scrape binary plus the coding-time resync loop. Separate Docker image so scraping survives app restarts.
- `scripts/slack_app_creation/` - standalone Rust CLI (not a workspace member) for creating the Slack app and installing tokens.
- `.github/` - CI and build/deploy workflows.
- `docker-compose.yml` - local dev setup (app + scraper + Postgres).

If you need details on a crate's internals, read its `src/` instead of relying on stale docs.

## Conventions

- Comments are one line and short; no multiline or long comment blocks.
- PostgreSQL is the only datastore. sqlx runtime queries with `$n` placeholders; no query macros.
- Logging is `tracing` only.
- Tests live in `*/tests/` (one file per area) and only reach `pub` items.
- Queries that must survive transient DB issues fall back with `unwrap_or` / `unwrap_or_default`, never panic.
- Errors use `Box<dyn std::error::Error>` (plus `Send + Sync` across await points) or `String` in scraper tasks.

## Finally

Thanks for your help! (To you the AI agent reading this or a human looking at this file) <3