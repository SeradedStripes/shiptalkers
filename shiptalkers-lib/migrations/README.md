# Database migrations

Migrations are embedded in the binaries with `sqlx::migrate!` and run before
the scraper or application starts using PostgreSQL.

`0001_initial.sql` is a baseline for the compact locator schema.
Existing databases created by the previous runtime bootstrap must be backed up before the first migration run.
The baseline drops incompatible legacy message and channel tables and requires
a full rescrape under the opt-in policy. Back up any existing database first.

`0002_purge_database.sql` intentionally removes all application data and
resets identity sequences.
