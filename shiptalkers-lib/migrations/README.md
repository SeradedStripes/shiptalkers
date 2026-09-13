# Database migrations

Migrations are embedded in the binaries with `sqlx::migrate!` and run before
the scraper or application starts using PostgreSQL.

`0001_initial.sql` is a baseline for the compact locator schema.
Existing databases created by the previous runtime bootstrap must be backed up before the first migration run.
The old runtime initializer remains available only for legacy conversion and is no longer called at startup.
