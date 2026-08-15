#!/bin/sh
set -e

chown -R app:app /data

exec su-exec app "$@"
