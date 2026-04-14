#!/bin/bash
set -e

mkdir -p /app/logs

echo "[entrypoint] Applying database migrations..."
PYTHONPATH=/app/src python /app/src/scripts/init_db.py

echo "[entrypoint] Starting supervisor (worker + api)..."
exec /usr/bin/supervisord -n -c /etc/supervisor/supervisord.conf
