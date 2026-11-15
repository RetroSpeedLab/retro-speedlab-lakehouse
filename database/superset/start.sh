#!/usr/bin/env bash
set -euo pipefail

superset db upgrade
superset init
python /app/assets.py
exec /usr/bin/run-server.sh
