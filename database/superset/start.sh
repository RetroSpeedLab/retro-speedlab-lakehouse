#!/usr/bin/env bash
set -euo pipefail

umask 077

readonly SYNC_SECONDS=3600

sync_datasets() {
    while true; do
        python /app/sync_datasets.py || echo "The lake dataset sync failed, it runs again in ${SYNC_SECONDS} seconds" >&2
        sleep "${SYNC_SECONDS}"
    done
}

superset db upgrade
python /app/dataset_names.py
superset init
python /app/owner.py
python /app/assets.py
sync_datasets &
exec /usr/bin/run-server.sh
