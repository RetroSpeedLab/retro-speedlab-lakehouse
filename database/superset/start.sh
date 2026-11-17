#!/usr/bin/env bash
set -euo pipefail

umask 077

readonly RESTART_SECONDS=60
readonly SYNC_SECONDS=3600

every() {
    local seconds="$1" script="$2"
    while true; do
        python "/app/${script}" || echo "${script} failed, it runs again in ${seconds} seconds" >&2
        sleep "${seconds}"
    done
}

superset db upgrade
python /app/dataset_names.py
superset init
python /app/owner.py
python /app/assets.py
every "${RESTART_SECONDS}" lake_snapshot.py &
every "${SYNC_SECONDS}" sync_datasets.py &
exec /usr/bin/run-server.sh
