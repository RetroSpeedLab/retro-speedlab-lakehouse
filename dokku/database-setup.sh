#!/usr/bin/env bash
set -euo pipefail

readonly HOST=dokku-black-omen
readonly SERVER=black-omen
readonly SUPERSET=retro-speedlab-superset
readonly DATA=/mnt/fastdata/database
readonly BRONZE=/mnt/fastdata/lakehouse/data/bronze
readonly NETWORK=retro-speedlab-database
readonly KEY=database
readonly SUPERSET_PORT=18088

source "$(dirname "${BASH_SOURCE[0]}")/apps.sh"
source "$(dirname "${BASH_SOURCE[0]}")/garage.sh"

ssh "${SERVER}" "mkdir -p ${DATA}/superset && chmod 700 ${DATA}/superset"
ensure_network "${NETWORK}"
home_app "${SUPERSET}" "${NETWORK}" 1280m 2 "--publish 127.0.0.1:${SUPERSET_PORT}:8088"
ensure_mount "${SUPERSET}" "${DATA}/superset" /app/superset_home
ensure_mount "${SUPERSET}" "${BRONZE}" /lake/bronze:ro
set_once "${SUPERSET}" SUPERSET_SECRET_KEY "openssl rand -hex 32"
set_once "${SUPERSET}" SUPERSET_ADMIN_PASSWORD "openssl rand -hex 16"
garage key info "${KEY}" >/dev/null 2>&1 || garage key create "${KEY}" >/dev/null
garage bucket allow --read "${BUCKET}" --key "${KEY}"
store_key "${KEY}" "${HOST}" "${SUPERSET}"
echo "Then: bash dokku/database-deploy.sh; bash dokku/lan-setup.sh"
