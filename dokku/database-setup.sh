#!/usr/bin/env bash
set -euo pipefail

readonly HOST=dokku-black-omen
readonly SERVER=black-omen
readonly THRIFT=retro-speedlab-thrift
readonly SUPERSET=retro-speedlab-superset
readonly DATA=/mnt/fastdata/database
readonly BRONZE=/mnt/fastdata/lakehouse/data/bronze
readonly NETWORK=retro-speedlab-database
readonly KEY=database
readonly SUPERSET_PORT=18088

source "$(dirname "${BASH_SOURCE[0]}")/apps.sh"
source "$(dirname "${BASH_SOURCE[0]}")/garage.sh"
source "$(dirname "${BASH_SOURCE[0]}")/settings.sh"

dokku() {
    ssh "${HOST}" "$@"
}

ssh "${SERVER}" "mkdir -p ${DATA}/thrift ${DATA}/superset && chmod 700 ${DATA}/superset \
    && sudo chown -R ${LAKEHOUSE_USER}:${LAKEHOUSE_USER} ${DATA}/thrift"
dokku network:create "${NETWORK}"

dokku apps:create "${THRIFT}"
dokku storage:mount "${THRIFT}" "${DATA}/thrift:/database"
dokku storage:mount "${THRIFT}" "${BRONZE}:/lake/bronze:ro"
dokku ps:set "${THRIFT}" procfile-path database/Procfile
dokku app-json:set "${THRIFT}" appjson-path database/app.json
dokku ps:set "${THRIFT}" restart-policy unless-stopped
dokku docker-options:add "${THRIFT}" build "--network host"
dokku docker-options:add "${THRIFT}" deploy "--cpu-shares 2048 --init --network-alias ${THRIFT}"
dokku resource:limit --process-type thrift --memory 1280m --cpu 1 "${THRIFT}"
dokku network:set "${THRIFT}" initial-network "${NETWORK}"
dokku proxy:disable "${THRIFT}"
dokku checks:disable "${THRIFT}"
garage key create "${KEY}" >/dev/null
garage bucket allow --read "${BUCKET}" --key "${KEY}"
store_key "${KEY}" "${HOST}" "${THRIFT}"

home_app "${SUPERSET}" "${NETWORK}" 768m 1 "--publish 127.0.0.1:${SUPERSET_PORT}:8088"
ensure_mount "${SUPERSET}" "${DATA}/superset" /app/superset_home
set_once "${SUPERSET}" SUPERSET_SECRET_KEY "openssl rand -hex 32"
set_once "${SUPERSET}" SUPERSET_ADMIN_PASSWORD "openssl rand -hex 16"
echo "Then: bash dokku/database-deploy.sh; bash dokku/lan-setup.sh"
