#!/usr/bin/env bash
set -euo pipefail

readonly HOST=dokku-black-omen
readonly SERVER=black-omen
readonly MAIN=dokku
readonly THRIFT=retro-speedlab-thrift
readonly SUPERSET=retro-speedlab-superset
readonly PROXY=retro-speedlab-database
readonly DOMAIN=database.retrospeedlab.com
readonly DATA=/mnt/fastdata/database
readonly BRONZE=/mnt/fastdata/lakehouse/data/bronze
readonly NETWORK=retro-speedlab-database
readonly KEY=database
readonly SUPERSET_PORT=18088
readonly QUERY_TIMEOUT=300s

source "$(dirname "${BASH_SOURCE[0]}")/garage.sh"

dokku() {
    ssh "${HOST}" "$@"
}

main() {
    ssh "${MAIN}" "$@"
}

ssh "${SERVER}" "mkdir -p ${DATA}/thrift ${DATA}/superset"
dokku network:create "${NETWORK}"

dokku apps:create "${THRIFT}"
dokku storage:mount "${THRIFT}" "${DATA}/thrift:/database"
dokku storage:mount "${THRIFT}" "${BRONZE}:/lake/bronze:ro"
dokku ps:set "${THRIFT}" procfile-path database/Procfile
dokku app-json:set "${THRIFT}" appjson-path database/app.json
dokku ps:set "${THRIFT}" restart-policy unless-stopped
dokku docker-options:add "${THRIFT}" build "--network host"
dokku docker-options:add "${THRIFT}" deploy "--cpu-shares 256 --init"
dokku resource:limit --process-type thrift --memory 1024m --cpu 1 "${THRIFT}"
dokku network:set "${THRIFT}" initial-network "${NETWORK}"
dokku proxy:disable "${THRIFT}"
dokku checks:disable "${THRIFT}"
garage key create "${KEY}" >/dev/null
garage bucket allow --read "${BUCKET}" --key "${KEY}"
store_key "${KEY}" "${HOST}" "${THRIFT}"

dokku apps:create "${SUPERSET}"
dokku storage:mount "${SUPERSET}" "${DATA}/superset:/app/superset_home"
dokku ps:set "${SUPERSET}" restart-policy unless-stopped
dokku docker-options:add "${SUPERSET}" deploy "--cpu-shares 256 --publish 127.0.0.1:${SUPERSET_PORT}:8088"
dokku resource:limit --memory 768m --cpu 1 "${SUPERSET}"
dokku network:set "${SUPERSET}" initial-network "${NETWORK}"
dokku proxy:disable "${SUPERSET}"
dokku checks:disable "${SUPERSET}"
ssh "${SERVER}" "sudo dokku config:set --no-restart ${SUPERSET} SUPERSET_SECRET_KEY=\$(openssl rand -hex 32) >/dev/null"

main apps:create "${PROXY}"
main domains:set "${PROXY}" "${DOMAIN}"
main ports:set "${PROXY}" http:80:8088
main nginx:set "${PROXY}" proxy-read-timeout "${QUERY_TIMEOUT}"
main resource:limit --memory 32m --cpu 0.25 "${PROXY}"
echo "Then: bash dokku/database-tunnel.sh; bash dokku/database-deploy.sh; ssh ${MAIN} letsencrypt:enable ${PROXY}"
