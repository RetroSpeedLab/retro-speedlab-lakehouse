#!/usr/bin/env bash
set -euo pipefail

readonly HOST=dokku-black-omen
readonly SERVER=black-omen
readonly APP=retro-speedlab-lakehouse
readonly DATA=/mnt/fastdata/lakehouse/data
readonly NETWORK=retro-speedlab-lakehouse
readonly TRACES_NETWORK=retro-speedlab-traces
readonly RUNNER_WORKING=/mnt/fastdata/dokku/retro-speedlab-runner/working

source "$(dirname "${BASH_SOURCE[0]}")/settings.sh"
source "$(dirname "${BASH_SOURCE[0]}")/apps.sh"

dokku() {
    ssh "${HOST}" "$@"
}

ssh "${SERVER}" "mkdir -p ${DATA}/state ${DATA}/bronze ${DATA}/checkpoints ${DATA}/airflow && echo 0 > ${DATA}/state/relay-cursor \
    && sudo chown -R ${LAKEHOUSE_USER}:${LAKEHOUSE_USER} ${DATA}"
dokku apps:create "${APP}"
dokku storage:mount "${APP}" "${DATA}:/lake"
dokku storage:mount "${APP}" "${RUNNER_WORKING}:/runner:ro"
dokku docker-options:add "${APP}" build "--network host"
dokku docker-options:add "${APP}" deploy "--cpu-shares 2048 --init"
dokku ps:set "${APP}" restart-policy unless-stopped
dokku network:set "${APP}" attach-post-deploy "${NETWORK}" "${TRACES_NETWORK}"
dokku resource:limit --process-type stream --memory 2560m --cpu 4 "${APP}"
dokku resource:limit --process-type relay --memory 256m --cpu 0.25 "${APP}"
dokku resource:limit --process-type chat --memory 256m --cpu 0.1 "${APP}"
dokku resource:limit --process-type airflow --memory 3584m --cpu 2 "${APP}"
dokku proxy:disable "${APP}"
dokku checks:disable "${APP}"
echo "Fill the wheelhouse once from WSL: bash dokku/wheelhouse.sh; the lake keys come from dokku/lake-keys.sh"
require_secret "${HOST}" "${APP}" AIRFLOW__API_AUTH__JWT_SECRET "the output of openssl rand -hex 32"
