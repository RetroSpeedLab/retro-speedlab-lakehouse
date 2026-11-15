#!/usr/bin/env bash
set -euo pipefail

readonly HOST=dokku-black-omen
readonly SERVER=black-omen
readonly APP=retro-speedlab-lakehouse
readonly BACKEND_HOST=dokku
readonly BACKEND_APP=retro-speedlab-backend
readonly DATA=/mnt/fastdata/lakehouse/data
readonly NETWORK=retro-speedlab-lakehouse
readonly RUNNER_WORKING=/mnt/fastdata/dokku/retro-speedlab-runner/working

dokku() {
    ssh "${HOST}" "$@"
}

copy_setting() {
    local host="$1" app="$2" name="$3" value
    value="$(ssh "${host}" config:get "${app}" "${name}")"
    [[ -n "${value}" ]] || { echo "${app} has no ${name}" >&2; exit 1; }
    dokku config:set --no-restart "${APP}" "${name}=${value}" >/dev/null
}

ssh "${SERVER}" "mkdir -p ${DATA}/state ${DATA}/bronze ${DATA}/checkpoints ${DATA}/airflow && echo 0 > ${DATA}/state/relay-cursor"
dokku apps:create "${APP}"
dokku storage:mount "${APP}" "${DATA}:/lake"
dokku storage:mount "${APP}" "${RUNNER_WORKING}:/runner:ro"
dokku docker-options:add "${APP}" build "--network host"
dokku docker-options:add "${APP}" deploy "--cpu-shares 512 --init"
dokku network:set "${APP}" attach-post-deploy "${NETWORK}"
dokku resource:limit --process-type stream --memory 3g --cpu 2 "${APP}"
dokku resource:limit --process-type relay --memory 256m --cpu 0.25 "${APP}"
dokku resource:limit --process-type airflow --memory 3584m --cpu 2 "${APP}"
dokku proxy:disable "${APP}"
dokku checks:disable "${APP}"
dokku config:set --no-restart "${APP}" AIRFLOW__API_AUTH__JWT_SECRET="$(openssl rand -hex 32)" >/dev/null
copy_setting "${BACKEND_HOST}" "${BACKEND_APP}" UPLOAD_API_KEY
echo "Fill the wheelhouse once from WSL: bash dokku/wheelhouse.sh; the lake keys come from dokku/lake-keys.sh"
