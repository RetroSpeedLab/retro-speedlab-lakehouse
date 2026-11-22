#!/usr/bin/env bash
set -euo pipefail

readonly APP=retro-speedlab-hosts
readonly BACKEND_HOST=dokku
readonly BACKEND_APP=retro-speedlab-backend
readonly BACKEND_URL=https://api.retrospeedlab.com
readonly STORAGE=/var/lib/dokku/data/storage
readonly HOME_LAB_DATA=/mnt/fastdata/retro-speedlab-hosts

setup() {
    local host="$1" name="$2" key
    ssh "${host}" apps:create "${APP}"
    ssh "${host}" storage:ensure-directory "${APP}"
    ssh "${host}" storage:mount "${APP}" "${STORAGE}/${APP}:/disks/root:ro"
    ssh "${host}" docker-options:add "${APP}" deploy "--init"
    ssh "${host}" ps:set "${APP}" restart-policy unless-stopped
    ssh "${host}" resource:limit --memory 96m --cpu 0.1 "${APP}"
    ssh "${host}" proxy:disable "${APP}"
    ssh "${host}" checks:disable "${APP}"
    key="$(ssh "${BACKEND_HOST}" config:get "${BACKEND_APP}" UPLOAD_API_KEY)"
    [[ -n "${key}" ]] || { echo "${BACKEND_APP} has no UPLOAD_API_KEY" >&2; exit 1; }
    ssh "${host}" config:set --no-restart "${APP}" HOST_NAME="${name}" BACKEND_URL="${BACKEND_URL}" \
        UPLOAD_API_KEY="${key}" >/dev/null
}

setup dokku main
setup dokku-black-omen home-lab
ssh black-omen "sudo mkdir -p ${HOME_LAB_DATA}"
ssh dokku-black-omen storage:mount "${APP}" "${HOME_LAB_DATA}:/disks/data:ro"
echo "Deploy the collectors after the lakehouse knows hosts.metrics: bash dokku/hosts-deploy.sh"
