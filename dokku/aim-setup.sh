#!/usr/bin/env bash
set -euo pipefail

readonly IMAGE=aimstack/aim:3.29.1@sha256:e74587a7e49610644a65830de3b14877dc899bf75196a16a0b25c359c5480635
readonly UI=retro-speedlab-aim
readonly SERVER=retro-speedlab-aim-server
readonly UI_PORT=18093
readonly AIM_UI_PORT=43800
readonly AIM_SERVER_PORT=53800
readonly DATA=/mnt/fastdata/aim
readonly REPO=/aim
readonly NETWORK=retro-speedlab-aim
readonly LAKEHOUSE_NETWORK=retro-speedlab-lakehouse

source "$(dirname "${BASH_SOURCE[0]}")/apps.sh"

dokku() {
    ssh "${HOME_LAB_DOKKU}" "$@"
}

ssh "${HOME_LAB}" "sudo -n test -d ${DATA}/.aim || { sudo -n install -d -m 700 ${DATA} \
    && sudo -n docker run --rm --volume ${DATA}:${REPO} ${IMAGE} init --repo ${REPO}; }"
ensure_network "${NETWORK}"
ensure_network "${LAKEHOUSE_NETWORK}"

home_app "${SERVER}" "${NETWORK}" 384m 0.5 ""
ensure_mount "${SERVER}" "${DATA}" "${REPO}"
dokku ps:set "${SERVER}" dockerfile-start-cmd "'server --host 0.0.0.0 --port ${AIM_SERVER_PORT} --repo ${REPO}'"
deploy_image "${SERVER}" "${IMAGE}"

home_app "${UI}" "${LAKEHOUSE_NETWORK}" 512m 0.5 "--publish 127.0.0.1:${UI_PORT}:${AIM_UI_PORT}"
ensure_mount "${UI}" "${DATA}" "${REPO}"
dokku ps:set "${UI}" dockerfile-start-cmd "'up --host 0.0.0.0 --port ${AIM_UI_PORT} --workers 1 --repo ${REPO}'"
deploy_image "${UI}" "${IMAGE}"
echo "Then: bash dokku/lan-setup.sh"
