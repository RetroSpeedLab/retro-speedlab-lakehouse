#!/usr/bin/env bash
set -euo pipefail

readonly POSTGRES=retro-speedlab-listmonk-postgres
readonly POSTGRES_IMAGE=postgres:17.12-alpine@sha256:a77aa70e641402a21411b92b721c58d10f23b50ae5fdd2333efd22d166e1e5f7
readonly LISTMONK=retro-speedlab-listmonk
readonly PROXY=retro-speedlab-newsletter
readonly DOMAIN=newsletter.retrospeedlab.com
readonly PORT=18091
readonly DATA=/mnt/fastdata/newsletter
readonly NETWORK=retro-speedlab-newsletter
readonly DATABASE=listmonk
readonly ADMIN_USER=owner

source "$(dirname "${BASH_SOURCE[0]}")/apps.sh"

dokku() {
    ssh "${HOME_LAB_DOKKU}" "$@"
}

ssh "${HOME_LAB}" "mkdir -p ${DATA}/postgres ${DATA}/uploads && chmod 700 ${DATA}"
ensure_network "${NETWORK}"

home_app "${POSTGRES}" "${NETWORK}" 256m 0.5 ""
ensure_mount "${POSTGRES}" "${DATA}/postgres" /var/lib/postgresql/data
dokku config:set --no-restart "${POSTGRES}" POSTGRES_USER="${DATABASE}" POSTGRES_DB="${DATABASE}" >/dev/null
set_once "${POSTGRES}" POSTGRES_PASSWORD "openssl rand -hex 24"
deploy_image "${POSTGRES}" "${POSTGRES_IMAGE}"

home_app "${LISTMONK}" "${NETWORK}" 256m 0.5 "--publish 127.0.0.1:${PORT}:9000"
ensure_mount "${LISTMONK}" "${DATA}/uploads" /listmonk/uploads
dokku config:set --no-restart "${LISTMONK}" TZ=Etc/UTC LISTMONK_app__address=0.0.0.0:9000 \
    LISTMONK_db__host="${POSTGRES}" LISTMONK_db__port=5432 LISTMONK_db__user="${DATABASE}" \
    LISTMONK_db__database="${DATABASE}" LISTMONK_db__ssl_mode=disable LISTMONK_db__max_open=10 \
    LISTMONK_db__max_idle=10 LISTMONK_db__max_lifetime=300s LISTMONK_ADMIN_USER="${ADMIN_USER}" >/dev/null
set_once "${LISTMONK}" LISTMONK_db__password "dokku config:get ${POSTGRES} POSTGRES_PASSWORD"
set_once "${LISTMONK}" LISTMONK_ADMIN_PASSWORD "openssl rand -hex 16"
bash "${APPS_SCRIPTS}/push-dir.sh" "${HOME_LAB_DOKKU}" "${LISTMONK}" database/listmonk

owner_proxy "${PROXY}" "${DOMAIN}" "${PORT}"
