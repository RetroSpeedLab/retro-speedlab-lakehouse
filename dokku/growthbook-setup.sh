#!/usr/bin/env bash
set -euo pipefail

readonly MONGO=retro-speedlab-growthbook-mongo
readonly MONGO_IMAGE=mongo:8.0.38@sha256:999744ea9784b56ca8a48cb47d697b78290c817feaf7ffb6946f44171b2d4055
readonly GROWTHBOOK=retro-speedlab-growthbook
readonly GROWTHBOOK_IMAGE=growthbook/growthbook:5.2.1@sha256:0da8afcb9ed92d8e09abca3ea9d9531e29b7e39b0bccba03d9a3d4cc47f1f60d
readonly UI_DOMAIN=growthbook.retrospeedlab.com
readonly API_DOMAIN=growthbook-api.retrospeedlab.com
readonly UI_PORT=18089
readonly API_PORT=18090
readonly DATA=/mnt/fastdata/growthbook
readonly NETWORK=retro-speedlab-growthbook
readonly DATABASE_USER=growthbook
readonly ADMIN_EMAIL=owner@retrospeedlab.com
readonly ADMIN_COMPANY="Retro Speedlab"
readonly READY_TRIES=60

source "$(dirname "${BASH_SOURCE[0]}")/apps.sh"

dokku() {
    ssh "${HOME_LAB_DOKKU}" "$@"
}

create_admin() {
    local organizations
    organizations="$(ssh "${HOME_LAB}" "for try in \$(seq ${READY_TRIES}); do \
        curl -fsS http://127.0.0.1:${API_PORT}/auth/hasorgs && exit 0; sleep 5; done; exit 1")"
    [[ "${organizations}" == *'"hasOrganizations":false'* ]] || return 0
    ssh "${HOME_LAB}" "printf '{\"email\":\"%s\",\"name\":\"Owner\",\"password\":\"%s\",\"companyname\":\"%s\"}' \
        ${ADMIN_EMAIL} \"\$(dokku config:get ${GROWTHBOOK} GROWTHBOOK_ADMIN_PASSWORD)\" '${ADMIN_COMPANY}' \
        | curl -fsS -o /dev/null -H 'Content-Type: application/json' --data @- http://127.0.0.1:${API_PORT}/auth/firsttime"
    echo "Created the GrowthBook admin ${ADMIN_EMAIL}"
}

ssh "${HOME_LAB}" "mkdir -p ${DATA}/mongo ${DATA}/uploads && chmod 700 ${DATA}"
ensure_network "${NETWORK}"

home_app "${MONGO}" "${NETWORK}" 512m 0.5 ""
ensure_mount "${MONGO}" "${DATA}/mongo" /data/db
dokku config:set --no-restart "${MONGO}" MONGO_INITDB_ROOT_USERNAME="${DATABASE_USER}" >/dev/null
set_once "${MONGO}" MONGO_INITDB_ROOT_PASSWORD "openssl rand -hex 24"
deploy_image "${MONGO}" "${MONGO_IMAGE}"

home_app "${GROWTHBOOK}" "${NETWORK}" 1280m 1 \
    "--publish 127.0.0.1:${UI_PORT}:3000 --publish 127.0.0.1:${API_PORT}:3100"
ensure_mount "${GROWTHBOOK}" "${DATA}/uploads" /usr/local/src/app/packages/back-end/uploads
dokku config:set --no-restart "${GROWTHBOOK}" NODE_ENV=production BACKEND_PORT=3100 \
    APP_ORIGIN="https://${UI_DOMAIN}" API_HOST="https://${API_DOMAIN}" \
    MONGODB_HOST="${MONGO}" MONGODB_USERNAME="${DATABASE_USER}" GROWTHBOOK_ADMIN_EMAIL="${ADMIN_EMAIL}" >/dev/null
set_once "${GROWTHBOOK}" MONGODB_PASSWORD "dokku config:get ${MONGO} MONGO_INITDB_ROOT_PASSWORD"
set_once "${GROWTHBOOK}" JWT_SECRET "openssl rand -hex 32"
set_once "${GROWTHBOOK}" ENCRYPTION_KEY "openssl rand -hex 32"
set_once "${GROWTHBOOK}" GROWTHBOOK_ADMIN_PASSWORD "openssl rand -hex 16"
deploy_image "${GROWTHBOOK}" "${GROWTHBOOK_IMAGE}"
create_admin
echo "Then: bash dokku/lan-setup.sh"
