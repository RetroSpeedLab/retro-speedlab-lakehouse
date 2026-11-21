#!/usr/bin/env bash
set -euo pipefail

readonly TEMPO=retro-speedlab-tempo
readonly TEMPO_IMAGE=grafana/tempo:2.10.10@sha256:12b0820f13ffa10ce956b6e9acc3b03f3cffded99109be6cc09d106fe56e5d38
readonly POSTGRES=retro-speedlab-experiments-postgres
readonly POSTGRES_IMAGE=postgres:16.15-alpine@sha256:aa6cb33cf4c19c765cefcf48d96176258800870a9a1e930ad126200f9d825139
readonly GRAFANA=retro-speedlab-grafana
readonly GRAFANA_IMAGE=grafana/grafana:13.3.0@sha256:488984b716c0862639f91a698adbe2862677a5a07bad42da5f03f3030883b205
readonly GRAFANA_URL=https://grafana.retrospeedlab.com/
readonly GRAFANA_PORT=18094
readonly TEMPO_DATA=/mnt/fastdata/tempo
readonly GRAFANA_DATA=/mnt/fastdata/grafana
readonly POSTGRES_DATA=/mnt/fastdata/experiments
readonly TEMPO_USER=10001
readonly GRAFANA_USER=472
readonly TRACES_NETWORK=retro-speedlab-traces
readonly LAKEHOUSE=retro-speedlab-lakehouse
readonly LAKEHOUSE_NETWORK=retro-speedlab-lakehouse
readonly CONFIG="$(dirname "${BASH_SOURCE[0]}")/../grafana"
readonly DATABASE=experiments
readonly WRITER=lakehouse
readonly READY_TRIES=60

source "$(dirname "${BASH_SOURCE[0]}")/apps.sh"

dokku() {
    ssh "${HOME_LAB_DOKKU}" "$@"
}

copy_config() {
    ssh "${HOME_LAB}" "sudo -n install -d -m 700 -o ${TEMPO_USER} -g ${TEMPO_USER} ${TEMPO_DATA} ${TEMPO_DATA}/data \
        && sudo -n install -d -m 700 -o ${GRAFANA_USER} -g ${GRAFANA_USER} ${GRAFANA_DATA} ${GRAFANA_DATA}/data \
        && sudo -n install -d -m 700 ${POSTGRES_DATA} ${POSTGRES_DATA}/postgres \
        && sudo -n rm -rf ${TEMPO_DATA}/config ${GRAFANA_DATA}/config"
    ssh "${HOME_LAB}" "sudo -n install -d ${TEMPO_DATA}/config && sudo -n tee ${TEMPO_DATA}/config/tempo.yaml >/dev/null" \
        <"${CONFIG}/tempo.yaml"
    tar -C "${CONFIG}" -c provisioning dashboards \
        | ssh "${HOME_LAB}" "sudo -n install -d ${GRAFANA_DATA}/config && sudo -n tar -C ${GRAFANA_DATA}/config -x --no-same-owner"
}

create_reader() {
    ssh "${HOME_LAB}" "for try in \$(seq ${READY_TRIES}); do \
        sudo -n docker exec ${POSTGRES}.web.1 pg_isready -q -U ${WRITER} && break; sleep 5; done; \
        { printf '\\set password %s\n' \"\$(dokku config:get ${GRAFANA} EXPERIMENTS_READER_PASSWORD)\"; cat; } \
        | sudo -n docker exec -i ${POSTGRES}.web.1 psql -q -v ON_ERROR_STOP=1 -U ${WRITER} -d ${DATABASE}" \
        <"${CONFIG}/reader.sql"
}

copy_config
ensure_network "${TRACES_NETWORK}"
ensure_network "${LAKEHOUSE_NETWORK}"

home_app "${TEMPO}" "${TRACES_NETWORK}" 384m 0.5 ""
ensure_mount "${TEMPO}" "${TEMPO_DATA}/config" /etc/tempo
ensure_mount "${TEMPO}" "${TEMPO_DATA}/data" /var/tempo
dokku config:set --no-restart "${TEMPO}" GOMEMLIMIT=320MiB >/dev/null
dokku ps:set -- "${TEMPO}" dockerfile-start-cmd -config.file=/etc/tempo/tempo.yaml
deploy_image "${TEMPO}" "${TEMPO_IMAGE}"

home_app "${POSTGRES}" "${LAKEHOUSE_NETWORK}" 256m 0.5 ""
ensure_mount "${POSTGRES}" "${POSTGRES_DATA}/postgres" /var/lib/postgresql/data
dokku config:set --no-restart "${POSTGRES}" POSTGRES_USER="${WRITER}" POSTGRES_DB="${DATABASE}" >/dev/null
set_once "${POSTGRES}" POSTGRES_PASSWORD "openssl rand -hex 24"
deploy_image "${POSTGRES}" "${POSTGRES_IMAGE}"

home_app "${GRAFANA}" "${LAKEHOUSE_NETWORK}" 384m 0.5 "--publish 127.0.0.1:${GRAFANA_PORT}:3000"
ensure_mount "${GRAFANA}" "${GRAFANA_DATA}/data" /var/lib/grafana
ensure_mount "${GRAFANA}" "${GRAFANA_DATA}/config/provisioning" /etc/grafana/provisioning
ensure_mount "${GRAFANA}" "${GRAFANA_DATA}/config/dashboards" /etc/grafana/dashboards
dokku network:set "${GRAFANA}" attach-post-deploy "${TRACES_NETWORK}"
dokku config:set --no-restart "${GRAFANA}" GF_SERVER_ROOT_URL="${GRAFANA_URL}" GF_SECURITY_ALLOW_EMBEDDING=true \
    GF_SECURITY_COOKIE_SECURE=true GF_SECURITY_COOKIE_SAMESITE=lax GF_SECURITY_DISABLE_GRAVATAR=true \
    GF_AUTH_ANONYMOUS_ENABLED=true GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer GF_USERS_VIEWERS_CAN_EDIT=true \
    GF_USERS_ALLOW_SIGN_UP=false GF_ANALYTICS_REPORTING_ENABLED=false GF_ANALYTICS_CHECK_FOR_UPDATES=false \
    GF_ANALYTICS_CHECK_FOR_PLUGIN_UPDATES=false GF_NEWS_NEWS_FEED_ENABLED=false GF_PLUGINS_PREINSTALL_DISABLED=true \
    GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/etc/grafana/dashboards/experiments.json GF_LOG_LEVEL=warn \
    GOMEMLIMIT=320MiB >/dev/null
set_once "${GRAFANA}" GF_SECURITY_ADMIN_PASSWORD "openssl rand -hex 16"
set_once "${GRAFANA}" EXPERIMENTS_READER_PASSWORD "openssl rand -hex 24"
deploy_image "${GRAFANA}" "${GRAFANA_IMAGE}"
create_reader
set_once "${LAKEHOUSE}" EXPERIMENTS_DATABASE_PASSWORD "dokku config:get ${POSTGRES} POSTGRES_PASSWORD"
