#!/usr/bin/env bash
set -euo pipefail

readonly HOST=dokku
readonly APP=retro-speedlab-lake
readonly BUCKET=retro-speedlab-lake
readonly CAPACITY=10G
readonly WRITER=lakehouse
readonly READER=backend
readonly WRITER_HOST=dokku-black-omen
readonly WRITER_APP=retro-speedlab-lakehouse
readonly READER_APP=retro-speedlab-backend

garage() {
    ssh "${HOST}" enter "${APP}" web /garage "$@"
}

store_key() {
    local name="$1" host="$2" app="$3" info id secret
    info="$(garage key info --show-secret "${name}")"
    id="$(grep -oP 'Key ID: \K\S+' <<<"${info}")"
    secret="$(grep -oP 'Secret key: \K\S+' <<<"${info}")"
    ssh "${host}" config:set --no-restart "${app}" LAKE_ACCESS_KEY_ID="${id}" LAKE_SECRET_ACCESS_KEY="${secret}" >/dev/null
}

node="$(garage node id -q | cut -d@ -f1)"
garage layout assign -z home -c "${CAPACITY}" "${node}"
garage layout apply --version 1
garage bucket create "${BUCKET}"
garage key create "${WRITER}" >/dev/null
garage key create "${READER}" >/dev/null
garage bucket allow --read --write --owner "${BUCKET}" --key "${WRITER}"
garage bucket allow --read "${BUCKET}" --key "${READER}"
store_key "${WRITER}" "${WRITER_HOST}" "${WRITER_APP}"
store_key "${READER}" "${HOST}" "${READER_APP}"
