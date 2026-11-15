readonly LAKE_HOST=dokku
readonly LAKE_APP=retro-speedlab-lake
readonly BUCKET=retro-speedlab-lake

garage() {
    ssh "${LAKE_HOST}" enter "${LAKE_APP}" web /garage "$@"
}

store_key() {
    local name="$1" host="$2" app="$3" info id secret
    info="$(garage key info --show-secret "${name}")"
    id="$(grep -oP 'Key ID:\s+\K\S+' <<<"${info}")"
    secret="$(grep -oP 'Secret key:\s+\K\S+' <<<"${info}")"
    ssh "${host}" config:set --no-restart "${app}" LAKE_ACCESS_KEY_ID="${id}" LAKE_SECRET_ACCESS_KEY="${secret}" >/dev/null
}
