readonly LAKE_HOST=dokku
readonly LAKE_APP=retro-speedlab-lake
readonly BUCKET=retro-speedlab-lake

garage() {
    ssh "${LAKE_HOST}" enter "${LAKE_APP}" web /garage "$@"
}

vault_key() {
    local name="$1" app="$2" shown="ssh ${LAKE_HOST} enter ${LAKE_APP} web /garage key info --show-secret $1"
    vault_step "LAKE_ACCESS_KEY_ID@${app}" "${app}" "the Key ID from ${shown}"
    vault_step "LAKE_SECRET_ACCESS_KEY@${app}" "${app}" "the Secret key from ${shown}"
}
