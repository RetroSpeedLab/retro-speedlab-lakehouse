readonly SETTINGS_SERVER=black-omen

copy_settings() {
    local host="$1" app="$2" target="$3" name
    shift 3
    for name in "$@"; do
        ssh "${host}" config:get "${app}" "${name}" \
            | ssh "${SETTINGS_SERVER}" "read -r value && test -n \"\${value}\" \
                && dokku config:set --no-restart ${target} ${name}=\"\${value}\" >/dev/null" \
            || { echo "${app} has no ${name}" >&2; exit 1; }
        echo "Copied ${name} from ${app} to ${target}"
    done
}
