readonly HOME_LAB=black-omen
readonly HOME_LAB_DOKKU=dokku-black-omen
readonly MAIN_DOKKU=dokku
readonly MAIN_SHELL=jumphost
readonly INTERACTIVE_CPU_SHARES=2048
APPS_SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly APPS_SCRIPTS

ensure_app() {
    local host="$1" app="$2"
    ssh "${host}" apps:exists "${app}" >/dev/null 2>&1 || ssh "${host}" apps:create "${app}"
}

ensure_network() {
    ssh "${HOME_LAB_DOKKU}" network:exists "$1" >/dev/null 2>&1 || ssh "${HOME_LAB_DOKKU}" network:create "$1"
}

ensure_mount() {
    local app="$1" source="$2" target="$3"
    [[ "$(ssh "${HOME_LAB_DOKKU}" storage:list "${app}")" == *"${source}:${target}"* ]] \
        || ssh "${HOME_LAB_DOKKU}" storage:mount "${app}" "${source}:${target}"
}

home_app() {
    local app="$1" network="$2" memory="$3" cpu="$4" options="$5"
    ensure_app "${HOME_LAB_DOKKU}" "${app}"
    ssh "${HOME_LAB_DOKKU}" ps:set "${app}" restart-policy unless-stopped
    ssh "${HOME_LAB_DOKKU}" docker-options:clear "${app}" deploy
    ssh "${HOME_LAB_DOKKU}" docker-options:add "${app}" deploy "--cpu-shares ${INTERACTIVE_CPU_SHARES} --network-alias ${app} ${options}"
    ssh "${HOME_LAB_DOKKU}" resource:limit --memory "${memory}" --cpu "${cpu}" "${app}"
    ssh "${HOME_LAB_DOKKU}" network:set "${app}" initial-network "${network}"
    ssh "${HOME_LAB_DOKKU}" network:set "${app}" bind-all-interfaces false
    ssh "${HOME_LAB_DOKKU}" proxy:disable "${app}"
    ssh "${HOME_LAB_DOKKU}" checks:disable "${app}"
}

set_once() {
    local app="$1" name="$2" command="$3"
    ssh "${HOME_LAB}" "dokku config:get ${app} ${name} >/dev/null || { value=\$(${command}) && test -n \"\${value}\" \
        && dokku config:set --no-restart ${app} ${name}=\"\${value}\" >/dev/null; }"
}

deploy_image() {
    local app="$1" image="$2"
    if [[ "$(ssh "${HOME_LAB_DOKKU}" git:report "${app}" --git-source-image)" == "${image}" ]]; then
        ssh "${HOME_LAB_DOKKU}" ps:restart "${app}"
    else
        ssh "${HOME_LAB_DOKKU}" git:from-image "${app}" "${image}"
    fi
}
