#!/usr/bin/env bash
set -euo pipefail

readonly BACKEND=retro-speedlab-backend
readonly SSO_URL=https://sso.retrospeedlab.com
readonly SSO_DOMAIN=sso.retrospeedlab.com
readonly OWNER_DISCORD_ID=712191617095696493
readonly GATE=/etc/nginx/gate
readonly SECRETS=/etc/nginx/gate/secrets
readonly OWNER_GATE=/etc/nginx/conf.d/owner-gate.conf
readonly BUGSINK_GATE=/var/lib/dokku/data/storage/bugs-gate

source "$(dirname "${BASH_SOURCE[0]}")/apps.sh"
readonly GATE_FILES="${APPS_SCRIPTS}/../gate"

install_file() {
    local source="$1" target="$2" owner="$3" mode="$4"
    scp -q "${source}" "${MAIN_SHELL}:/tmp/gate-file"
    ssh "${MAIN_SHELL}" "sudo install -D -o ${owner} -g ${owner} -m ${mode} /tmp/gate-file ${target} && rm /tmp/gate-file"
}

write_secret() {
    local name="$1"
    ssh "${MAIN_SHELL}" "sudo install -d -m 700 ${SECRETS} && sudo sh -c 'umask 077 && cat > ${SECRETS}/${name}.conf'"
}

owner_hosts() {
    local conf app
    {
        for conf in "${GATE_FILES}"/apps/*.conf; do
            app="$(basename "${conf}" .conf)"
            ssh "${MAIN_DOKKU}" domains:report "${app}" --domains-app-vhosts | tr ' ' '\n'
        done
        printf '%s\n' "${LAN_DOMAINS[@]}"
    } | paste -sd, -
}

sign_in_host() {
    if [[ "$(ssh "${MAIN_DOKKU}" domains:report "${BACKEND}" --domains-app-vhosts)" != *"${SSO_DOMAIN}"* ]]; then
        ssh "${MAIN_DOKKU}" domains:add "${BACKEND}" "${SSO_DOMAIN}"
        ssh "${MAIN_DOKKU}" letsencrypt:enable "${BACKEND}"
    fi
    require_secret "${MAIN_DOKKU}" "${BACKEND}" SSO_SECRET "the output of openssl rand -hex 32"
    local hosts
    hosts="$(owner_hosts)"
    [[ "$(ssh "${MAIN_DOKKU}" config:get "${BACKEND}" SSO_HOSTS)" == "${hosts}" ]] && return
    ssh "${MAIN_DOKKU}" config:set --no-restart "${BACKEND}" "SSO_URL=${SSO_URL}" \
        "SSO_OWNER_ID=${OWNER_DISCORD_ID}" "SSO_HOSTS=${hosts}"
    ssh "${MAIN_DOKKU}" ps:rebuild "${BACKEND}"
}

shared_files() {
    install_file "${GATE_FILES}/owner-gate.conf" "${OWNER_GATE}" root 644
    local file
    for file in gate.conf proxy.conf signed-in.conf framed.conf umami-login.html; do
        install_file "${GATE_FILES}/${file}" "${GATE}/${file}" root 644
    done
}

secrets_for() {
    case "$1" in
        bugs)
            ssh "${MAIN_SHELL}" "sudo docker exec bugs.web.1 bugsink-manage shell -c \
                'from users.models import User; print(User.objects.get(is_superuser=True).username)' | tail -1" \
                | xargs printf 'proxy_set_header X-Gate-User "%s";\n' | write_secret bugs ;;
        tracking)
            printf "proxy_set_body '{\"username\":\"%s\",\"password\":\"%s\"}';\n" \
                "$(plain_secret "${MAIN_SHELL}" "${BACKEND}" UMAMI_USERNAME)" \
                "$(plain_secret "${MAIN_SHELL}" "${BACKEND}" UMAMI_PASSWORD)" | write_secret tracking ;;
    esac
}

gate_app() {
    local app="$1"
    install_file "${GATE_FILES}/apps/${app}.conf" "/home/dokku/${app}/nginx.conf.d/gate.conf" dokku 644
    ssh "${MAIN_SHELL}" "sudo rm -f /home/dokku/${app}/nginx.conf.d/registration.conf"
    ssh "${MAIN_DOKKU}" proxy:build-config "${app}"
}

trust_gate() {
    case "$1" in
        bugs)
            install_file "${GATE_FILES}/bugsink/settings.py" "${BUGSINK_GATE}/settings.py" root 644
            install_file "${GATE_FILES}/bugsink/middleware.py" "${BUGSINK_GATE}/middleware.py" root 644
            [[ "$(ssh "${MAIN_DOKKU}" storage:list bugs)" == *"${BUGSINK_GATE}:/app/gate"* ]] \
                || ssh "${MAIN_DOKKU}" storage:mount bugs "${BUGSINK_GATE}:/app/gate"
            ssh "${MAIN_DOKKU}" config:set bugs DJANGO_SETTINGS_MODULE=gate.settings ;;
        uptime)
            scp -q "${GATE_FILES}/uptime-disable-auth.js" "${MAIN_SHELL}:/tmp/uptime-disable-auth.js"
            ssh "${MAIN_SHELL}" "sudo docker cp /tmp/uptime-disable-auth.js uptime.web.1:/tmp/disable-auth.js \
                && sudo docker exec uptime.web.1 node /tmp/disable-auth.js && rm /tmp/uptime-disable-auth.js"
            ssh "${MAIN_DOKKU}" ps:restart uptime ;;
    esac
}

sign_in_host
shared_files
for app in "$@"; do
    [[ -f "${GATE_FILES}/apps/${app}.conf" ]] || { echo "gate/apps has no ${app}.conf" >&2; exit 1; }
    secrets_for "${app}"
    gate_app "${app}"
    trust_gate "${app}"
    echo "${app} signs in through ${SSO_URL}"
done
