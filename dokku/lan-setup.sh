#!/usr/bin/env bash
set -euo pipefail

readonly PLACEHOLDER=retro-speedlab-lan
readonly DOMAINS=(database.retrospeedlab.com growthbook.retrospeedlab.com growthbook-api.retrospeedlab.com)
readonly CERTS_USER=lan-certs
readonly CERTS_HOME=/var/lib/lan-certs
readonly PLACEHOLDER_TLS=/home/dokku/${PLACEHOLDER}/tls
readonly CERTS=/etc/retro-speedlab/lan-certs
readonly SERVICE=retro-speedlab-lan-certs
readonly PULL=/usr/local/sbin/${SERVICE}
readonly NGINX_INCLUDE=/etc/nginx/retro-speedlab-lan/server.conf
readonly NGINX_SITE=/etc/nginx/conf.d/retro-speedlab-lan.conf
readonly MAIN_DEFAULT=/etc/nginx/conf.d/00-default-site.conf
readonly ROUTE_PROBE=1.1.1.1
readonly ANSWER_SECONDS=10

source "$(dirname "${BASH_SOURCE[0]}")/apps.sh"
LAN_FILES="$(cd "${APPS_SCRIPTS}/../lan" && pwd)"
readonly LAN_FILES
MAIN_ADDRESS="$(ssh -G "${MAIN_SHELL}" | awk '/^hostname /{print $2}')"
readonly MAIN_ADDRESS
read -r LAN_ADDRESS LAN_INTERFACE < <(ssh "${HOME_LAB}" "ip -4 route get ${ROUTE_PROBE}" \
    | awk '{for (i = 1; i < NF; i++) {if ($i == "src") address = $(i + 1); if ($i == "dev") device = $(i + 1)}}
        END {print address, device}')
readonly LAN_ADDRESS LAN_INTERFACE

step() {
    echo "==> $1"
}

install_on() {
    local host="$1" target="$2" mode="$3"
    ssh "${host}" "cat > /tmp/lan-file && if sudo cmp -s /tmp/lan-file ${target}; then rm /tmp/lan-file \
        && echo '    unchanged ${host}:${target}'; else sudo install -D -m ${mode} /tmp/lan-file ${target} \
        && rm /tmp/lan-file && echo '    installed ${host}:${target}'; fi"
}

reload_nginx() {
    local host="$1" file="$2"
    ssh "${host}" "sudo nginx -t -q || { sudo rm ${file}; echo '    removed ${file}, nginx rejected it' >&2; exit 1; }
        sudo systemctl reload nginx && echo '    reloaded nginx on ${host}'"
}

placeholder() {
    step "Main: ${PLACEHOLDER} holds ${DOMAINS[*]} and renews their certificate"
    ensure_app "${MAIN_DOKKU}" "${PLACEHOLDER}"
    ssh "${MAIN_DOKKU}" domains:set "${PLACEHOLDER}" "${DOMAINS[@]}"
    ssh "${MAIN_DOKKU}" resource:limit --memory 16m --cpu 0.1 "${PLACEHOLDER}" >/dev/null
    if [[ "$(ssh "${MAIN_DOKKU}" ps:report "${PLACEHOLDER}" --deployed)" == true ]]; then
        echo "    ${PLACEHOLDER} is already deployed"
    else
        bash "${APPS_SCRIPTS}/push-dir.sh" "${MAIN_DOKKU}" "${PLACEHOLDER}" lan/placeholder
    fi
    if [[ "$(ssh "${MAIN_DOKKU}" letsencrypt:active "${PLACEHOLDER}")" == true ]]; then
        echo "    the certificate is already active"
    else
        ssh "${MAIN_DOKKU}" letsencrypt:enable "${PLACEHOLDER}"
    fi
}

main_names() {
    ssh "${MAIN_SHELL}" "sudo nginx -T 2>/dev/null" \
        | awk '$1 == "server_name" {for (i = 2; i <= NF; i++) {gsub(";", "", $i); if ($i != "_") print $i}}' | sort -u
}

main_answers() {
    local name
    for name in $(main_names); do
        echo "${name} $(curl -s -o /dev/null -m "${ANSWER_SECONDS}" -w '%{http_code}' \
            --resolve "${name}:443:${MAIN_ADDRESS}" "https://${name}/")"
    done
}

main_default_site() {
    step "Main: unknown names get no answer instead of another app"
    local before after
    before="$(main_answers)"
    install_on "${MAIN_SHELL}" "${MAIN_DEFAULT}" 644 < "${LAN_FILES}/main-default.conf"
    reload_nginx "${MAIN_SHELL}" "${MAIN_DEFAULT}"
    after="$(main_answers)"
    if [[ "${before}" != "${after}" ]]; then
        diff <(echo "${before}") <(echo "${after}") >&2 || true
        ssh "${MAIN_SHELL}" "sudo rm ${MAIN_DEFAULT} && sudo systemctl reload nginx"
        echo "    removed ${MAIN_DEFAULT} again: these names answered differently with it" >&2
        exit 1
    fi
    echo "    all $(wc -l <<< "${after}") names on Main answer as before"
}

certificate_reader() {
    local public_key="$1" export_command="/usr/bin/tar -C ${PLACEHOLDER_TLS} -c server.crt server.key"
    if ssh "${MAIN_SHELL}" "id ${CERTS_USER}" >/dev/null 2>&1; then
        echo "    user ${CERTS_USER} exists on Main"
    else
        ssh "${MAIN_SHELL}" "sudo useradd --system --create-home --home-dir ${CERTS_HOME} --shell /bin/sh ${CERTS_USER}"
        echo "    created user ${CERTS_USER} on Main"
    fi
    echo "${CERTS_USER} ALL=(root) NOPASSWD: ${export_command}" \
        | ssh "${MAIN_SHELL}" "cat > /tmp/lan-sudoers && sudo visudo -cqf /tmp/lan-sudoers && rm /tmp/lan-sudoers"
    echo "${CERTS_USER} ALL=(root) NOPASSWD: ${export_command}" \
        | install_on "${MAIN_SHELL}" "/etc/sudoers.d/${SERVICE}" 440
    echo "restrict,command=\"sudo -n ${export_command}\" ${public_key}" \
        | install_on "${MAIN_SHELL}" "${CERTS_HOME}/.ssh/authorized_keys" 644
}

certificate_pull() {
    step "Home Lab: fetch the certificate from Main now and daily"
    if ssh "${HOME_LAB}" "sudo test -f ${CERTS}/id_ed25519"; then
        echo "    the fetch key exists"
    else
        ssh "${HOME_LAB}" "sudo install -d -m 700 ${CERTS} \
            && sudo ssh-keygen -q -t ed25519 -N '' -C ${SERVICE}@${HOME_LAB} -f ${CERTS}/id_ed25519"
        echo "    created the fetch key"
    fi
    certificate_reader "$(ssh "${HOME_LAB}" "sudo cat ${CERTS}/id_ed25519.pub")"
    echo "${MAIN_ADDRESS} $(ssh "${MAIN_SHELL}" "cut -d' ' -f1,2 /etc/ssh/ssh_host_ed25519_key.pub")" \
        | install_on "${HOME_LAB}" "${CERTS}/known_hosts" 644
    install_on "${HOME_LAB}" "${PULL}" 755 < "${LAN_FILES}/pull-certs.sh"
    sed "s/{{MAIN_ADDRESS}}/${MAIN_ADDRESS}/" "${LAN_FILES}/${SERVICE}.service" \
        | install_on "${HOME_LAB}" "/etc/systemd/system/${SERVICE}.service" 644
    install_on "${HOME_LAB}" "/etc/systemd/system/${SERVICE}.timer" 644 < "${LAN_FILES}/${SERVICE}.timer"
    ssh "${HOME_LAB}" "sudo systemctl daemon-reload && sudo systemctl start ${SERVICE} \
        && sudo systemctl enable --now --quiet ${SERVICE}.timer \
        && echo \"    \$(sudo journalctl -u ${SERVICE} -n 1 -o cat --no-pager)\""
}

lan_proxy() {
    step "Home Lab: nginx serves the tools on ${LAN_ADDRESS} only"
    install_on "${HOME_LAB}" "${NGINX_INCLUDE}" 644 < "${LAN_FILES}/server.conf"
    sed "s/{{LAN_ADDRESS}}/${LAN_ADDRESS}/" "${LAN_FILES}/tools.conf" | install_on "${HOME_LAB}" "${NGINX_SITE}" 644
    reload_nginx "${HOME_LAB}" "${NGINX_SITE}"
    ssh "${HOME_LAB}" "sudo ufw allow in on ${LAN_INTERFACE} to ${LAN_ADDRESS} port 80,443 proto tcp \
        comment 'Retro Speedlab owner tools on the LAN'" | sed 's/^/    ufw: /'
}

placeholder
main_default_site
certificate_pull
lan_proxy
step "Point these names at ${LAN_ADDRESS} in the LAN's DNS"
for name in "${DOMAINS[@]}"; do
    echo "    ${LAN_ADDRESS} ${name}"
done
