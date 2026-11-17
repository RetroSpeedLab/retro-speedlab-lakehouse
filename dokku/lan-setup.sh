#!/usr/bin/env bash
set -euo pipefail

readonly PLACEHOLDER=retro-speedlab-lan
readonly DOMAINS=(database.retrospeedlab.com growthbook.retrospeedlab.com growthbook-api.retrospeedlab.com
    newsletter.retrospeedlab.com)
readonly CERTS_USER=lan-certs
readonly CERTS_HOME=/var/lib/lan-certs
readonly PLACEHOLDER_TLS=/home/dokku/${PLACEHOLDER}/tls
readonly CERTS=/etc/retro-speedlab/lan-certs
readonly SERVICE=retro-speedlab-lan-certs
readonly PULL=/usr/local/sbin/${SERVICE}
readonly NGINX_INCLUDE=/etc/nginx/retro-speedlab-lan/server.conf
readonly NGINX_SITE=/etc/nginx/conf.d/retro-speedlab-lan.conf
readonly ROUTE_PROBE=1.1.1.1

source "$(dirname "${BASH_SOURCE[0]}")/apps.sh"
LAN_FILES="$(cd "${APPS_SCRIPTS}/../lan" && pwd)"
readonly LAN_FILES
MAIN_ADDRESS="$(ssh -G "${MAIN_SHELL}" | awk '/^hostname /{print $2}')"
readonly MAIN_ADDRESS
read -r LAN_ADDRESS LAN_INTERFACE < <(ssh "${HOME_LAB}" "ip -4 route get ${ROUTE_PROBE}" \
    | awk '{for (i = 1; i < NF; i++) {if ($i == "src") address = $(i + 1); if ($i == "dev") device = $(i + 1)}}
        END {print address, device}')
readonly LAN_ADDRESS LAN_INTERFACE

install_on() {
    local host="$1" target="$2" mode="$3"
    ssh "${host}" "cat > /tmp/lan-file && sudo install -D -m ${mode} /tmp/lan-file ${target} && rm /tmp/lan-file"
}

placeholder() {
    ensure_app "${MAIN_DOKKU}" "${PLACEHOLDER}"
    ssh "${MAIN_DOKKU}" domains:set "${PLACEHOLDER}" "${DOMAINS[@]}"
    ssh "${MAIN_DOKKU}" resource:limit --memory 16m --cpu 0.1 "${PLACEHOLDER}"
    bash "${APPS_SCRIPTS}/push-dir.sh" "${MAIN_DOKKU}" "${PLACEHOLDER}" lan/placeholder
    [[ "$(ssh "${MAIN_DOKKU}" letsencrypt:active "${PLACEHOLDER}")" == true ]] \
        || ssh "${MAIN_DOKKU}" letsencrypt:enable "${PLACEHOLDER}"
}

certificate_reader() {
    local public_key="$1" export_command="/usr/bin/tar -C ${PLACEHOLDER_TLS} -c server.crt server.key"
    ssh "${MAIN_SHELL}" "id ${CERTS_USER} >/dev/null 2>&1 \
        || sudo useradd --system --create-home --home-dir ${CERTS_HOME} --shell /bin/sh ${CERTS_USER}"
    echo "${CERTS_USER} ALL=(root) NOPASSWD: ${export_command}" | ssh "${MAIN_SHELL}" "cat > /tmp/lan-sudoers \
        && sudo visudo -cqf /tmp/lan-sudoers && sudo install -m 440 /tmp/lan-sudoers /etc/sudoers.d/${SERVICE} \
        && rm /tmp/lan-sudoers"
    echo "restrict,command=\"sudo -n ${export_command}\" ${public_key}" \
        | install_on "${MAIN_SHELL}" "${CERTS_HOME}/.ssh/authorized_keys" 644
}

certificate_pull() {
    ssh "${HOME_LAB}" "sudo install -d -m 700 ${CERTS} \
        && { sudo test -f ${CERTS}/id_ed25519 || sudo ssh-keygen -q -t ed25519 -N '' -C ${SERVICE}@${HOME_LAB} -f ${CERTS}/id_ed25519; }"
    certificate_reader "$(ssh "${HOME_LAB}" "sudo cat ${CERTS}/id_ed25519.pub")"
    echo "${MAIN_ADDRESS} $(ssh "${MAIN_SHELL}" "cut -d' ' -f1,2 /etc/ssh/ssh_host_ed25519_key.pub")" \
        | install_on "${HOME_LAB}" "${CERTS}/known_hosts" 644
    install_on "${HOME_LAB}" "${PULL}" 755 < "${LAN_FILES}/pull-certs.sh"
    sed "s/{{MAIN_ADDRESS}}/${MAIN_ADDRESS}/" "${LAN_FILES}/${SERVICE}.service" \
        | install_on "${HOME_LAB}" "/etc/systemd/system/${SERVICE}.service" 644
    install_on "${HOME_LAB}" "/etc/systemd/system/${SERVICE}.timer" 644 < "${LAN_FILES}/${SERVICE}.timer"
    ssh "${HOME_LAB}" "sudo systemctl daemon-reload && sudo systemctl start ${SERVICE} \
        && sudo systemctl enable --now ${SERVICE}.timer"
}

lan_proxy() {
    install_on "${HOME_LAB}" "${NGINX_INCLUDE}" 644 < "${LAN_FILES}/server.conf"
    sed "s/{{LAN_ADDRESS}}/${LAN_ADDRESS}/" "${LAN_FILES}/tools.conf" | install_on "${HOME_LAB}" "${NGINX_SITE}" 644
    ssh "${HOME_LAB}" "sudo nginx -t -q && sudo systemctl reload nginx"
    ssh "${HOME_LAB}" "sudo ufw allow in on ${LAN_INTERFACE} to ${LAN_ADDRESS} port 80,443 proto tcp \
        comment 'Retro Speedlab owner tools on the LAN'"
}

placeholder
certificate_pull
lan_proxy
echo "Point these names at ${LAN_ADDRESS} in the LAN's DNS: ${DOMAINS[*]}"
