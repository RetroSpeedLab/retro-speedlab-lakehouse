#!/usr/bin/env bash
set -euo pipefail

readonly MAIN=jumphost
readonly MAIN_ADDRESS=178.104.84.157
readonly SERVER=black-omen
readonly TUNNEL=database-tunnel
readonly SERVICE=retro-speedlab-database-tunnel
readonly LISTEN=172.17.0.1:18088
readonly KEYS=/etc/retro-speedlab/database-tunnel
readonly TUNNEL_HOME=/var/lib/database-tunnel
TUNNEL_FILES="$(cd "$(dirname "${BASH_SOURCE[0]}")/../database/tunnel" && pwd)"
readonly TUNNEL_FILES

ssh "${SERVER}" "sudo install -d -m 700 ${KEYS} \
    && { sudo test -f ${KEYS}/id_ed25519 || sudo ssh-keygen -q -t ed25519 -N '' -C ${TUNNEL}@${SERVER} -f ${KEYS}/id_ed25519; }"
public_key="$(ssh "${SERVER}" "sudo cat ${KEYS}/id_ed25519.pub")"
host_key="$(ssh "${MAIN}" "cut -d' ' -f1,2 /etc/ssh/ssh_host_ed25519_key.pub")"

ssh "${MAIN}" "id ${TUNNEL} >/dev/null 2>&1 \
    || sudo useradd --system --create-home --home-dir ${TUNNEL_HOME} --shell /usr/sbin/nologin ${TUNNEL}"
ssh "${MAIN}" "sudo install -d -o root -g root -m 755 ${TUNNEL_HOME}/.ssh \
    && echo 'restrict,port-forwarding,permitlisten=\"${LISTEN}\" ${public_key}' \
    | sudo tee ${TUNNEL_HOME}/.ssh/authorized_keys >/dev/null"
scp -q "${TUNNEL_FILES}/sshd.conf" "${MAIN}:/tmp/${TUNNEL}.conf"
ssh "${MAIN}" "sudo install -m 644 /tmp/${TUNNEL}.conf /etc/ssh/sshd_config.d/60-${TUNNEL}.conf \
    && rm /tmp/${TUNNEL}.conf && sudo sshd -t && sudo systemctl reload ssh"
ssh "${MAIN}" "sudo ufw allow in on docker0 from 172.17.0.0/16 to ${LISTEN%:*} port ${LISTEN#*:} proto tcp \
    comment 'Retro Speedlab database tunnel'"

ssh "${SERVER}" "echo '${MAIN_ADDRESS} ${host_key}' | sudo tee ${KEYS}/known_hosts >/dev/null"
scp -q "${TUNNEL_FILES}/${SERVICE}.service" "${SERVER}:/tmp/${SERVICE}.service"
ssh "${SERVER}" "sudo install -m 644 /tmp/${SERVICE}.service /etc/systemd/system/${SERVICE}.service \
    && rm /tmp/${SERVICE}.service && sudo systemctl daemon-reload && sudo systemctl enable --now ${SERVICE}"
