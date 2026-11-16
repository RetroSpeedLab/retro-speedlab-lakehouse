#!/usr/bin/env bash
set -euo pipefail

readonly MAIN=jumphost
MAIN_ADDRESS="$(ssh -G "${MAIN}" | awk '/^hostname /{print $2}')"
readonly MAIN_ADDRESS
readonly SERVER=black-omen
readonly TUNNEL=database-tunnel
readonly SERVICE=retro-speedlab-database-tunnel
readonly WATCHDOG=${SERVICE}-watchdog
readonly BRIDGE=172.17.0.1
readonly PORTS=(18088 18089 18090 18091)
readonly KEYS=/etc/retro-speedlab/database-tunnel
readonly TUNNEL_HOME=/var/lib/database-tunnel
TUNNEL_FILES="$(cd "$(dirname "${BASH_SOURCE[0]}")/../database/tunnel" && pwd)"
readonly TUNNEL_FILES

permit_listen="$(printf "permitlisten=\"${BRIDGE}:%s\"," "${PORTS[@]}")"

ssh "${SERVER}" "sudo install -d -m 700 ${KEYS} \
    && { sudo test -f ${KEYS}/id_ed25519 || sudo ssh-keygen -q -t ed25519 -N '' -C ${TUNNEL}@${SERVER} -f ${KEYS}/id_ed25519; }"
public_key="$(ssh "${SERVER}" "sudo cat ${KEYS}/id_ed25519.pub")"
host_key="$(ssh "${MAIN}" "cut -d' ' -f1,2 /etc/ssh/ssh_host_ed25519_key.pub")"

ssh "${MAIN}" "id ${TUNNEL} >/dev/null 2>&1 \
    || sudo useradd --system --create-home --home-dir ${TUNNEL_HOME} --shell /usr/sbin/nologin ${TUNNEL}"
ssh "${MAIN}" "sudo install -d -o root -g root -m 755 ${TUNNEL_HOME}/.ssh \
    && echo 'restrict,port-forwarding,${permit_listen%,} ${public_key}' \
    | sudo tee ${TUNNEL_HOME}/.ssh/authorized_keys >/dev/null"
scp -q "${TUNNEL_FILES}/sshd.conf" "${MAIN}:/tmp/${TUNNEL}.conf"
ssh "${MAIN}" "sudo install -m 644 /tmp/${TUNNEL}.conf /etc/ssh/sshd_config.d/60-${TUNNEL}.conf \
    && rm /tmp/${TUNNEL}.conf && sudo sshd -t && sudo systemctl reload ssh"
for port in "${PORTS[@]}"; do
    ssh "${MAIN}" "sudo ufw allow in on docker0 from 172.17.0.0/16 to ${BRIDGE} port ${port} proto tcp \
        comment 'Retro Speedlab database tunnel'"
done

ssh "${SERVER}" "echo '${MAIN_ADDRESS} ${host_key}' | sudo tee ${KEYS}/known_hosts >/dev/null"
sed "s/{{MAIN_ADDRESS}}/${MAIN_ADDRESS}/" "${TUNNEL_FILES}/${SERVICE}.service" | ssh "${SERVER}" "cat > /tmp/${SERVICE}.service"
ssh "${SERVER}" "sudo install -m 644 /tmp/${SERVICE}.service /etc/systemd/system/${SERVICE}.service \
    && rm /tmp/${SERVICE}.service && sudo systemctl daemon-reload \
    && sudo systemctl enable ${SERVICE} && sudo systemctl restart ${SERVICE}"
scp -q "${TUNNEL_FILES}/${WATCHDOG}.service" "${TUNNEL_FILES}/${WATCHDOG}.timer" "${SERVER}:/tmp/"
ssh "${SERVER}" "sudo install -m 644 /tmp/${WATCHDOG}.service /tmp/${WATCHDOG}.timer /etc/systemd/system/ \
    && rm /tmp/${WATCHDOG}.service /tmp/${WATCHDOG}.timer && sudo systemctl daemon-reload \
    && sudo systemctl enable --now ${WATCHDOG}.timer"
