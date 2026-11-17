#!/usr/bin/env bash
set -euo pipefail

readonly MAIN_ADDRESS="$1"
readonly CERTS=/etc/retro-speedlab/lan-certs
readonly VALID_SECONDS=86400

download="$(mktemp -d)"
trap 'rm -rf "${download}"' EXIT
ssh -i "${CERTS}/id_ed25519" -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=yes \
    -o UserKnownHostsFile="${CERTS}/known_hosts" "lan-certs@${MAIN_ADDRESS}" \
    | tar -x --no-same-owner -C "${download}" server.crt server.key
if cmp -s "${download}/server.crt" "${CERTS}/server.crt"; then
    echo "The LAN certificate is current, $(openssl x509 -enddate -noout -in "${CERTS}/server.crt")"
    exit 0
fi
openssl x509 -checkend "${VALID_SECONDS}" -noout -in "${download}/server.crt"
[[ "$(openssl x509 -pubkey -noout -in "${download}/server.crt")" == "$(openssl pkey -pubout -in "${download}/server.key")" ]] \
    || { echo "The downloaded certificate and key do not match" >&2; exit 1; }
install -m 600 "${download}/server.key" "${download}/server.crt" "${CERTS}/"
nginx -t -q
systemctl reload nginx
echo "Installed the LAN certificate, $(openssl x509 -enddate -noout -in "${CERTS}/server.crt")"
