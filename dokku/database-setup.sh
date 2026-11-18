#!/usr/bin/env bash
set -euo pipefail

readonly HOST=dokku-black-omen
readonly COCKPIT=retro-speedlab-cockpit
readonly KEY=database

source "$(dirname "${BASH_SOURCE[0]}")/garage.sh"

garage key info "${KEY}" >/dev/null 2>&1 || garage key create "${KEY}" >/dev/null
garage bucket allow --read "${BUCKET}" --key "${KEY}"
store_key "${KEY}" "${HOST}" "${COCKPIT}"
echo "Then: bash dokku/lan-setup.sh"
