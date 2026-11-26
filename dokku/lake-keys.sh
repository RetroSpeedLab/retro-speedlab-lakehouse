#!/usr/bin/env bash
set -euo pipefail

readonly CAPACITY=10G
readonly WRITER=lakehouse
readonly READER=backend
readonly WRITER_APP=retro-speedlab-lakehouse
readonly READER_APP=retro-speedlab-backend

source "$(dirname "${BASH_SOURCE[0]}")/apps.sh"
source "$(dirname "${BASH_SOURCE[0]}")/garage.sh"

node="$(garage node id -q | cut -d@ -f1)"
garage layout assign -z home -c "${CAPACITY}" "${node}"
garage layout apply --version=1
garage bucket create "${BUCKET}"
garage key create "${WRITER}" >/dev/null
garage key create "${READER}" >/dev/null
garage bucket allow --read --write --owner "${BUCKET}" --key "${WRITER}"
garage bucket allow --read "${BUCKET}" --key "${READER}"
vault_key "${WRITER}" "${WRITER_APP}"
vault_key "${READER}" "${READER_APP}"
exit 1
