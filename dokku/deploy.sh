#!/usr/bin/env bash
set -euo pipefail

if [[ $# -eq 0 ]]; then
    echo "Name the apps to deploy: retro-speedlab-lakehouse, retro-speedlab-thrift or both" >&2
    exit 1
fi
readonly APPS=("$@")
readonly HOST=dokku-black-omen
readonly SERVER=black-omen
readonly WHEELHOUSE=/mnt/fastdata/wheelhouse
readonly WHEELHOUSE_PORT=8765
LAKEHOUSE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly LAKEHOUSE

on_server() {
    if [[ "$(hostname)" == "${SERVER}" ]]; then
        bash -c "$1"
    else
        ssh "${SERVER}" "$1"
    fi
}

if ! git -C "${LAKEHOUSE}" diff --quiet HEAD; then
    echo "retro-speedlab-lakehouse has uncommitted changes: commit them, only HEAD ships" >&2
    exit 1
fi
bundle="$(mktemp -d)"
wheelhouse=""
cleanup() {
    rm -rf "${bundle}"
    [[ -z "${wheelhouse}" ]] || on_server "kill ${wheelhouse}"
}
trap cleanup EXIT
git -C "${LAKEHOUSE}" archive HEAD | tar -x -C "${bundle}"
git -C "${bundle}" init --quiet --initial-branch=main
git -C "${bundle}" add --all
git -C "${bundle}" -c user.name=deploy -c user.email=deploy@retrospeedlab.com commit --quiet \
    --message "retro-speedlab-lakehouse=$(git -C "${LAKEHOUSE}" rev-parse HEAD)"
wheelhouse="$(on_server "cd ${WHEELHOUSE} && { python3 -m http.server ${WHEELHOUSE_PORT} --bind 127.0.0.1 >/dev/null 2>&1 & echo \$!; }")"
for app in "${APPS[@]}"; do
    git -C "${bundle}" push --force "${HOST}:${app}" main
done
