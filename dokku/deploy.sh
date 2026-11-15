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
readonly REPLAYED=(retro-speedlab-core retro-speedlab-engine retro-speedlab-games)
LAKEHOUSE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly LAKEHOUSE

on_server() {
    if [[ "$(hostname)" == "${SERVER}" ]]; then
        bash -c "$1"
    else
        ssh "${SERVER}" "$1"
    fi
}

for repository in retro-speedlab-lakehouse "${REPLAYED[@]}"; do
    if ! git -C "${LAKEHOUSE}/../${repository}" diff --quiet HEAD; then
        echo "${repository} has uncommitted changes: commit them, only HEAD ships" >&2
        exit 1
    fi
done
bundle="$(mktemp -d)"
wheelhouse=""
cleanup() {
    rm -rf "${bundle}"
    [[ -z "${wheelhouse}" ]] || on_server "kill ${wheelhouse}"
}
trap cleanup EXIT
git -C "${LAKEHOUSE}" archive HEAD | tar -x -C "${bundle}"
revisions="retro-speedlab-lakehouse=$(git -C "${LAKEHOUSE}" rev-parse HEAD)"
for repository in "${REPLAYED[@]}"; do
    mkdir -p "${bundle}/speedlab/${repository}"
    git -C "${LAKEHOUSE}/../${repository}" archive HEAD | tar -x -C "${bundle}/speedlab/${repository}"
    revisions="${revisions},${repository}=$(git -C "${LAKEHOUSE}/../${repository}" rev-parse HEAD)"
done
git -C "${bundle}" init --quiet --initial-branch=main
git -C "${bundle}" add --all
git -C "${bundle}" -c user.name=deploy -c user.email=deploy@retrospeedlab.com commit --quiet \
    --message "${revisions}"
wheelhouse="$(on_server "cd ${WHEELHOUSE} && { python3 -m http.server ${WHEELHOUSE_PORT} --bind 127.0.0.1 >/dev/null 2>&1 & echo \$!; }")"
for app in "${APPS[@]}"; do
    git -C "${bundle}" push --force "${HOST}:${app}" main
done
