#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
    echo "Usage: bash dokku/push-dir.sh HOST APP DIRECTORY" >&2
    exit 1
fi
readonly HOST="$1" APP="$2" DIRECTORY="$3"
LAKEHOUSE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly LAKEHOUSE

bundle="$(mktemp -d)"
trap 'rm -rf "${bundle}"' EXIT
git -C "${LAKEHOUSE}" archive HEAD "${DIRECTORY}" | tar -x -C "${bundle}"
git -C "${bundle}/${DIRECTORY}" init --quiet --initial-branch=main
git -C "${bundle}/${DIRECTORY}" add --all
git -C "${bundle}/${DIRECTORY}" -c user.name=deploy -c user.email=deploy@retrospeedlab.com commit --quiet \
    --message "retro-speedlab-lakehouse=$(git -C "${LAKEHOUSE}" rev-parse HEAD)"
git -C "${bundle}/${DIRECTORY}" push --force "${HOST}:${APP}" main
