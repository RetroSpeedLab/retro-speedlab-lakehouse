#!/usr/bin/env bash
set -euo pipefail

readonly HOST=dokku
readonly APP=retro-speedlab-lake
LAKEHOUSE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly LAKEHOUSE

bundle="$(mktemp -d)"
trap 'rm -rf "${bundle}"' EXIT
git -C "${LAKEHOUSE}" archive HEAD lake | tar -x -C "${bundle}"
git -C "${bundle}/lake" init --quiet --initial-branch=main
git -C "${bundle}/lake" add --all
git -C "${bundle}/lake" -c user.name=deploy -c user.email=deploy@retrospeedlab.com commit --quiet \
    --message "retro-speedlab-lakehouse=$(git -C "${LAKEHOUSE}" rev-parse HEAD)"
git -C "${bundle}/lake" push --force "${HOST}:${APP}" main
