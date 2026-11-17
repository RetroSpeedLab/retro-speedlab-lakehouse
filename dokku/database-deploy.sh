#!/usr/bin/env bash
set -euo pipefail

DOKKU_SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly DOKKU_SCRIPTS

bash "${DOKKU_SCRIPTS}/push-dir.sh" dokku-black-omen retro-speedlab-superset database/superset
