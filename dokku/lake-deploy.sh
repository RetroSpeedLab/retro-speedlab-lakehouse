#!/usr/bin/env bash
set -euo pipefail

exec bash "$(dirname "${BASH_SOURCE[0]}")/push-dir.sh" dokku retro-speedlab-lake lake
