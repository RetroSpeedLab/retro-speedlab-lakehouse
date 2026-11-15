#!/usr/bin/env bash
set -euo pipefail

readonly SERVER=black-omen
readonly APP=retro-speedlab-lakehouse
readonly BACKEND=(GITHUB_TOKEN TWITCH_CLIENT_ID TWITCH_CLIENT_SECRET TWITCH_CHANNEL DISCORD_BOT_TOKEN
    DISCORD_RECORDS_CHANNEL UMAMI_API_BASE UMAMI_USERNAME UMAMI_PASSWORD UMAMI_WEBSITE_ID WANDB_API_KEY WANDB_PROJECT
    BUGSINK_API_BASE BUGSINK_API_TOKEN)
readonly RUNNER=(LANGFUSE_BASE_URL LANGFUSE_PUBLIC_KEY LANGFUSE_SECRET_KEY)

copy_settings() {
    local host="$1" app="$2" name
    shift 2
    for name in "$@"; do
        ssh "${host}" config:get "${app}" "${name}" \
            | ssh "${SERVER}" "read -r value && test -n \"\${value}\" \
                && dokku config:set --no-restart ${APP} ${name}=\"\${value}\" >/dev/null" \
            || { echo "${app} has no ${name}" >&2; exit 1; }
        echo "Copied ${name} from ${app}"
    done
}

copy_settings dokku retro-speedlab-backend "${BACKEND[@]}"
copy_settings dokku-black-omen retro-speedlab-runner "${RUNNER[@]}"
