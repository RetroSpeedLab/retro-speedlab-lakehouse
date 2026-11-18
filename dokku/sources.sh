#!/usr/bin/env bash
set -euo pipefail

readonly APP=retro-speedlab-lakehouse
readonly BACKEND=(GITHUB_TOKEN TWITCH_CLIENT_ID TWITCH_CLIENT_SECRET TWITCH_CHANNEL DISCORD_BOT_TOKEN
    DISCORD_RECORDS_CHANNEL UMAMI_API_BASE UMAMI_USERNAME UMAMI_PASSWORD UMAMI_WEBSITE_ID WANDB_API_KEY WANDB_PROJECT
    BUGSINK_API_BASE BUGSINK_API_TOKEN STREAM_ELEMENTS_JWT)
readonly RUNNER=(LANGFUSE_BASE_URL LANGFUSE_PUBLIC_KEY LANGFUSE_SECRET_KEY)

source "$(dirname "${BASH_SOURCE[0]}")/settings.sh"

copy_settings dokku retro-speedlab-backend "${APP}" "${BACKEND[@]}"
copy_settings dokku-black-omen retro-speedlab-runner "${APP}" "${RUNNER[@]}"
