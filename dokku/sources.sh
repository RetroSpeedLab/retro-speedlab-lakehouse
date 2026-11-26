#!/usr/bin/env bash
set -euo pipefail

readonly APP=retro-speedlab-lakehouse
readonly BACKEND=(TWITCH_CLIENT_ID TWITCH_CHANNEL DISCORD_RECORDS_CHANNEL UMAMI_API_BASE UMAMI_USERNAME
    UMAMI_WEBSITE_ID BUGSINK_API_BASE YOUTUBE_CLIENT_ID)

source "$(dirname "${BASH_SOURCE[0]}")/settings.sh"

copy_settings dokku retro-speedlab-backend "${APP}" "${BACKEND[@]}"
