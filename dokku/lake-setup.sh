#!/usr/bin/env bash
set -euo pipefail

readonly HOST=dokku
readonly APP=retro-speedlab-lake
readonly DOMAIN=lake.retrospeedlab.com
readonly NETWORK=retro-speedlab-lake
readonly DATA=/var/lib/dokku/data/storage/retro-speedlab-lake

dokku() {
    ssh "${HOST}" "$@"
}

dokku apps:create "${APP}"
dokku git:set "${APP}" deploy-branch main
dokku storage:ensure-directory "${APP}"
dokku storage:mount "${APP}" "${DATA}:/var/lib/garage"
dokku config:set --no-restart "${APP}" GARAGE_RPC_SECRET="$(openssl rand -hex 32)" >/dev/null
dokku domains:set "${APP}" "${DOMAIN}"
dokku ports:set "${APP}" http:80:3900
dokku nginx:set "${APP}" client-max-body-size 64m
dokku resource:limit --memory 384m --cpu 0.5 "${APP}"
dokku resource:reserve --memory 128m "${APP}"
dokku network:create "${NETWORK}"
dokku network:set "${APP}" attach-post-deploy "${NETWORK}"
echo "Deploy with bash dokku/lake-deploy.sh, then: ssh ${HOST} letsencrypt:enable ${APP}; bash dokku/lake-keys.sh"
