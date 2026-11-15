#!/usr/bin/env bash
set -euo pipefail

readonly HOST=dokku-black-omen
readonly SERVER=black-omen
readonly APP=retro-speedlab-kafka
readonly IMAGE=apache/kafka:4.3.2@sha256:852a0d49fc4e656b022e7f23785411802c9a088c78a7c5e39f33969de9c227a7
readonly DATA=/mnt/fastdata/lakehouse/kafka
readonly NETWORK=retro-speedlab-lakehouse
readonly ADDRESS=retro-speedlab-kafka.web

dokku() {
    ssh "${HOST}" "$@"
}

ssh "${SERVER}" "mkdir -p ${DATA}"
dokku apps:create "${APP}"
dokku storage:mount "${APP}" "${DATA}:/var/lib/kafka/data"
dokku config:set --no-restart "${APP}" \
    CLUSTER_ID="$(python3 -c 'import base64, uuid; print(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("="))')" \
    KAFKA_NODE_ID=1 \
    KAFKA_PROCESS_ROLES=broker,controller \
    KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://${ADDRESS}:9092" \
    KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
    KAFKA_LOG_DIRS=/var/lib/kafka/data \
    KAFKA_LOG_RETENTION_HOURS=168 \
    KAFKA_LOG_RETENTION_BYTES=1073741824 >/dev/null
dokku config:set --no-restart --encoded "${APP}" KAFKA_HEAP_OPTS="$(printf '%s' '-Xms768m -Xmx768m' | base64 -w0)" >/dev/null
dokku proxy:disable "${APP}"
dokku resource:limit --memory 1280m --cpu 1 "${APP}"
dokku docker-options:add "${APP}" deploy "--cpu-shares 512"
dokku network:create "${NETWORK}"
dokku network:set "${APP}" attach-post-deploy "${NETWORK}"
dokku git:from-image "${APP}" "${IMAGE}"
