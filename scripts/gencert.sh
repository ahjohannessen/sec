#!/usr/bin/env bash
set -e

SEC_ES_IMAGE_SOURCE="${SEC_ES_IMAGE_SOURCE:-quay.io/ahjohannessen/es-gencert-cli}"
SEC_ES_IMAGE_VERSION="${SEC_ES_IMAGE_VERSION:-1.0.1}"
SEC_ES_IMAGE="${SEC_ES_IMAGE_SOURCE}:${SEC_ES_IMAGE_VERSION}"
SEC_ES_CERTS_PATH="${SEC_ES_CERTS_PATH:-${PWD}/certs}"

mkdir -p $SEC_ES_CERTS_PATH

docker pull $SEC_ES_IMAGE

docker run --rm --volume $SEC_ES_CERTS_PATH:/tmp --user $(id -u):$(id -g) $SEC_ES_IMAGE \
  create-ca -out /tmp/ca

docker run --rm --volume $SEC_ES_CERTS_PATH:/tmp --user $(id -u):$(id -g) $SEC_ES_IMAGE \
  create-node -ca-certificate /tmp/ca/ca.crt -ca-key /tmp/ca/ca.key -out /tmp/node \
  -ip-addresses 127.0.0.1 \
  -dns-names es.sec.local
