#!/usr/bin/env bash
set -e

SEC_GENCERT_IMAGE_SOURCE="${SEC_GENCERT_IMAGE_SOURCE:-quay.io/ahjohannessen/es-gencert-cli}"
SEC_GENCERT_IMAGE_VERSION="${SEC_GENCERT_IMAGE_VERSION:-1.0.1}"
SEC_GENCERT_IMAGE="${SEC_GENCERT_IMAGE_SOURCE}:${SEC_GENCERT_IMAGE_VERSION}"
SEC_GENCERT_CERTS_PATH="${SEC_GENCERT_CERTS_PATH:-${PWD}/certs}"
SEC_GENCERT_IP_ADDRESSES="${SEC_GENCERT_IP_ADDRESSES:-127.0.0.1,172.17.0.2}"
SEC_GENCERT_DNS_NAMES="${SEC_GENCERT_DNS_NAMES:-es.sec.local}"

mkdir -p $SEC_GENCERT_CERTS_PATH

docker pull $SEC_GENCERT_IMAGE

docker run --rm --volume $SEC_GENCERT_CERTS_PATH:/tmp --user $(id -u):$(id -g) $SEC_GENCERT_IMAGE \
  create-ca -out /tmp/ca

echo "Generating node certificate for '$SEC_GENCERT_IP_ADDRESSES' and '$SEC_GENCERT_DNS_NAMES'"

docker run --rm --volume $SEC_GENCERT_CERTS_PATH:/tmp --user $(id -u):$(id -g) $SEC_GENCERT_IMAGE \
  create-node -ca-certificate /tmp/ca/ca.crt -ca-key /tmp/ca/ca.key -out /tmp/node \
  -ip-addresses $SEC_GENCERT_IP_ADDRESSES \
  -dns-names $SEC_GENCERT_DNS_NAMES

chmod -R 777 $SEC_GENCERT_CERTS_PATH