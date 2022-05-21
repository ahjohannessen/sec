#!/usr/bin/env bash
set -ea
SCRIPT_DIR=$(dirname $BASH_SOURCE)

EVENSTORE_CLI_IMAGE="${EVENSTORE_CLI_IMAGE:-ghcr.io/eventstore/es-gencert-cli/es-gencert-cli:1.1.0}"
EVENTSTORE_DB_IMAGE="${EVENTSTORE_DB_IMAGE:-ghcr.io/eventstore/eventstore:21.10.1-focal}"

. $SCRIPT_DIR/shared.env

docker-compose -p single -f $SCRIPT_DIR/gencert.yml -f $SCRIPT_DIR/single-node.ci.yml "$@"