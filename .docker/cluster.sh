#!/usr/bin/env bash
set -e
SCRIPT_DIR=$(dirname $BASH_SOURCE)
docker-compose -p cluster --env-file $SCRIPT_DIR/shared.env -f $SCRIPT_DIR/gencert.yml -f $SCRIPT_DIR/cluster.ci.yml "$@"