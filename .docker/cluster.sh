#!/usr/bin/env bash
set -ea
SCRIPT_DIR=$(dirname $BASH_SOURCE)

. $SCRIPT_DIR/images.env
. $SCRIPT_DIR/shared.env

docker compose -p cluster -f $SCRIPT_DIR/gencert.yml -f $SCRIPT_DIR/cluster.ci.yml "$@"