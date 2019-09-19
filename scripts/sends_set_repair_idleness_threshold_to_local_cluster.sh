#!/usr/bin/env bash
set -e
THRESHOLD=$1

cargo run set-repair-idleness-threshold --repair-idleness-threshold ${THRESHOLD}
