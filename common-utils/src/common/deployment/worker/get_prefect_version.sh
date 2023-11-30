#!/usr/bin/env bash

# Bash script for building docker image from path and context

set -euo pipefail

DEPENDENCY_GROUP=${1}

poetry show prefect --only $DEPENDENCY_GROUP | awk '/version/ { print $3 }'