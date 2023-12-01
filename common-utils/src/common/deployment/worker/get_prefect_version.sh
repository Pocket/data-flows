#!/usr/bin/env bash

# Bash script for getting prefect version for environment

set -euo pipefail

DEPENDENCY_GROUP=${1}

poetry show prefect --only $DEPENDENCY_GROUP | awk '/version/ { print $3 }'