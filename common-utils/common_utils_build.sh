#!/usr/bin/env bash

# Bash script for installing common-utils in Prefect package

set -e

CURRENT_DIR=${PWD}
mkdir -p dist
pushd ../common-utils
poetry build -f wheel
cp dist/*.whl ${CURRENT_DIR}/dist/
popd