#!/usr/bin/env bash

# Bash script for prepping common-utils code for Docker containers

set -e

CURRENT_DIR=${PWD}
rm -rf dist
mkdir -p dist
pushd ../common-utils
rm -rf dist
mkdir -p dist
cp -R src ${CURRENT_DIR}/dist/
cp poetry.lock ${CURRENT_DIR}/dist/
cp pyproject.toml ${CURRENT_DIR}/dist/
cp README.md ${CURRENT_DIR}/dist/
popd