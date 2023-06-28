#!/usr/bin/env bash

# Bash script for running unit test suite once per deployment type

set -e

for env in "dev" "staging" "main"
do
    export DF_CONFIG_DEPLOYMENT_TYPE=$env
    echo "Running unit tests for deployment type: $env..."
    poetry run python -m pytest -s -vvv --cov=src --cov-report term-missing --cov-fail-under=100
done