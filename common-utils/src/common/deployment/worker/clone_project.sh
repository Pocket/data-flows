#!/usr/bin/env bash

# Bash script for cloning data-flows project

set -euo pipefail

PROJECT_NAME=${1}
GIT_REPO=${2}
BRANCH=${3}
ROOT_DIR=${4:-opt}

mkdir -p /${ROOT_DIR}/prefect
cd /${ROOT_DIR}/prefect
git init
git remote add -f origin ${GIT_REPO}
git config core.sparseCheckout true
echo "${PROJECT_NAME}" >> .git/info/sparse-checkout
git pull origin ${BRANCH}
