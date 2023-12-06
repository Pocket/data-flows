#!/usr/bin/env bash

# Bash script for building docker image from path

set -euo pipefail

IMAGE_NAME=${1}
DOCKERFILE_PATH=${2}
DEPENDENCY_GROUP=${3}
PYTHON_VERSION=${4}
PREFECT_VERSION=${5}

docker build -t ${IMAGE_NAME} \
-f ${DOCKERFILE_PATH} \
--platform linux/amd64 \
--build-arg DEPENDENCY_GROUP=${DEPENDENCY_GROUP} \
--build-arg PYTHON_VERSION=${PYTHON_VERSION} \
--build-arg PREFECT_VERSION=${PREFECT_VERSION} .