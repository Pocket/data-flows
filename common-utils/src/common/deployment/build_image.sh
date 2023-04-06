#!/usr/bin/env bash

# Bash script for building docker image from path and context

set -e

IMAGE_NAME=${1}
DOCKERFILE_PATH=${2}
PYTHON_VERSION=${3}
DOCKER_BUILD_CONTEXT=${4}

docker build -t ${IMAGE_NAME} -f ${DOCKERFILE_PATH} --platform linux/amd64 --build-arg PYTHON_VERSION=${PYTHON_VERSION} ${DOCKER_BUILD_CONTEXT}