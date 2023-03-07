#!/usr/bin/env bash

# Bash script for pushing image to ECR

set -e

IMAGE_NAME=${1}
ACCOUNT_ID=${2}

DOCKER_BASE_URL=${ACCOUNT_ID}.dkr.ecr.${AWS_DEFAULT_REGION}.amazonaws.com
GIT_SHA="${CIRCLE_SHA1:=dev}"
IMAGE_TAG=${GIT_SHA:0:7}
ECR_IMAGE_NAME=${DOCKER_BASE_URL}/data-flows-prefect-envs:${IMAGE_NAME}-${IMAGE_TAG}

aws ecr get-login-password --region ${AWS_DEFAULT_REGION} | docker login --username AWS --password-stdin ${DOCKER_BASE_URL}
docker tag ${IMAGE_NAME}:latest ${ECR_IMAGE_NAME}
docker push ${ECR_IMAGE_NAME}
echo $ECR_IMAGE_NAME