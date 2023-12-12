#!/usr/bin/env bash

# Bash script for pushing image to ECR

set -euo pipefail

IMAGE_NAME=${1}
AWS_ACCOUNT_ID=${2}
AWS_REGION=${3}
IMAGE_TAG=${4}

AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text --no-cli-pager)
DOCKER_BASE_URL=${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
ECR_IMAGE_NAME=${DOCKER_BASE_URL}/data-flows-prefect-v2-envs:${IMAGE_NAME}-${IMAGE_TAG}

aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${DOCKER_BASE_URL}
docker tag ${IMAGE_NAME}:latest ${ECR_IMAGE_NAME}
docker push ${ECR_IMAGE_NAME}
echo $ECR_IMAGE_NAME


