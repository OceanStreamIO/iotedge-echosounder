#!/bin/bash

# Variables
ACR_NAME="oceanstreamdevcr.azurecr.io"
REPO_NAME="echosounder"
TAG="latestarm"

FULL_IMAGE_NAME="$ACR_NAME/$REPO_NAME:$TAG"

echo "Pulling base image $FULL_IMAGE_NAME..."
docker pull $FULL_IMAGE_NAME

echo "Building Docker image $FULL_IMAGE_NAME..."
docker build -t $FULL_IMAGE_NAME .

echo "Logging into Azure Container Registry..."
docker login $ACR_NAME || { echo "Failed to log in to ACR"; exit 1; }

echo "Pushing Docker image $FULL_IMAGE_NAME to ACR..."
docker push $FULL_IMAGE_NAME || { echo "Failed to push the image to ACR"; exit 1; }

echo "Docker image $FULL_IMAGE_NAME built and pushed successfully!"
