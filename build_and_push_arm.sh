#!/bin/bash

# Variables
ACR_NAME="oceanstreamdevcr.azurecr.io"
REPO_NAME="echosounder"
TAG="latestarm"
DOCKERFILE="Dockerfile.Linux.arm64"

# Full image name for easy reference
FULL_IMAGE_NAME="$ACR_NAME/$REPO_NAME:$TAG"

# Pull the latest base image
echo "Pulling base image $FULL_IMAGE_NAME..."
docker pull $FULL_IMAGE_NAME

# Build the new Docker image with the specified Dockerfile
echo "Building Docker image $FULL_IMAGE_NAME using $DOCKERFILE..."
docker build -t $FULL_IMAGE_NAME -f $DOCKERFILE .

# Log in to ACR if not already authenticated
echo "Logging into Azure Container Registry..."
docker login $ACR_NAME || { echo "Failed to log in to ACR"; exit 1; }

# Push the new image to ACR
echo "Pushing Docker image $FULL_IMAGE_NAME to ACR..."
docker push $FULL_IMAGE_NAME || { echo "Failed to push the image to ACR"; exit 1; }

echo "Docker image $FULL_IMAGE_NAME built and pushed successfully!"
