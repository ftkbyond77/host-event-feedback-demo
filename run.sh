#!/usr/bin/env bash

IMAGE_NAME="braze-mock-server"
CONTAINER_NAME="braze-mock-server"
NGROK_TOKEN="3CcHMKGRn7kVcefd2AbpILfOiRy_6J3qzamWXxQ7TCoQTgqVj"

# --- Step 1: Build only if image does not exist ---
if [[ -z "$(docker images -q $IMAGE_NAME 2>/dev/null)" ]]; then
    echo "Docker image not found. Building..."
    docker build -t $IMAGE_NAME .
else
    echo "Docker image exists. Skipping build."
fi

# --- Optional: stop/remove old container if already running ---
if [ "$(docker ps -aq -f name=$CONTAINER_NAME)" ]; then
    echo "Removing old container..."
    docker rm -f $CONTAINER_NAME
fi

# --- Step 2: Run container ---
docker run \
  --name $CONTAINER_NAME \
  -p 8000:8000 \
  -p 4040:4040 \
  -e NGROK_AUTHTOKEN=$NGROK_TOKEN \
  -v "$(pwd):/app" \
  $IMAGE_NAME