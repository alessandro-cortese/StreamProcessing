#!/bin/bash

CONTAINER_NAME="gc25-challenger"
IMAGE_TARBALL="gc25cdocker.tar"
IMAGE_NAME="micro-challenger:latest"

echo "--- Stopping and removing any existing Challenger container ---"
docker stop "$CONTAINER_NAME" > /dev/null 2>&1
docker rm "$CONTAINER_NAME" > /dev/null 2>&1

echo "--- Loading Challenger Docker image from $IMAGE_TARBALL ---"
docker image load -i "$IMAGE_TARBALL"

echo "--- Running Challenger container ---"
docker run -d \
  -p 8866:8866 \
  --network docker_stream_processing_network \
  --name="$CONTAINER_NAME" \
  --volume ./gc25-chall-data/data:/data \
  "$IMAGE_NAME" 0.0.0.0:8866 /data


echo "--- Following Challenger logs ---"
docker logs -f "$CONTAINER_NAME"

echo "--- Challenger logs stopped or container terminated ---"