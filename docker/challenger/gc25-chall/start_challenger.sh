#!/bin/bash

CONTAINER_NAME="gc25-challenger"
IMAGE_TARBALL="gc25cdocker.tar"
IMAGE_NAME="micro-challenger:latest"

docker stop "$CONTAINER_NAME" > /dev/null 2>&1
docker rm "$CONTAINER_NAME" > /dev/null 2>&1

docker image load -i "$IMAGE_TARBALL"

echo "--- Running Challenger container ---"
docker run \
  -p 8866:8866 \
  --network docker_stream_processing_network \
  --name="$CONTAINER_NAME" \
  --volume ./gc25-chall-data/data:/data \
  "$IMAGE_NAME" 0.0.0.0:8866 /data
