#!/bin/bash

docker compose build --no-cache

docker compose up -d --scale flink-taskmanager=4

cd challenger/gc25-chall

docker image load -i gc25cdocker.tar
docker run -d -p 8866:8866 \
  --network stream_processing_network \
  --name=gc25-challenger \
  --volume ./gc25-chall-data/data:/data \
  micro-challenger:latest 0.0.0.0:8866 /data
