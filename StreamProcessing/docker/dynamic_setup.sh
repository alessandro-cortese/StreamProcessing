#!/bin/bash

NUM_WORKERS=$1
if [[ -z "$NUM_WORKERS" ]]; then
  echo "Usage: $0 <num_workers>"
  exit 1
fi

echo "Start SetUp with $NUM_WORKERS Flink TaskManager..."

docker compose build --no-cache

docker compose up -d --scale flink-taskmanager=$NUM_WORKERS

echo "SetUp with $NUM_WORKERS workers Done"