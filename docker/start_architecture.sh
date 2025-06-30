#!/bin/bash

PARTITIONS=${1:-2}  # default 2 if not passed as argument

export KAFKA_TOPIC_PARTITIONS=$PARTITIONS

docker compose build
docker compose up -d --scale kafka-consumer-app=$PARTITIONS
