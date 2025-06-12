#!/bin/bash
docker image load -i gc25cdocker.tar
docker run -p 8866:8866 --network stream_processing_network --name=gc25-challenger --volume ./data:/data micro-challenger:latest 0.0.0.0:8866 /data 