#!/bin/bash

docker buildx build \
  --platform linux/amd64 \
  -t monkey-island-payment:v0.1 \
  -f Dockerfile \
  --output type=docker,dest=monkey-island-payment-amd64.tar ..
