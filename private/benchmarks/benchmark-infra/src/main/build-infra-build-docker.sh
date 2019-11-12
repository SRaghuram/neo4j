#!/usr/bin/env bash
#login
# shellcheck disable=SC2091
$(aws ecr get-login --no-include-email --region eu-west-1)
#build
docker build infra-build -t 065531048259.dkr.ecr.eu-west-1.amazonaws.com/benchmarks-worker:infra-builder
#Push
docker push 065531048259.dkr.ecr.eu-west-1.amazonaws.com/benchmarks-worker:infra-builder
