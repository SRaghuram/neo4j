#!/usr/bin/env bash
#login
# shellcheck disable=SC2091
$(aws ecr get-login --no-include-email --region eu-west-1)
#build
docker build -f profilers/Dockerfile .  -t 065531048259.dkr.ecr.eu-west-1.amazonaws.com/benchmarks-worker:profile
docker build  -f oraclejdk-11/Dockerfile . -t 065531048259.dkr.ecr.eu-west-1.amazonaws.com/benchmarks-worker:oracle_11
docker build  -f oraclejdk-8/Dockerfile . -t 065531048259.dkr.ecr.eu-west-1.amazonaws.com/benchmarks-worker:oracle_8
#Push
docker push 065531048259.dkr.ecr.eu-west-1.amazonaws.com/benchmarks-worker:profile
docker push 065531048259.dkr.ecr.eu-west-1.amazonaws.com/benchmarks-worker:oracle_11
docker push 065531048259.dkr.ecr.eu-west-1.amazonaws.com/benchmarks-worker:oracle_8