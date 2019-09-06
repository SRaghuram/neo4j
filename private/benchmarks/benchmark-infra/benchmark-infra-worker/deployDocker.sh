#!/usr/bin/env bash
#login
# shellcheck disable=SC2091
$(aws ecr get-login --no-include-email --region eu-north-1)
#build
docker build -f profilers/Dockerfile .  -t 535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:profile
docker build  -f oraclejdk-11/Dockerfile . -t 535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:oracle_11
docker build  -f oraclejdk-8/Dockerfile . -t 535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:oracle_8
#Push
docker push 535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:profile
docker push 535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:oracle_11
docker push 535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:oracle_8