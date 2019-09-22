#!/usr/bin/env bash
#login
# shellcheck disable=SC2091
$(aws ecr get-login --no-include-email --region eu-north-1)
#build
docker build  -f src/main/docker/OracleJDK11.dockerfile src/main/docker -t 535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:oracle_11
docker build  -f src/main/docker/OracleJDK8.dockerfile src/main/docker -t 535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:oracle_8
#Push
docker push 535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:oracle_11
docker push 535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:oracle_8
