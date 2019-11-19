#!/usr/bin/env bash
set -eux
#login
# shellcheck disable=SC2091
$(aws ecr get-login --no-include-email --region eu-north-1)
#build
docker build  \
  --no-cache \
  --build-arg JVM_RELEASE=11.0.4_linux-x64_bin \
  --build-arg JAVA_HOME=/usr/lib/jvm/oracle-jdk-11 \
  -t 535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:oracle_11 \
  src/main/docker
docker build \
  --no-cache \
  --build-arg JVM_RELEASE=8u221-linux-x64 \
  --build-arg JAVA_HOME=/usr/lib/jvm/oracle-jdk-8 \
   -t 535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:oracle_8 \
   src/main/docker
#Push
docker push 535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:oracle_11
docker push 535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:oracle_8
