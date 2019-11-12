#!/usr/bin/env bash
# set -eux

# by default don't push docker images
push=0
# by default we don't want tag suffix, this is needed when you want to build custom image for testing
tag_suffix=

while (("$#")); do
  case "$1" in
  --push)
    push=1
    shift
    ;;
  --tag-suffix)
    tag_suffix=$2
    shift 2
    ;;
  --) # end of argument parsing
    shift
    break
    ;;
  esac
done

#build
docker build \
  --no-cache \
  --build-arg JVM_RELEASE=11.0.4_linux-x64_bin \
  --build-arg JAVA_HOME=/usr/lib/jvm/oracle-jdk-11 \
  -t 535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:oracle_11${tag_suffix:+"_$tag_suffix"}  \
  worker
docker build \
  --no-cache \
  --build-arg JVM_RELEASE=8u221-linux-x64 \
  --build-arg JAVA_HOME=/usr/lib/jvm/oracle-jdk-8 \
  -t 535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:oracle_8${tag_suffix:+"_$tag_suffix"} \
  worker
#Push
if [ $push == 1 ]; then
# shellcheck disable=SC2091
  $(aws ecr get-login --no-include-email --region eu-north-1)
  docker push 535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:oracle_11${tag_suffix:+"_$tag_suffix"}
  docker push 535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:oracle_8${tag_suffix:+"_$tag_suffix"}
else
  echo "images built, use --push to push them to registry"
fi
