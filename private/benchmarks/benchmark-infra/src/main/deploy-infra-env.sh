#!/usr/bin/env bash
# this script is a helpful wrapper which deploys private copy of benchmarking infrastructure
set -eux

env=

while (("$#")); do
  case "$1" in
  --env)
    env=$2
    shift 2
    ;;
  --) # end of argument parsing
    shift
    break
    ;;
  esac
done

if [[ -z "$env" ]]; then
  echo -e "no environment, please call this script with args, like this: \n\n $0 --env [environment name]"
  exit 1
fi

neo4jCommit=$(git rev-parse HEAD)

echo "# building worker docker images and pushing them to ECR registry #"
# shellcheck disable=SC1091
source build-worker-docker.sh --push --tag-suffix "$env"

oracle11_worker_image="535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:oracle_11_$env"
oracle8_worker_image="535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:oracle_8_$env"

echo "# building AMI image for AWS batch #"
# make sure old manifest doesn't exist
if [[ -f "manifest.json" ]]; then
  rm manifest.json
fi
packer build -var stage="$env" ami/benchmark-run-batch-worker/template.json
ami_id=$(jq -r '.builds[0].artifact_id | split(":")[1]' manifest.json)

echo "# cloudformation deploy #"
aws --region eu-north-1 cloudformation deploy --stack-name "benchmarking-$env" \
                                              --template-file stack/aws-batch-formation.json \
                                              --parameter-overrides \
                                              AMIID="$ami_id" \
                                              NEO4JCOMMIT="$neo4jCommit" \
                                              STAGE="$env" \
                                              BUILDID="$env" \
                                              ORACLE11WORKERIMAGE="$oracle11_worker_image" \
                                              ORACLE8WORKERIMAGE="$oracle8_worker_image"
