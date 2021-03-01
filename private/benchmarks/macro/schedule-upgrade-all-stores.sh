#!/usr/bin/env bash
# Copyright (c) 2002-2019 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.

set -eux

env=
s3_dest_datasets_url="s3://storage.benchmarking.neo4j.today/datasets/macro"

while (("$#")); do
  case "$1" in
  --env)
    env=$2
    shift 2
    ;;
  --new-neo4j-version)
    new_neo4j_version=$2
    shift 2
    ;;
  --old-neo4j-version)
    old_neo4j_version=$2
    shift 2
    ;;
  --s3-dest-datasets-url)
    s3_dest_datasets_url=$2
    shift 2
    ;;
  --) # end of argument parsing
    shift
    break
    ;;
  *)
    shift
    ;;
  esac
done

if [[ -z "$env" ]]; then
  echo "environment not set"
  exit 1
fi

if [[ -z "$new_neo4j_version" ]]; then
  echo "new neo4j version not set"
  exit 1
fi

if [[ -z "$old_neo4j_version" ]]; then
  echo "old neo4j version not set"
  exit 1
fi

#check if artifacts exist
if [[ ! -f "upgrade-all-stores.sh" ]]; then
  echo "missing upgrade-all-stores.sh"
  exit 1
fi

if [[ ! -f "target/macro.jar" ]]; then
  echo "missing target/macro.jar"
  exit 1
fi

id=$(uuidgen)
base_artifacts_uri="s3://storage.benchmarking.neo4j.today/artifacts/upgrader/$id"

# copy script and macro jar to s3
aws s3 cp upgrade-all-stores.sh "$base_artifacts_uri/upgrade-all-stores.sh"
aws s3 cp target/macro.jar "$base_artifacts_uri/target/macro.jar"

# shellcheck disable=SC2089
json_parameters='{ "--base-artifact-uri" : "'$base_artifacts_uri'","--new-neo4j-version" : "'$new_neo4j_version'","--old-neo4j-version" : "'$old_neo4j_version'","--s3-dest-datasets-url" : "'$s3_dest_datasets_url'"}'

job_name=$(echo "macro_workload_store_upgrade_${new_neo4j_version}_${old_neo4j_version}" | sed 's/\./_/g')


# get queue name from stack output
# shellcheck disable=SC2016
job_queue=$(aws cloudformation describe-stacks --stack-name "benchmarking-batch-infrastructure-$env" --query 'Stacks[].Outputs[?OutputKey==`MacroUpgradeQueue`][OutputValue]' --output text)

# submit upgrade job
aws batch submit-job \
  --job-name "$job_name" \
  --job-queue "$job_queue" \
  --job-definition "upgrade-r5_2xlarge-oracle-11-benchmarking-batch-infrastructure-$env" \
  --parameters "$json_parameters"
