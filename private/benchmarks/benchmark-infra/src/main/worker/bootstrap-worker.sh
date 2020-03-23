#!/usr/bin/env bash
# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.
set -ex

declare -a params
while (( "$#" )); do
  case "$1" in
    --worker-artifact-uri)
      workerArtifactUri=$2
      shift 2
      ;;
    --) # end argument parsing
      shift
      break
      ;;
    *) # preserve positional arguments
      params+=("$1")
      shift
      ;;
  esac
done

cd /work

worker_artifact=/work/benchmark-worker.jar
work_dir=/work/run/

# make sure we start clean with working directory
# and worker artifact
rm -rf "${worker_artifact}"
find ${work_dir:?} -mindepth 1 -delete

# download bootstrap jar
aws --region eu-north-1 s3 cp "${workerArtifactUri}" "${worker_artifact}"

# shellcheck disable=SC2086
java ${JAVA_OPTS:+"$JAVA_OPTS"} -cp "${worker_artifact}" com.neo4j.bench.infra.worker.Main "${params[@]}" --workspace-dir "${work_dir}" --batch-job-id "${AWS_BATCH_JOB_ID}"
