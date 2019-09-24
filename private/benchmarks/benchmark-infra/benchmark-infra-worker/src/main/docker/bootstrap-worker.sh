#!/usr/bin/env bash
# Copyright (c) 2002-2019 "Neo4j,"
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

# download bootstrap jar
aws --region eu-north-1 s3 cp "${workerArtifactUri}" /work/benchmark-worker.jar

cd /work
work_dir=$(pwd)/macro_work_dir/
mkdir "${work_dir}"

# shellcheck disable=SC2086
java ${JAVA_OPTS:+"$JAVA_OPTS"} -jar benchmark-worker.jar "${params[@]}" --workspace-dir "${work_dir}" --worker-artifact-uri "${workerArtifactUri}"
