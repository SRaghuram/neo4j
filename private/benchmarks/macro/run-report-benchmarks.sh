#!/usr/bin/env bash
#
# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.
#

set -eux

if [[ -z "$JAVA_HOME" ]]; then
  echo "JAVA_HOME not set, bye, bye"
  exit 1
fi

if [ $# -lt 8 ]; then
  echo "Expected at least 8 arguments, but got $#"
  echo "usage: ./run-report-benchmarks.sh db jvm neo4j_config work_dir results_store_uri results_store_user results_store_password error_policy"
  exit 1
fi

db="${1}"
jvm="${2}"
neo4j_config="${3}"
work_dir="${4}"
results_store_uri="${5}"
results_store_user="${6}"
results_store_password="${7}"
# FIXME: https://trello.com/c/Kjcg1DQ1/2352-error-policy-is-ignored
# add to command once bug fixed:
#  --error-policy "${error_policy}" \
# error_policy="${8}"

macro_benchmark_dir=$(pwd)

jar_path="${macro_benchmark_dir}/target/macro.jar"

# path to on-out-of-memory script
basedir=$(dirname "$(realpath "$0")")
out_of_memory_script="$basedir/on-out-of-memory.sh"
out_of_memory_base_dir="$basedir/out-of-memory"
# path to benchmark process out of memory output directory,
# WARNING: benchmark process will do heap dump outside of forks directories
out_of_memory_dir="$out_of_memory_base_dir/benchmark"
mkdir -p "$out_of_memory_dir"

#shellcheck disable=SC2068
${jvm} -Xmx1g -XX:OnOutOfMemoryError="$out_of_memory_script --jvm-pid %p --output-dir $out_of_memory_dir" \
  -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath="$out_of_memory_dir" \
  -jar "${jar_path}" run-workload \
  --db-dir "${db}" \
  --neo4j-config "${neo4j_config}" \
  --work-dir "${work_dir}" \
  --results-store-uri "${results_store_uri}" \
  --results-store-user "${results_store_user}" \
  --results-store-pass "${results_store_password}" \
  "${@:9}"
