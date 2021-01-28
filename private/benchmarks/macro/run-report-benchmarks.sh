#!/usr/bin/env bash
#
# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.
#

set -eux

if [[ -z "$JAVA_HOME" ]]; then
  echo "JAVA_HOME not set, bye, bye"
  exit 1
fi

if [ $# -lt 29 ]; then
  echo "Expected at least 29 arguments, but got $#"
  echo "usage: ./run-report-benchmarks.sh workload db warmup_count measurement_count db_edition jvm neo4j_config work_dir profilers forks time_unit results_store_uri results_store_user results_store_password neo4j_commit neo4j_version neo4j_branch neo4j_branch_owner teamcity_build parent_teamcity_build jvm_args recreate_schema triggered_by error_policy deployment queries"
  exit 1
fi

workload="${1}"
db="${2}"
warmup_count="${3}"
measurement_count="${4}"
db_edition="${5}"
jvm="${6}"
neo4j_config="${7}"
work_dir="${8}"
profilers="${9}"
forks="${10}"
time_unit="${11}"
results_store_uri="${12}"
results_store_user="${13}"
results_store_password="${14}"
neo4j_commit="${15}"
neo4j_version="${16}"
neo4j_branch="${17}"
neo4j_branch_owner="${18}"
teamcity_build="${19}"
parent_teamcity_build="${20}"
execution_mode="${21}"
jvm_args="${22}"
recreate_schema="${23}"
planner="${24}"
runtime="${25}"
triggered_by="${26}"
error_policy="${27}"
deployment="${28}"
queries="${29}"

# parse optional arguments
all_args=("$@")
optional_args=("${all_args[@]:29}")
aws_endpoint_url=
test_run_id=
recordings_base_uri=

while ((${#optional_args[@]})); do
  arg=${optional_args[0]}
  optional_args=("${optional_args[@]:1}")
  case "$arg" in
  --aws-endpoint-url)
    aws_endpoint_url=${optional_args[0]}
    optional_args=("${optional_args[@]:1}")
    ;;
  --test-run-id)
    test_run_id=${optional_args[0]}
    optional_args=("${optional_args[@]:1}")
    ;;
   --recordings-base-uri)
    recordings_base_uri=${optional_args[0]}
    optional_args=("${optional_args[@]:1}")
    ;;
  --)
    break
    ;;
  esac
done

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

echo "JSON file containing definition of workload                    : ${workload}"
echo "Store directory                                                : ${db}"
echo "Neo4j edition (COMMUNITY or ENTERPRISE)                        : ${db_edition}"
echo "Path to JVM -- will also be used when launching fork processes : ${jvm}"
echo "Neo4j configuration file                                       : ${neo4j_config}"
echo "Work directory (intermediate results/logs/profiler recordings) : ${work_dir}"
echo "Comma separated list of profilers to run with                  : ${profilers}"
echo "Warmup execution count                                         : ${warmup_count}"
echo "Measurement execution count                                    : ${measurement_count}"
echo "Fork count                                                     : ${forks}"
echo "Time unit to report results in                                 : ${time_unit}"
echo "Benchmark dir                                                  : ${macro_benchmark_dir}"
echo "Results store uri                                              : ${results_store_uri}"
echo "Results store user                                             : ${results_store_user}"
echo "TeamCity Build ID                                              : ${teamcity_build}"
echo "TeamCity Parent Build ID                                       : ${parent_teamcity_build}"
echo "Neo4j version                                                  : ${neo4j_version}"
echo "Neo4j commit                                                   : ${neo4j_commit}"
echo "Neo4j branch                                                   : ${neo4j_branch}"
echo "Neo4j branch owner                                             : ${neo4j_branch_owner}"
echo "Macro execute mode                                             : ${execution_mode}"
echo "JVM args                                                       : ${jvm_args}"
echo "Recreate Schema                                                : ${recreate_schema}"
echo "Cypher planner                                                 : ${planner}"
echo "Cypher runtime                                                 : ${runtime}"
echo "Path to the jar                                                : ${jar_path}"
echo "Triggered by                                                   : ${triggered_by}"
echo "Error policy                                                   : ${error_policy}"
echo "Neo4j Directory                                                : ${deployment}"
echo "Queries                                                        : ${queries}"
echo "Test run id                                                    : ${test_run_id}"

if [ "$recreate_schema" != "true" ]; then
  recreate_schema=""
fi

#shellcheck disable=SC2068
${jvm} -Xmx1g -XX:OnOutOfMemoryError="$out_of_memory_script --jvm-pid %p --output-dir $out_of_memory_dir" \
  -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath="$out_of_memory_dir" \
  -jar "${jar_path}" run-workload \
  --workload "${workload}" \
  --db-dir "${db}" \
  --warmup-count "${warmup_count}" \
  --measurement-count "${measurement_count}" \
  --db-edition "${db_edition}" \
  --jvm "${jvm}" \
  --neo4j-config "${neo4j_config}" \
  --work-dir "${work_dir}" \
  --profilers "${profilers}" \
  --forks "${forks}" \
  --time-unit "${time_unit}" \
  --neo4j-commit "${neo4j_commit}" \
  --neo4j-version "${neo4j_version}" \
  --neo4j-branch "${neo4j_branch}" \
  --neo4j-branch-owner "${neo4j_branch_owner}" \
  --teamcity-build "${teamcity_build}" \
  --parent-teamcity-build "${parent_teamcity_build}" \
  --execution-mode "${execution_mode}" \
  --jvm-args "${jvm_args}" \
  --planner "${planner}" \
  --runtime "${runtime}" \
  --triggered-by "${triggered_by}" \
  --neo4j-deployment "${deployment}" \
  --queries "${queries}" \
  --results-store-uri "${results_store_uri}" \
  --results-store-user "${results_store_user}" \
  --results-store-pass "${results_store_password}" \
  ${recordings_base_uri:+--recordings-base-uri $recordings_base_uri} \
  --aws-region "eu-north-1" \
  ${aws_endpoint_url:+--aws-endpoint-url $aws_endpoint_url} \
  ${recreate_schema:+--recreate-schema} \
  ${test_run_id:+--test-run-id $test_run_id}