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

if [ $# -lt 32 ]; then
  echo "Expected at least 31 arguments, but got $#"
  echo "usage: ./run-report-benchmarks.sh workload db warmup_count measurement_count db_edition jvm neo4j_config work_dir profilers forks results time_unit results_store_uri results_store_user results_store_password neo4j_commit neo4j_version neo4j_branch neo4j_branch_owner tool_commit tool_branch_owner tool_branch teamcity_build parent_teamcity_build jvm_args recreate_schema triggered_by error_policy deployment queries"
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
results_path="${11}"
time_unit="${12}"
results_store_uri="${13}"
results_store_user="${14}"
results_store_password="${15}"
neo4j_commit="${16}"
neo4j_version="${17}"
neo4j_branch="${18}"
neo4j_branch_owner="${19}"
tool_commit="${20}"
tool_branch_owner="${21}"
tool_branch="${22}"
teamcity_build="${23}"
parent_teamcity_build="${24}"
execution_mode="${25}"
jvm_args="${26}"
recreate_schema="${27}"
planner="${28}"
runtime="${29}"
triggered_by="${30}"
error_policy="${31}"
deployment="${32}"
queries="${33}"

# parse optional arguments
all_args=("$@")
optional_args=("${all_args[@]:33}")
aws_endpoint_url=
batch_job_id=

while ((${#optional_args[@]})); do
  arg=${optional_args[0]}
  optional_args=("${optional_args[@]:1}")
  case "$arg" in
  --aws-endpoint-url)
    aws_endpoint_url=${optional_args[0]}
    optional_args=("${optional_args[@]:1}")
    ;;
  --batch-job-id)
    batch_job_id=${optional_args[0]}
    optional_args=("${optional_args[@]:1}")
    ;;
  --)
    break
    ;;
  esac
done

macro_benchmark_dir=$(pwd)

jar_path="${macro_benchmark_dir}/target/macro.jar"

uuid=$(uuidgen)
profiler_recording_output_dir="${macro_benchmark_dir}"/"${uuid}"
mkdir "${profiler_recording_output_dir}"

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
echo "Path to where the results file will be written                 : ${results_path}"
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
echo "Macro benchmarks commit                                        : ${tool_commit}"
echo "Macro branch                                                   : ${tool_branch}"
echo "Macro branch owner                                             : ${tool_branch_owner}"
echo "Macro execute mode                                             : ${execution_mode}"
echo "JVM args                                                       : ${jvm_args}"
echo "Recreate Schema                                                : ${recreate_schema}"
echo "Cypher planner                                                 : ${planner}"
echo "Cypher runtime                                                 : ${runtime}"
echo "Path to the jar                                                : ${jar_path}"
echo "Profiler Recording directory                                   : ${profiler_recording_output_dir}"
echo "Triggered by                                                   : ${triggered_by}"
echo "Error policy                                                   : ${error_policy}"
echo "Neo4j Directory                                                : ${deployment}"
echo "Queries                                                        : ${queries}"
echo "Batch job id                                                   : ${batch_job_id}"

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
  --results "${results_path}" \
  --time-unit "${time_unit}" \
  --neo4j-commit "${neo4j_commit}" \
  --neo4j-version "${neo4j_version}" \
  --neo4j-branch "${neo4j_branch}" \
  --neo4j-branch-owner "${neo4j_branch_owner}" \
  --tool-commit "${tool_commit}" \
  --tool-branch-owner "${tool_branch_owner}" \
  --tool-branch "${tool_branch}" \
  --teamcity-build "${teamcity_build}" \
  --parent-teamcity-build "${parent_teamcity_build}" \
  --execution-mode "${execution_mode}" \
  --jvm-args "${jvm_args}" \
  --planner "${planner}" \
  --runtime "${runtime}" \
  --profiler-recordings-dir "${profiler_recording_output_dir}" \
  --triggered-by "${triggered_by}" \
  --neo4j-deployment "${deployment}" \
  --queries "${queries}" \
  --results-store-uri "${results_store_uri}" \
  --results-store-user "${results_store_user}" \
  --results-store-pass "${results_store_password}" \
  --s3-bucket "benchmarking.neo4j.com/recordings/" \
  --aws-region "eu-north-1" \
  ${aws_endpoint_url:+--aws-endpoint-url $aws_endpoint_url} \
  ${recreate_schema:+--recreate-schema} \
  ${batch_job_id:+--batch-job-id $batch_job_id}
