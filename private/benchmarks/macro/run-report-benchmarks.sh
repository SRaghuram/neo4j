#!/usr/bin/env bash
#
# Copyright (c) 2002-2019 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.
#


set -e
set -u

if [ $# -lt 31 ] ; then
    echo "Expected at least 31 arguments, but got $#"
    echo "usage: ./run-report-benchmarks.sh workload db warmup_count measurement_count db_edition jvm neo4j_config work_dir profilers forks results time_unit results_store_uri results_store_user results_store_password neo4j_commit neo4j_version neo4j_branch neo4j_branch_owner tool_commit tool_branch_owner tool_branch teamcity_build parent_teamcity_build jvm_args recreate_schema triggered_by error_policy"
    exit -1
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

# here we are checking for optional AWS endpoint URL, 
# this is required for end to end testing, where we mock s3
AWS_EXTRAS=
if [[ $# -eq 32 ]]; then
	AWS_EXTRAS="--endpoint-url=${32}"
fi

macro_benchmark_dir=$(pwd)

jar_path="${macro_benchmark_dir}/target/macro.jar"

uuid=$(uuidgen)
profiler_recording_output_dir="${macro_benchmark_dir}"/"${uuid}"
mkdir "${profiler_recording_output_dir}"

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

function runExport {
    #shellcheck disable=SC2068
    java -jar "${jar_path}" run-workload  \
            --workload "${workload}" \
            --db "${db}" \
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
            --skip-flamegraphs \
            --profiler-recordings-dir "${profiler_recording_output_dir}" \
            --triggered-by "${triggered_by}" \
            $@
}

if [ "${recreate_schema}" = "true" ]
then
    runExport "--recreate-schema" && echo "Will recreate the schema"
else
    runExport
fi

# --- create archive of profiler recording artifacts---
profiler_recording_dir_name=$(basename "${profiler_recording_output_dir}")
archive="${profiler_recording_dir_name}".tar.gz
tar czvf "${archive}" "${profiler_recording_dir_name}"

# --- upload archive of profiler recording artifacts to S3 ---
# shellcheck disable=SC2086
aws ${AWS_EXTRAS:+"$AWS_EXTRAS"} --region eu-north-1 s3 cp "${archive}" s3://benchmarking.neo4j.com/recordings/"${archive}"
# --- upload profiler recording artifacts to S3 ---
# shellcheck disable=SC2086
aws ${AWS_EXTRAS:+"$AWS_EXTRAS"} --region eu-north-1 s3 sync "${profiler_recording_output_dir}" s3://benchmarking.neo4j.com/recordings/"${profiler_recording_dir_name}"

# --- enrich results file with profiler recording information (locations in S3) ---
java -cp "${jar_path}" com.neo4j.bench.client.Main add-profiles \
    --dir "${profiler_recording_output_dir}"  \
    --s3-bucket benchmarking.neo4j.com/recordings/"${profiler_recording_dir_name}" \
    --archive benchmarking.neo4j.com/recordings/"${archive}"  \
    --test_run_report "${results_path}" \
    --ignore_unrecognized_files

java -cp "${jar_path}" com.neo4j.bench.client.Main report \
            --results_store_uri "${results_store_uri}"  \
            --results_store_user "${results_store_user}"  \
            --results_store_pass "${results_store_password}" \
            --test_run_results "${results_path}" \
            --error-policy "${error_policy}"
