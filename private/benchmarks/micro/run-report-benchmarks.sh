#!/bin/bash
#
# Copyright (c) 2002-2019 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.
#


set -e
set -u
set -x

if [ $# -lt 20 ] ; then
    echo "Expected at least 20 arguments, but got $#"
    echo "usage: ./run-report-benchmarks.sh neo4j_version neo4j_commit neo4j_branch neo4j_branch_owner tool_branch tool_branch_owner tool_commit results_store_uri results_store_user results_store_password benchmark_config teamcity_build_id jvm_args jmh_args neo4j_config_path jvm_path profilers triggered_by work_dir"
    exit 1
fi

neo4j_version="${1}"
neo4j_commit="${2}"
neo4j_branch="${3}"
neo4j_branch_owner="${4}"
tool_branch="${5}"
tool_branch_owner="${6}"
tool_commit="${7}"
results_store_uri="${8}"
results_store_user="${9}"
results_store_password="${10}"
benchmark_config="${11}"
teamcity_build_id="${12}"
parent_teamcity_build_id="${13}"
jvm_args="${14}"
jmh_args="${15}"
neo4j_config_path="${16}"
jvm_path="${17}"
profilers="${18}"
triggered_by="${19}"
work_dir="${20}"
micro_benchmarks_dir=$(pwd)
json_path=${micro_benchmarks_dir}/results.json

# here we are checking for optional AWS endpoint URL,
# this is required for end to end testing, where we mock s3
AWS_EXTRAS=
if [[ $# -eq 21 ]]; then
	AWS_EXTRAS="--endpoint-url=${21}"
fi

if [[ -z "$JAVA_HOME" ]]; then
 echo "JAVA_HOME not set, bye, bye"
 exit 1
fi

uuid=$(uuidgen)
profiler_recording_output_dir="${micro_benchmarks_dir}"/"${uuid}"
mkdir "${profiler_recording_output_dir}"

echo "Neo4j version: ${neo4j_version}"
echo "Neo4j commit: ${neo4j_commit}"
echo "Neo4j branch: ${neo4j_branch}"
echo "Neo4j branch owner: ${neo4j_branch_owner}"
echo "Tool branch: ${tool_branch}"
echo "Tool branch owner: ${tool_branch_owner}"
echo "Tool benchmarks commit: ${tool_commit}"
echo "Results store uri: ${results_store_uri}"
echo "Results store user: ${results_store_user}"
echo "Benchmark config: ${benchmark_config}"
echo "Neo4j config path: ${neo4j_config_path}"
echo "TeamCity Build ID: ${teamcity_build_id}"
echo "TeamCity Parent Build ID: ${parent_teamcity_build_id}"
echo "JVM: ${jvm_path}"
echo "JVM args: ${jvm_args}"
echo "JMH args: ${jmh_args}"
echo "FlameGraph dir: ${FLAMEGRAPH_DIR}"
echo "JFR FlameGraph Dir: ${JFR_FLAMEGRAPH}"
echo "JSON Path : ${json_path}"
echo "Profiler Recordings dir: ${profiler_recording_output_dir}"
echo "Profilers: ${profilers}"
echo "Build triggered by : ${triggered_by}"
echo "Work directory : ${work_dir}"

jar_path="${micro_benchmarks_dir}/target/micro-benchmarks.jar"

${jvm_path} -jar "${jar_path}" run-export  \
        --jvm "${jvm_path}" \
        --jvm_args "${jvm_args}" \
        --jmh "${jmh_args}" \
        --neo4j_config "${neo4j_config_path}" \
        --json_path "${json_path}" \
        --neo4j_commit "${neo4j_commit}" \
        --neo4j_version "${neo4j_version}" \
        --neo4j_branch "${neo4j_branch}" \
        --branch_owner "${neo4j_branch_owner}" \
        --teamcity_build "${teamcity_build_id}" \
        --parent_teamcity_build "${parent_teamcity_build_id}" \
        --tool_commit "${tool_commit}" \
        --tool_branch "${tool_branch}" \
        --tool_branch_owner "${tool_branch_owner}" \
        --config "${benchmark_config}" \
        --triggered-by "${triggered_by}" \
        --stores-dir "${work_dir}" \
        --profiles-dir "${profiler_recording_output_dir}" \
        --profilers "${profilers}"

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
${jvm_path} -cp "${jar_path}" com.neo4j.bench.client.Main add-profiles \
    --dir "${profiler_recording_output_dir}"  \
    --s3-bucket benchmarking.neo4j.com/recordings/"${profiler_recording_dir_name}" \
    --archive benchmarking.neo4j.com/recordings/"${archive}"  \
    --test_run_report "${json_path}" \
    --ignore_unrecognized_files

${jvm_path} -cp "${jar_path}" com.neo4j.bench.client.Main report \
            --results_store_uri "${results_store_uri}"  \
            --results_store_user "${results_store_user}"  \
            --results_store_pass "${results_store_password}" \
            --test_run_results "${json_path}"
