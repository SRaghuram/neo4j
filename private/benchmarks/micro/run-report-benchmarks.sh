#!/bin/bash
#
# Copyright (c) 2002-2019 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.
#


set -e
set -u

if [ $# -lt 21 ] ; then
    echo "Expected at least 21 arguments, but got $#"
    echo "usage: ./run-report-benchmarks.sh neo4j_version neo4j_commit neo4j_branch neo4j_branch_owner tool_branch tool_branch_owner tool_commit results_store_uri results_store_user results_store_password benchmark_config teamcity_build_id tarball jvm_args jmh_args neo4j_config_path jvm_path with_jfr with_async triggered_by"
    exit -1
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
tarball="${14}"
jvm_args="${15}"
jmh_args="${16}"
neo4j_config_path="${17}"
jvm_path="${18}"
with_jfr="${19}"
with_async="${20}"
triggered_by="${21}"
micro_benchmarks_dir=$(pwd)
json_path=${micro_benchmarks_dir}/results.json

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
echo "Neo4j package: ${tarball}"
echo "Benchmark config: ${benchmark_config}"
echo "Neo4j config path: ${neo4j_config_path}"
echo "TeamCity Build ID: ${teamcity_build_id}"
echo "TeamCity Parent Build ID: ${parent_teamcity_build_id}"
echo "Tarball: ${tarball}"
echo "JVM: ${jvm_path}"
echo "JVM args: ${jvm_args}"
echo "JMH args: ${jmh_args}"
echo "FlameGraph dir: ${FLAMEGRAPH_DIR}"
echo "JFR FlameGraph Dir: ${JFR_FLAMEGRAPH}"
echo "JSON Path : ${json_path}"
echo "Profiler Recordings dir: ${profiler_recording_output_dir}"
echo "JFR Enabled : ${with_jfr}"
echo "ASYNC Enabled : ${with_async}"
echo "Build triggered by : ${triggered_by}"

function runExport {
    #shellcheck disable=SC2068
    java -jar "${micro_benchmarks_dir}"/micro/target/micro-benchmarks.jar run-export  \
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
            --neo4j_package_for_jvm_args "${tarball}" \
            --config "${benchmark_config}" \
            --triggered-by "${triggered_by}" \
            $@
}
profilers_input="--profiles-dir ${profiler_recording_output_dir}"

[ "${with_jfr}" = "true" ] && profilers_input="$profilers_input --profile-jfr" && echo "Profiling with JFR Enabled!"
[ "${with_async}" = "true" ] && profilers_input="$profilers_input --profile-async" && echo "Profiling with Async Enabled!"
#shellcheck disable=SC2086
runExport ${profilers_input}

# --- create archive of profiler recording artifacts---
profiler_recording_dir_name=$(basename "${profiler_recording_output_dir}")
archive="${profiler_recording_dir_name}".tar.gz
tar czvf "${archive}" "${profiler_recording_dir_name}"

# --- upload archive of profiler recording artifacts to S3 ---
aws --region eu-north-1 s3 cp "${archive}" s3://benchmarking.neo4j.com/recordings/"${archive}"
# --- upload profiler recording artifacts to S3 ---
aws --region eu-north-1 s3 sync "${profiler_recording_output_dir}" s3://benchmarking.neo4j.com/recordings/"${profiler_recording_dir_name}"

# --- enrich results file with profiler recording information (locations in S3) ---
java -cp "${micro_benchmarks_dir}"/micro/target/micro-benchmarks.jar com.neo4j.bench.client.Main add-profiles \
    --dir "${profiler_recording_output_dir}"  \
    --s3-bucket benchmarking.neo4j.com/recordings/"${profiler_recording_dir_name}" \
    --archive benchmarking.neo4j.com/recordings/"${archive}"  \
    --test_run_report "${json_path}" \
    --ignore_unrecognized_files

java -cp "${micro_benchmarks_dir}"/micro/target/micro-benchmarks.jar com.neo4j.bench.client.Main report \
            --results_store_uri "${results_store_uri}"  \
            --results_store_user "${results_store_user}"  \
            --results_store_pass "${results_store_password}" \
            --test_run_results "${json_path}"
