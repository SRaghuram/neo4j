#!/bin/bash
#
# Copyright (c) 2002-2019 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.
#


set -e
set -u

if [ $# -lt 34 ] ; then
    echo "Expected at least 34 arguments, but got $#"
    echo "usage: ./run-report-benchmarks.sh neo4j_version neo4j_commit neo4j_branch neo4j_branch_owner neo4j_api neo4j_planner neo4j_runtime neo4j_config neo4j_benchmark_config neo4j_tarball teamcity_build_id teamcity_parent_build_id ldbc_tooling_commit tool-branch tool-branch-owner results_store_uri results_store_user results_store_pass ldbc_read_params ldbc_write_params ldbc_config ldbc_read_threads ldbc_warmup_count ldbc_run_count ldbc_repetition_count ldbc_results_dir ldbc_working_dir ldbc_source_db ldbc_db_reuse_policy ldbc_jvm_args jvm_path with_jfr with_async triggered_by"
    exit -1
fi

neo4j_version="${1}"
neo4j_commit="${2}"
neo4j_branch="${3}"
neo4j_branch_owner="${4}"
neo4j_api="${5}"
neo4j_planner="${6}"
neo4j_runtime="${7}"
neo4j_config="${8}"
neo4j_benchmark_config="${9}"
neo4j_tarball="${10}"
teamcity_build_id="${11}"
teamcity_parent_build_id="${12}"
ldbc_tooling_commit="${13}"
tool_branch="${14}"
tool_branch_owner="${15}"
results_store_uri="${16}"
results_store_user="${17}"
results_store_pass="${18}"
ldbc_read_params="${19}"
ldbc_write_params="${20}"
ldbc_config="${21}"
ldbc_read_threads="${22}"
ldbc_warmup_count="${23}"
ldbc_run_count="${24}"
ldbc_repetition_count="${25}"
ldbc_results_dir="${26}"
ldbc_working_dir="${27}"
# TODO do something like ---> mkdir temp && mv ${ldbc_source_db} temp/ --> provide temp
ldbc_source_db="${28}"
ldbc_db_reuse_policy="${29}"
ldbc_jvm_args="${30}"
jvm_path="${31}"
with_jfr="${32}"
with_async="${33}"
triggered_by="${34}"
ldbc_benchmarks_dir=$(pwd)
json_path=${ldbc_benchmarks_dir}/results.json

echo "------------------------------------------------"
echo "------------------------------------------------"
echo "Neo4j version:       ${neo4j_version}"
echo "Neo4j commit:        ${neo4j_commit}"
echo "Neo4j branch:        ${neo4j_branch}"
echo "Neo4j branch owner:  ${neo4j_branch_owner}"
echo "Neo4j API:           ${neo4j_api}"
echo "Neo4j planner:       ${neo4j_planner}"
echo "Neo4j runtime:       ${neo4j_runtime}"
echo "Neo4j config:        ${neo4j_config}"
echo "Neo4j bench config:  ${neo4j_benchmark_config}"
echo "Neo4j package:       ${neo4j_tarball}"
echo "------------------------------------------------"
echo "TeamCity Build ID:            ${teamcity_build_id}"
echo "TeamCity Parent Build ID:     ${teamcity_parent_build_id}"
echo "TeamCity Build Triggered by:  ${triggered_by}"
echo "------------------------------------------------"
echo "LDBC tooling commit:       ${ldbc_tooling_commit}"
echo "LDBC tooling branch:       ${tool_branch}"
echo "LDBC tooling branch owner: ${tool_branch_owner}"
echo "------------------------------------------------"
echo "Results store uri:   ${results_store_uri}"
echo "Results store user:  ${results_store_user}"
echo "JSON path:           ${json_path}"
echo "------------------------------------------------"
echo "Read params dir:     ${ldbc_read_params}"
echo "Write params dir:    ${ldbc_write_params}"
echo "LDBC config:         ${ldbc_config}"
echo "Read threads:        ${ldbc_read_threads}"
echo "Warmup count:        ${ldbc_warmup_count}"
echo "Run count:           ${ldbc_run_count}"
echo "Repetition count:    ${ldbc_repetition_count}"
echo "JVM :                ${jvm_path}"
echo "JVM args:            ${ldbc_jvm_args}"
echo "------------------------------------------------"
echo "Results dir:         ${ldbc_results_dir}"
echo "Working dir:         ${ldbc_working_dir}"
echo "Source DB dir:       ${ldbc_source_db}"
echo "DB reuse policy:     ${ldbc_db_reuse_policy}"
echo "------------------------------------------------"
echo "FlameGraph dir:      ${FLAMEGRAPH_DIR}"
echo "JFR Enabled:         ${with_jfr}"
echo "JFR FlameGraph Dir:  ${JFR_FLAMEGRAPH}"
echo "ASYNC Enabled:       ${with_async}"
echo "Async Dir:           ${ASYNC_PROFILER_DIR}"
echo "------------------------------------------------"
echo "------------------------------------------------"

function runReport {
    #shellcheck disable=SC2068
    java -jar "${ldbc_benchmarks_dir}"/neo4j-connectors/target/ldbc.jar run-export \
        --jvm "${jvm_path}" \
        --jvm-args "${ldbc_jvm_args}" \
        --neo4j-package-for-jvm-args "${neo4j_tarball}" \
        --reads "${ldbc_read_params}" \
        --writes "${ldbc_write_params}" \
        --ldbc-config "${ldbc_config}" \
        --read-threads "${ldbc_read_threads}" \
        --warmup-count "${ldbc_warmup_count}" \
        --run-count "${ldbc_run_count}" \
        --results "${ldbc_results_dir}" \
        --repetition-count "${ldbc_repetition_count}" \
        --neo4j-api "${neo4j_api}" \
        --planner "${neo4j_planner}" \
        --runtime "${neo4j_runtime}" \
        --db "${ldbc_source_db}" \
        --neo4j-config "${neo4j_config}" \
        --neo4j-benchmark-config "${neo4j_benchmark_config}" \
        --neo4j-branch "${neo4j_branch}" \
        --neo4j-commit "${neo4j_commit}" \
        --neo4j-branch-owner "${neo4j_branch_owner}" \
        --neo4j-version "${neo4j_version}" \
        --teamcity_build "${teamcity_build_id}" \
        --teamcity_parent_build "${teamcity_parent_build_id}" \
        --tool-commit "${ldbc_tooling_commit}" \
        --tool-branch-owner "${tool_branch_owner}" \
        --tool-branch "${tool_branch}" \
        --json-output "${json_path}" \
        --ldbc-jar "${ldbc_benchmarks_dir}"/neo4j-connectors/target/ldbc.jar  \
        --working-dir "${ldbc_working_dir}" \
        --reuse-db "${ldbc_db_reuse_policy}" \
        --triggered-by "${triggered_by}" \
        --trace \
         --profile-gc \
        $@
}


uuid=$(uuidgen)
profiler_recording_dir="${ldbc_results_dir}/${uuid}"
profilers_input="--profiles-dir ${profiler_recording_dir}"

[ "${with_jfr}" = "true" ] && profilers_input="$profilers_input --profile-jfr" && echo "Profiling with JFR Enabled!"
[ "${with_async}" = "true" ] && profilers_input="$profilers_input --profile-async" && echo "Profiling with Async Enabled!"
#shellcheck disable=SC2086
runReport ${profilers_input}

# --- create archive of profiler recording artifacts---
profiler_recording_dir_name=$(basename "${profiler_recording_dir}")
archive=${profiler_recording_dir_name}.tar.gz
tar czvf "${archive}" "${profiler_recording_dir}"

# --- upload archive of profiler recording artifacts to S3 ---
aws --region eu-north-1 s3 cp "${archive}" s3://benchmarking.neo4j.com/recordings/"${archive}"
# --- upload profiler recording artifacts to S3 ---
aws --region eu-north-1 s3 sync "${profiler_recording_dir}" s3://benchmarking.neo4j.com/recordings/"${profiler_recording_dir_name}"

# --- enrich results file with profiler recording information (locations in S3) ---
java -cp "${ldbc_benchmarks_dir}"/neo4j-connectors/target/ldbc.jar com.neo4j.bench.client.Main add-profiles \
        --dir "${profiler_recording_dir}"  \
        --s3-bucket benchmarking.neo4j.com/recordings/"${profiler_recording_dir_name}" \
        --archive benchmarking.neo4j.com/recordings/"${archive}"  \
        --test_run_report "${json_path}" \
        --ignore_unrecognized_files

java -cp "${ldbc_benchmarks_dir}"/neo4j-connectors/target/ldbc.jar com.neo4j.bench.client.Main report \
            --results_store_uri "${results_store_uri}" \
            --results_store_user "${results_store_user}" \
            --results_store_pass "${results_store_pass}" \
            --test_run_results "${json_path}"
