#!/bin/bash
#
# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.
#
set -eux

if [ $# -lt 17 ] ; then
    echo "Expected at least 17 arguments, but got $#"
    echo "usage: ./run-report-benchmarks.sh neo4j_version neo4j_commit neo4j_branch neo4j_branch_owner results_store_uri results_store_user results_store_password benchmark_config teamcity_build_id jvm_args jmh_args neo4j_config_path jvm_path profilers triggered_by work_dir"
    exit 1
fi

neo4j_version="${1}"
neo4j_commit="${2}"
neo4j_branch="${3}"
neo4j_branch_owner="${4}"
results_store_uri="${5}"
results_store_user="${6}"
results_store_password="${7}"
benchmark_config="${8}"
teamcity_build_id="${9}"
parent_teamcity_build_id="${10}"
jvm_args="${11}"
jmh_args="${12}"
neo4j_config_path="${13}"
jvm_path="${14}"
profilers="${15}"
triggered_by="${16}"
work_dir="${17}"
micro_benchmarks_dir=$(pwd)
recordings_base_uri=

# here we are checking for optional AWS endpoint URL,
# this is required for end to end testing, where we mock s3
# parse optional arguments
all_args=("$@")
optional_args=("${all_args[@]:17}")
aws_endpoint_url=
recordings_base_uri=

while ((${#optional_args[@]})); do
  arg=${optional_args[0]}
  optional_args=("${optional_args[@]:1}")
  case "$arg" in
  --aws-endpoint-url)
    aws_endpoint_url=${optional_args[0]}
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

if [[ -z "$JAVA_HOME" ]]; then
 echo "JAVA_HOME not set, bye, bye"
 exit 1
fi

echo "Neo4j version: ${neo4j_version}"
echo "Neo4j commit: ${neo4j_commit}"
echo "Neo4j branch: ${neo4j_branch}"
echo "Neo4j branch owner: ${neo4j_branch_owner}"
echo "Results store uri: ${results_store_uri}"
echo "Results store user: ${results_store_user}"
echo "Benchmark config: ${benchmark_config}"
echo "Neo4j config path: ${neo4j_config_path}"
echo "TeamCity Build ID: ${teamcity_build_id}"
echo "TeamCity Parent Build ID: ${parent_teamcity_build_id}"
echo "JVM: ${jvm_path}"
echo "JVM args: ${jvm_args}"
echo "JMH args: ${jmh_args}"
echo "FlameGraph dir: ${FLAMEGRAPH_DIR:+$FLAMEGRAPH_DIR}"
echo "JFR FlameGraph Dir: ${JFR_FLAMEGRAPH:+$JFR_FLAMEGRAPH}"
echo "Profilers: ${profilers}"
echo "Build triggered by : ${triggered_by}"
echo "Work directory : ${work_dir}"

jar_path="${micro_benchmarks_dir}/target/micro-benchmarks.jar"

${jvm_path} -jar "${jar_path}" run-export  \
        --jvm "${jvm_path}" \
        --jvm_args "${jvm_args}" \
        --jmh "${jmh_args}" \
        --neo4j_config "${neo4j_config_path}" \
        --neo4j_commit "${neo4j_commit}" \
        --neo4j_version "${neo4j_version}" \
        --neo4j_branch "${neo4j_branch}" \
        --branch_owner "${neo4j_branch_owner}" \
        --teamcity_build "${teamcity_build_id}" \
        --parent_teamcity_build "${parent_teamcity_build_id}" \
        --config "${benchmark_config}" \
        --triggered-by "${triggered_by}" \
        --work-dir "${work_dir}" \
        --profilers "${profilers}" \
        --results-store-uri "${results_store_uri}" \
        --results-store-user "${results_store_user}" \
        --results-store-pass "${results_store_password}" \
        ${recordings_base_uri:+--recordings-base-uri $recordings_base_uri} \
        ${recordings_base_uri:+--recordings-base-uri $recordings_base_uri} \
        --aws-region "eu-north-1" \
        ${aws_endpoint_url:+--aws-endpoint-url $aws_endpoint_url}