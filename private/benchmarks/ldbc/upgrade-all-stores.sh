#!/usr/bin/env bash
#
# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.
#

set -eu

old_neo4j_version="42"
new_neo4j_version="43"
set -e
set -u

if [[ -z "$JAVA_HOME" ]]; then
    echo "JAVA_HOME not set, bye, bye"
fi

dbs=(
 "db_sf001_p006_regular_utc_${old_neo4j_version}ce;db_sf001_p006_regular_utc_${new_neo4j_version}ce;neo4j_sf001.conf"
 "db_sf001_p064_regular_utc_${old_neo4j_version}ce;db_sf001_p064_regular_utc_${new_neo4j_version}ce;neo4j_sf001.conf"
 "db_sf010_p064_regular_utc_${old_neo4j_version}ce;db_sf010_p064_regular_utc_${new_neo4j_version}ce;neo4j_sf010.conf"
 "db_sf100_p064_dense1_utc_day_${old_neo4j_version}ce;db_sf100_p064_dense1_utc_day_${new_neo4j_version}ce;neo4j_sf100.conf"
 "db_sf100_p064_dense1_utc_day_${old_neo4j_version}ee;db_sf100_p064_dense1_utc_day_${new_neo4j_version}ee;neo4j_sf100_ee.conf"
 "db_sf100_p064_regular_utc_${old_neo4j_version}ce;db_sf100_p064_regular_utc_${new_neo4j_version}ce;neo4j_sf100.conf"
)

working_dir=$(pwd)
for i in "${dbs[@]}"; do

    # shellcheck disable=SC2206
    arr=(${i//;/ })
    old_db_name=${arr[0]}
    new_db_name=${arr[1]}
    neo4j_config_file=${arr[2]}
    neo4j_config="neo4j-connectors/src/main/resources/neo4j/${neo4j_config_file}"
    old_db_path="$(pwd)/${old_db_name}"
    old_tar="${old_db_name}".tar.gz
    new_db_path="$(pwd)/${new_db_name}"
    new_tar="${new_db_name}".tar.gz
    echo "---------------"
    echo "Upgrading database"
    echo "Old name : ${old_db_name}"
    echo "New name : ${new_db_name}"
    echo "Old path : ${old_db_path}"
    echo "New path : ${new_db_path}"
    echo "Config   : ${neo4j_config}"
    aws s3 cp s3://benchmarking.neo4j.com/datasets/ldbc/db/"${old_tar}" . --no-progress

    tar -xzvf "${old_tar}"
    rm "${old_tar}"
    echo "---------------"
    echo "Preparing temporary locations"
	temp_old_db_path="$working_dir/"$(basename "$(mktemp -d -u)")

    echo "Temporary old db path : ${temp_old_db_path}"

	mkdir -p "${temp_old_db_path}"
	mv "${old_db_path}"/* "${temp_old_db_path}"

	temp_new_db_path="$working_dir/"$(basename "$(mktemp -d -u)")

    echo "Temporary old db path : ${temp_old_db_path}"

    "${JAVA_HOME}/bin/java" -jar neo4j-connectors/target/ldbc.jar upgrade-store \
        --original-db "${temp_old_db_path}" \
        --upgraded-db "${new_db_path}" \
        --recreate-indexes  \
        --config "${neo4j_config}"

    tar -cvzf "${new_tar}" "${new_db_name}"

    aws s3 cp "${new_tar}" s3://benchmarking.neo4j.com/datasets/ldbc/db/"${new_tar}" --no-progress
    rm -rf "${old_db_path}"
    rm -rf "${new_db_path}"
    rm -rf "${temp_old_db_path}"
    rm -rf "${temp_new_db_path}"
    rm -f "${new_tar}"

done
