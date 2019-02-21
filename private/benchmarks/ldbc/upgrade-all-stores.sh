#!/usr/bin/env bash
#
# Copyright (c) 2002-2019 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.
#


set -e
set -u

old_neo4j_version="34"
new_neo4j_version="35"

dbs=(
 "db_sf001_p006_regular_utc_${old_neo4j_version}ce;db_sf001_p006_regular_utc_${new_neo4j_version}ce;neo4j_sf001.conf"
 "db_sf001_p064_regular_utc_${old_neo4j_version}ce;db_sf001_p064_regular_utc_${new_neo4j_version}ce;neo4j_sf001.conf"
 "db_sf010_p064_regular_utc_${old_neo4j_version}ce;db_sf010_p064_regular_utc_${new_neo4j_version}ce;neo4j_sf010.conf"
 "db_sf100_p064_dense1_utc_day_${old_neo4j_version}ce;db_sf100_p064_dense1_utc_day_${new_neo4j_version}ce;neo4j_sf100.conf"
 "db_sf100_p064_dense1_utc_day_${old_neo4j_version}ee;db_sf100_p064_dense1_utc_day_${new_neo4j_version}ee;neo4j_sf100_ee.conf"
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
    new_db_path="$(pwd)/${new_db_name}"
    echo "---------------"
    echo "Upgrading database"
    echo "Old name : ${old_db_name}"
    echo "New name : ${new_db_name}"
    echo "Old path : ${old_db_path}"
    echo "New path : ${new_db_path}"
    echo "Config   : ${neo4j_config}"
    aws s3 sync s3://benchmarking.neo4j.com/datasets/ldbc/db/"${old_db_name}" "${old_db_name}" --no-progress
    
    echo "---------------"
    echo "Preparing temporary locations"
	temp_old_db_path=$(mktemp -d -p "${working_dir}")
	temp_new_db_path=$(mktemp -d -u -p "${working_dir}")
    echo "Temporary old db path : ${temp_old_db_path}"
    echo "Temporary new db path : ${temp_new_db_path}"
	
	mkdir "${temp_old_db_path}"/graph.db
	mv "${old_db_path}"/* "${temp_old_db_path}"/graph.db
	
    java -jar neo4j-connectors/target/ldbc.jar upgrade-store \
        --original-db "${temp_old_db_path}" \
        --upgraded-db "${temp_new_db_path}" \
        --recreate-indexes  \
        --config "${neo4j_config}"

	mkdir "${new_db_path}"
	mv "${temp_new_db_path}"/graph.db/* "${new_db_path}"

    aws s3 sync "${new_db_path}" s3://benchmarking.neo4j.com/datasets/ldbc/db/"${new_db_name}" --no-progress --delete
    rm -rf "${old_db_path}"
    rm -rf "${new_db_path}"
    rm -rf "${temp_old_db_path}"
    rm -rf "${temp_new_db_path}"
done
