#!/usr/bin/env bash
#
# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.
#


set -e
set -u
set -x

neo4j_version="34"

dbs=(
 "db_sf001_p006_regular_utc_${neo4j_version}ce"
 "db_sf001_p064_regular_utc_${neo4j_version}ce"
 "db_sf010_p064_regular_utc_${neo4j_version}ce"
 "db_sf100_p064_dense1_utc_day_${neo4j_version}ce"
 "db_sf100_p064_dense1_utc_day_${neo4j_version}ee"
)

for i in "${dbs[@]}"; do
    # shellcheck disable=SC2206
    arr=(${i//;/ })
    db_name=${arr[0]}
    tar_name="${db_name}".tar.gz
    echo "Zipping database"
    echo "DB Name : ${db_name}"
    echo "targz name : ${tar_name}"
    aws s3 cp s3://benchmarking.neo4j.com/datasets/ldbc/db/"${db_name}" "${db_name}" --no-progress --recursive
    tar -cvzf "${tar_name}" "${db_name}"

    aws s3 cp "${tar_name}" s3://benchmarking.neo4j.com/datasets/ldbc/db/"${tar_name}" --no-progress
    rm -rf "${db_name}"
    rm -rf "${tar_name}"
done
