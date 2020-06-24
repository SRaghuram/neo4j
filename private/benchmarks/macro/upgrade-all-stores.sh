#!/usr/bin/env bash
#
# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.
#


if [ $# -lt 2 ] ; then
    echo "Expected at least 2 arguments, but got $#"
    echo "usage: ./upgrade-all-stores.sh new_neo4j_version old_neo4j_version"
    exit 1
fi

if [[ -z "$JAVA_HOME" ]]; then
    echo "JAVA_HOME not set, bye, bye"
fi

new_neo4j_version="${1}"
old_neo4j_version="${2}"

db_and_workloads=(
 "accesscontrol;accesscontrol"
 "bubble_eye;bubble_eye"
 "cineasts;cineasts"
 "cineasts_csv;cineasts_csv"
 "elections;elections"
 "generatedmusicdata;generatedmusicdata_read"
 "grid;grid"
 "ldbc_sf001;ldbc_sf001"
 "ldbc_sf010;ldbc_sf010"
 "levelstory;levelstory"
 "logistics;logistics"
 "musicbrainz;musicbrainz"
 "nexlp;nexlp"
 "pokec;pokec_read"
 "qmul;qmul_read"
 "recommendations;recommendations"
 "socialnetwork;socialnetwork"
 "osmnodes;osmnodes"
 "offshore_leaks;offshore_leaks"
 "fraud-poc;fraud-poc-credit"
 "alacrity;alacrity"
 "fraud-poc-aml;fraud-poc-aml"
 "zero;zero")

for i in "${db_and_workloads[@]}"; do

    # shellcheck disable=SC2206
    arr=(${i//;/ })
    db_name=${arr[0]}
    workload=${arr[1]}
    zip_file=${db_name}.tgz
    echo "---------------"
    echo Working on file: "${zip_file}"
    echo With workload: "${workload}"
    aws s3 cp s3://benchmarking.neo4j.com/datasets/macro/"${old_neo4j_version}"-enterprise-datasets/"${zip_file}" ./ --no-progress
    tar xzvf "${zip_file}"
    rm "${zip_file}"

    "${JAVA_HOME}/bin/java"  -jar target/macro.jar upgrade-store \
                               --original-db "${db_name}" \
                               --workload "${workload}" \
                               --db-edition ENTERPRISE

    tar -cvzf "${zip_file}" "${db_name}"
    aws s3 cp "${zip_file}" s3://benchmarking.neo4j.com/datasets/macro/"${new_neo4j_version}"-enterprise-datasets/"${zip_file}" --no-progress
    rm "${zip_file}"
    rm -r "${db_name}"
done
