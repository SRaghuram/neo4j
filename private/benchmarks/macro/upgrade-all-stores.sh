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
# default s3 destination base
s3_dest_datasets_url="s3://storage.benchmarking.neo4j.today/datasets/macro"

db_and_workloads_record_format=(
 "accesscontrol;accesscontrol;high_limit"
 "bubble_eye;bubble_eye;high_limit"
 "cineasts;cineasts;high_limit"
 "cineasts_csv;cineasts_csv;high_limit"
 "elections;elections;high_limit"
 "generatedmusicdata;generatedmusicdata_read;high_limit"
 "grid;grid;high_limit"
 "ldbc_sf001;ldbc_sf001;high_limit"
 "ldbc_sf010;ldbc_sf010;high_limit"
 "levelstory;levelstory;high_limit"
 "logistics;logistics;high_limit"
 "musicbrainz;musicbrainz;high_limit"
 "nexlp;nexlp;high_limit"
 "pokec;pokec_read;high_limit"
 "qmul;qmul_read;high_limit"
 "recommendations;recommendations;high_limit"
 "socialnetwork;socialnetwork;high_limit"
 "osmnodes;osmnodes;high_limit"
 "offshore_leaks;offshore_leaks;high_limit"
 "fraud-poc;fraud-poc-credit;high_limit"
 "alacrity;alacrity;high_limit"
 "ciena;ciena;standard"
 "zero;zero;high_limit")

compress_with=$(command -v pigz)

if [[ ! -f "$compress_with" ]]; then
  compress_with=$(command -v gzip)
fi

for i in "${db_and_workloads_record_format[@]}"; do

    # shellcheck disable=SC2206
    arr=(${i//;/ })
    db_name=${arr[0]}
    workload=${arr[1]}
    record_format=${arr[2]}
    zip_file=${db_name}.tgz
    echo "---------------"
    echo Working on file: "${zip_file}"
    echo With workload: "${workload}"
    aws s3 cp "${s3_dest_datasets_url}/${old_neo4j_version}-enterprise-datasets/${zip_file}" ./ --no-progress
    tar xzvf "${zip_file}"
    rm "${zip_file}"

    "${JAVA_HOME}/bin/java"  -jar target/macro.jar upgrade-store \
                               --original-db "${db_name}" \
                               --workload "${workload}" \
                               --db-edition ENTERPRISE \
                               --record-format "${record_format}"

    tar -cvzf "${zip_file}" "${db_name}"
    aws s3 cp "${zip_file}" "${s3_dest_datasets_url}/${new_neo4j_version}-enterprise-datasets/${zip_file}" --no-progress
    rm "${zip_file}"
    rm -r "${db_name}"
done
