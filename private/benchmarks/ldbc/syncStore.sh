#!/usr/bin/env bash
#
# Copyright (c) 2002-2019 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.
#


set -e
set -u
set -x
if [[ $# -lt 4 ]] ; then
    echo "Expected 1 argument, but got $#"
    echo "usage: ./syncStore.sh dataSet dbName rootDbDir rootCsvDir"
    exit 1
fi
dataSet="${1}"
dbName="${2}"
rootDbDir="${3}"
rootCsvDir="${4}"
dataSetTar="${dbName}".tar.gz
#Check if we need to sync, and store the result in doDownload
doDownload=$(aws s3 sync s3://benchmarking.neo4j.com/datasets/ldbc/db/ "${rootDbDir}" --exclude "*" --include "${dataSetTar}" --dryrun)
if [[ -n ${doDownload} ]]; then
    #sync
    aws s3 sync s3://benchmarking.neo4j.com/datasets/ldbc/db/ "${rootDbDir}" --exclude "*" --include "${dataSetTar}"
    #remove the old db folder on disk
    rm -rf "${rootDbDir:?}"/"${dbName}"
    #extract the synced db into the right folder
    tar -xzvf "${rootDbDir}"/"${dataSetTar}" -C "${rootDbDir}"
fi

aws s3 sync s3://benchmarking.neo4j.com/datasets/ldbc/csv/"${dataSet}" "${rootCsvDir}"/"${dataSet}"