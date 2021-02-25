#!/usr/bin/env bash
#
# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.
#
set -eux

dataSet=
dbName=
rootDbDir=
rootCsvDir=
recordings_base_uri=s3://storage.benchmarking.neo4j.today/datasets/ldbc

while (("$#")); do
  case "$1" in
  --data-set)
    dataSet=$2
    shift 2
    ;;
  --db-name)
    dbName=$2
    shift 2
    ;;
  --root-db-dir)
    rootDbDir=$2
    shift 2
    ;;
  --root-csv-dir)
    rootCsvDir=$2
    shift 2
    ;;
  --recordings-base-uri)
    recordings_base_uri=$2
    shift 2
    ;;
  --) # end of argument parsing
    shift
    break
    ;;
  *)
    shift
    ;;
  esac
done

if [[ -z "$dataSet" ]]; then
  echo "data set argument is not set"
  exit 1
fi

if [[ -z "$dbName" ]]; then
  echo "db name argument is not set"
  exit 1
fi

if [[ -z "$rootDbDir" ]]; then
  echo "root db dir argument is not set"
  exit 1
fi

if [[ -z "$rootCsvDir" ]]; then
  echo "root csv dir argument is not set"
  exit 1
fi

dataSetTar="${dbName}".tar.gz

#remove trailing /, if needed
recordings_base_uri=${recordings_base_uri%/}

#Check if we need to sync, and store the result in doDownload
doDownload=$(aws s3 sync "${recordings_base_uri}/db/" "${rootDbDir}" --exclude "*" --include "${dataSetTar}" --dryrun)
if [[ -n ${doDownload} ]]; then
  #sync
  aws s3 sync "${recordings_base_uri}/db/" "${rootDbDir}" --exclude "*" --include "${dataSetTar}"
  #remove the old db folder on disk
  rm -rf "${rootDbDir:?}"/"${dbName}"
  #extract the synced db into the right folder
  tar -xzvf "${rootDbDir}"/"${dataSetTar}" -C "${rootDbDir}"
fi

aws s3 sync "${recordings_base_uri}/csv/${dataSet}" "${rootCsvDir}"/"${dataSet}"