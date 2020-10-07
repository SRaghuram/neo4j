#!/bin/bash
#
# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.
#

set -e
set -u
set -x
#this is a reasonable default if we do not want to specify it
benchmark_conf=$(pwd)/benchmark.conf
#If we do not want any partitions
partitions=-1
returnedPartitionNumber=-1

while (("$#")); do
  case "$1" in
  --group)
    group=$2
    shift 2
    ;;
  --partitions)
    partitions=$2
    shift 2
    ;;
  --config-path)
    benchmark_conf=$2
    shift 2
    ;;
  --returnedPartitionNumber)
    returnedPartitionNumber=$2
    shift 2
    ;;
  --) # end of argument parsing
    shift
    break
    ;;
  esac
done

java -jar target/micro-benchmarks.jar config groups \
  --path "${benchmark_conf}" \
  "${group}"

if [ "${partitions}" -ge 1 ]; then
  benchmark_partitions=$(pwd)/partition
  mkdir "${benchmark_partitions}"
  java -jar target/micro-benchmarks.jar config partition \
    -p "${partitions}" \
    -d "${benchmark_partitions}" \
    --config-path "${benchmark_conf}"
  # now we have generated the configs in $benchmark_partition
  # if we want a specific number back we have to be returned
  if [ "${returnedPartitionNumber}" -ge 1 ]; then
    returnedPartition=${benchmark_partitions}/micro_"${returnedPartitionNumber}".conf
    mv "${returnedPartition}" "${benchmark_conf}"
  fi
fi
