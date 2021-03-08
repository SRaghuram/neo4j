#!/usr/bin/env bash
#
# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.
#
set -eux

if [[ -z "$JAVA_HOME" ]]; then
  echo "JAVA_HOME not set, bye, bye"
fi

new_neo4j_version=
old_neo4j_version=
workload=
db_name=
record_format=
# default s3 destination base
s3_origin_datasets_url="s3://storage.benchmarking.neo4j.today/datasets/macro"
s3_dest_datasets_url="s3://storage.benchmarking.neo4j.today/datasets/macro"

while (("$#")); do
  case "$1" in
  --new-neo4j-version)
    new_neo4j_version=$2
    shift 2
    ;;
  --old-neo4j-version)
    old_neo4j_version=$2
    shift 2
    ;;
  --s3-dest-datasets-url)
    s3_dest_datasets_url=$2
    shift 2
    ;;
  --s3-origin-datasets-url)
    s3_origin_datasets_url=$2
    shift 2
    ;;
  --workload)
    workload=$2
    shift 2
    ;;
  --store)
    db_name=$2
    shift 2
    ;;
  --record-format)
    record_format=$2
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

compress_with=$(command -v pigz)

if [[ ! -f "$compress_with" ]]; then
  compress_with=$(command -v gzip)
fi

zip_file="${db_name}.tgz"
echo "---------------"
echo Working on file: "${zip_file}"
echo With workload: "${workload}"
aws s3 cp "${s3_origin_datasets_url}/${old_neo4j_version}-enterprise-datasets/${zip_file}" ./ --no-progress
tar xzvf "${zip_file}"
rm "${zip_file}"
mkdir -p "${db_name}"/data/databases
mv "${db_name}"/graph.db "${db_name}"/data/databases/neo4j

"${JAVA_HOME}/bin/java" -jar macro/target/macro.jar upgrade-store \
  --original-db "${db_name}" \
  --workload "${workload}" \
  --db-edition ENTERPRISE \
  --record-format ${record_format}

tar -c -I "$compress_with" -vf "${zip_file}" "${db_name}"

aws s3 cp "${zip_file}" "${s3_dest_datasets_url}/${new_neo4j_version}-enterprise-datasets/${zip_file}" --no-progress
rm "${zip_file}"
rm -r "${db_name}"
