#!/usr/bin/env bash
# this scripts creates workspace needed by schedule-run-micro-benchmark.sh
set -eu

workspace_dir=
config=
neo4j_edition=enterprise
neo4j_version=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)

while (("$#")); do
  case "$1" in
  --workspace-dir)
    workspace_dir=$2
    shift 2
    ;;
  --config)
    config=$2
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

if [[ -z "$workspace_dir" ]]; then
  echo -e "missing required arguments, call this script with: \n\n $0 --workspace-dir [workspace dir]"
  exit 1
fi

if [[ -z "$config" ]]; then
  echo -e "missing required arguments, call this script with: \n\n $0 --config [config]"
  exit 1
fi

if [[ ! -d "$workspace_dir" ]]; then
    echo "$workspace_dir directory doesn't exist"
    exit 1
fi

if [[ ! -f "$config" ]]; then
    echo "$config file doesn't exist"
    exit 1
fi

echo -e "make sure you have build product and benchmark, from repository root"
echo -e " mvn -Dcheckstyle.skip -Drevapi.skip -DskipTests -Dlicensing.skip -Dlicense.skip -TC2 clean install -PfullBenchmarks -pl :micro,:benchmark-infra-scheduler,:benchmark-infra-worker -am"
echo -e "and product archive, from private/packaging directory"
echo -e " mvn -Dcheckstyle.skip -Drevapi.skip -DskipTests -Dlicensing.skip -Dlicense.skip clean install"

cp "$config" "$workspace_dir"
cp "../benchmark-infra/benchmark-infra-worker/target/benchmark-infra-worker.jar" "$workspace_dir"
mkdir -p "$workspace_dir/micro/target"
cp run-report-benchmarks.sh "$workspace_dir/micro"
cp target/micro-benchmarks.jar "$workspace_dir/micro/target"

tar -xz -f "../../packaging/standalone/target/neo4j-$neo4j_edition-$neo4j_version-unix.tar.gz" --strip-components=2 -C "$workspace_dir" "neo4j-$neo4j_edition-$neo4j_version/conf/neo4j.conf"
