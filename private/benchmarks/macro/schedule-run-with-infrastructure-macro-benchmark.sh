#!/usr/bin/env bash
set -eu
# this script schedules run of benchmark in batch infrastructure

# required arguments
infrastructure_capabilities=
batch_stack=
workspace_dir=
branch_owner=

# optional arguments
workload=accesscontrol
profilers="GC"
db_name=$workload
dataset_base_uri=
# get neo4j version from POM
neo4j_version=
neo4j_branch=

recordings_base_uri=
artifact_base_uri=

while (("$#")); do
  case "$1" in
  --infrastructure-capabilities)
    infrastructure_capabilities=$2
    shift 2
    ;;
  --batch-stack)
    batch_stack=$2
    shift 2
    ;;
  --results-store-pass-secret-name)
    results_store_pass_secret_name=$2
    shift 2
    ;;
  --workspace-dir)
    workspace_dir=$2
    shift 2
    ;;
  --profilers)
    profilers=$2
    shift 2
    ;;
  --workload)
    workload=$2
    shift 2
    ;;
  --db-name)
    db_name=$2
    shift 2
    ;;
  --branch-owner)
    branch_owner=$2
    shift 2
    ;;
  --neo4j-version)
    neo4j_version=$2
    shift 2
    ;;
  --neo4j-branch)
    neo4j_branch=$2
    shift 2
    ;;
  --dataset-base-uri)
    dataset_base_uri=$2
    shift 2
    ;;
  --artifact-base-uri)
    artifact_base_uri=$2
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

if [[ -z "$neo4j_version" ]]; then
  neo4j_version=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
fi

if [[ -z "$neo4j_branch" ]]; then
  neo4j_branch=$(git rev-parse --abbrev-ref HEAD)
fi

if [[ -z "$infrastructure_capabilities" || -z "$batch_stack" || -z "$workspace_dir" || -z "$branch_owner" ]]; then
  echo -e "missing required arguments, call this script like this: \n\n $0 --infrastructure-capabilities [infrastructure capabilities] --batch-stack [batch stack] --workspace-dir [workspace dir] --branch-owner [branch owner]"
  exit 1
fi

if [[ -z "$JAVA_HOME" ]]; then
  echo "no JAVA_HOME set, bye bye"
  exit 1
fi

java_cmd="$JAVA_HOME/bin/java"

benchmark_infra_scheduler_jar="target/macro.jar"
if [[ ! -f "$benchmark_infra_scheduler_jar" ]]; then
  echo "$benchmark_infra_scheduler_jar doesn't exists run mvn package -PfullBenchmarks"
  exit 1
fi

neo4j_commit=$(git rev-parse HEAD)
triggered_by=$(whoami)
parent_teamcity_build="-1"
teamcity_build="$RANDOM"
artifact_base_uri=s3://storage.benchmarking.neo4j.com/artifacts/macro/

$java_cmd -jar $benchmark_infra_scheduler_jar \
  schedule \
  --artifact-base-uri \
  "$artifact_base_uri" \
  --workload \
  "$workload" \
  --db-edition \
  ENTERPRISE \
  --jvm \
  /usr/lib/jvm/oracle-jdk-8/bin/java \
  --profilers \
  "$profilers" \
  --warmup-count \
  1000 \
  --measurement-count \
  1000 \
  --forks \
  1 \
  --time-unit \
  MICROSECONDS \
  --runtime \
  DEFAULT \
  --planner \
  DEFAULT \
  --execution-mode \
  EXECUTE \
  --error-policy \
  FAIL \
  --jvm-args \
  -Xmx4g \
  --neo4j-deployment \
  embedded \
  --neo4j-commit \
  "$neo4j_commit" \
  --neo4j-version \
  "$neo4j_version" \
  --neo4j-branch \
  "$neo4j_branch" \
  --neo4j-branch-owner \
  "$branch_owner" \
  --teamcity-build \
  "$teamcity_build" \
  --parent-teamcity-build \
  "$parent_teamcity_build" \
  --triggered-by \
  "$triggered_by" \
  --workspace-dir \
  "$workspace_dir" \
  --db-name \
  "$db_name" \
  --results-store-pass-secret-name \
  "$results_store_pass_secret_name" \
  --infrastructure-capabilities \
  "$infrastructure_capabilities" \
  --batch-stack \
  "$batch_stack" \
  ${dataset_base_uri:+--dataset-base-uri $dataset_base_uri} \
  ${recordings_base_uri:+--recordings-base-uri $recordings_base_uri}
