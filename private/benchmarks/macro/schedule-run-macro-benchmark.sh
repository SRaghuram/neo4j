#!/usr/bin/env bash
set -eu
# this script schedules run of benchmark in batch infrastructure

# required arguments
job_queue=
job_definition=
batch_stack=
workspace_dir=
branch_owner=

# optional arguments
workload=accesscontrol
profilers="GC"
db_name=$workload
results_store_pass_secret_name="ResultStoreSecret-test"
# get neo4j version from POM
neo4j_version=
neo4j_branch=

while (("$#")); do
  case "$1" in
  --job-queue)
    job_queue=$2
    shift 2
    ;;
  --job-definition)
    job_definition=$2
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

if [[ -z "$job_queue" || -z "$job_definition" || -z "$batch_stack" || -z "$workspace_dir" || -z "$branch_owner" ]]; then
  echo -e "missing required arguments, call this script like this: \n\n $0 --job-queue [job queue] --job-definition [job definition] --batch-stack [batch stack] --results-db-password [results db password] --workspace-dir [workspace dir] --branch-owner [branch owner]"
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
artifact_base_uri=s3://benchmarking.neo4j.com/artifacts/macro/$triggered_by/$teamcity_build/
worker_artifact_uri="$artifact_base_uri"benchmark-infra-worker.jar

$java_cmd -jar $benchmark_infra_scheduler_jar \
  schedule \
  --worker-artifact-uri \
  "$worker_artifact_uri" \
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
  --tool-commit \
  "$neo4j_commit" \
  --tool-branch-owner \
  "$branch_owner" \
  --tool-branch \
  "$neo4j_branch" \
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
  --results-store-user \
  neo4j \
  --results-store-pass-secret-name \
  "$results_store_pass_secret_name" \
  --results-store-uri \
  neo4j://1a20c636.databases.neo4j.io \
  --job-queue \
  "$job_queue" \
  --job-definition \
  "$job_definition" \
  --batch-stack \
  "$batch_stack"