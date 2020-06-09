#!/usr/bin/env bash
set -aux

# this script schedules runs of all macro benchmarks in batch infrastructure

#required arguments

workspace_dir=
branch_owner=
neo4j_version=
neo4j_branch=

while (("$#")); do
  case "$1" in
  --workspace-dir)
    workspace_dir=$2
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
  echo "--neo4j-version is not set" || exit 1
fi

if [[ -z "$neo4j_branch" ]]; then
  echo "--neo4j-branch is not set" || exit 1
fi

if [[ -z "$workspace_dir" ]]; then
  echo "--workspace-dir is not set" || exit 1
fi
if [[ -z "$branch_owner" ]]; then
  echo "--branch-owner is not set" || exit 1
fi

workloads=("accesscontrol M5DLargeJobQueue accesscontrol"
  "bubble_eye M5DLargeJobQueue bubble_eye"
  "cineasts M5DLargeJobQueue cineasts"
  "cineasts_csv M5DLargeJobQueue cineasts_csv"
  "elections M5DLargeJobQueue elections"
  "generatedmusicdata_read M5DLargeJobQueue generatedmusicdata"
  "generatedmusicdata_write M5DLargeJobQueue generatedmusicdata"
  "generated_queries M5D2XLargeJobQueue ldbc_sf001"
  "grid M5DLargeJobQueue grid"
  "index_backed_order_by M5D2XLargeJobQueue pokec"
  "ldbc_sf001 M5D2XLargeJobQueue ldbc_sf001"
  "ldbc_sf010 M5D2XLargeJobQueue ldbc_sf010"
  "levelstory M5DLargeJobQueue levelstory"
  "logistics M5DLargeJobQueue logistics"
  "musicbrainz M5D2XLargeJobQueue musicbrainz"
  "nexlp M5DLargeJobQueue nexlp"
  "pokec_read M5D2XLargeJobQueue pokec"
  "pokec_write M5D2XLargeJobQueue pokec"
  "qmul_read M5DLargeJobQueue qmul"
  "qmul_write M5DLargeJobQueue qmul"
  "recommendations M5DLargeJobQueue recommendations"
  "socialnetwork M5DLargeJobQueue socialnetwork"
  "osmnodes M5D2XLargeJobQueue osmnodes"
  "offshore_leaks M5D2XLargeJobQueue offshore_leaks")

for i in "${workloads[@]}"; do
  workload=($(echo ${i}))
  workload_name=${workload[0]}
  instance_type=${workload[1]}
  db_name=${workload[2]}

  #making copy of workspace
  cp -R "$workspace_dir" "macro-workspace-$workload_name"

  job_definition="macro-benchmark-job-definition-oracle8-production"
  if [[ $instance_type == "M5DLargeJobQueue" ]]; then
    job_definition="macro-benchmark-job-definition-small-oracle8-production
"
  fi

  (
    . schedule-run-macro-benchmark.sh \
      --job-queue "$instance_type" \
      --batch-stack "benchmarking-production" \
      --workspace-dir "macro-workspace-$workload_name" \
      --job-definition "$job_definition" \
      --workload "$workload_name" \
      --db-name "$db_name" \
      --branch-owner "$branch_owner" \
      --neo4j-version "$neo4j_version" \
      --neo4j-branch "$neo4j_branch" &
    sleep 30 #we need this sleep to not saturate your local network when uploading workspaces
  )

done
