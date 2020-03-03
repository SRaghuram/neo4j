#!/usr/bin/env bash
set -eu

# this script finds ec2 instance id running specific job
# you can use it to log into machine with EC2 instance connect

job_id=${1:-""}

if [ -z "$job_id" ]; then
    echo "provide job id as first argument"
    exit 1
fi

job_queue=$(aws batch describe-jobs --jobs "$job_id" | jq -r ".jobs[].jobQueue")

ecs_container=$(aws batch describe-jobs --jobs "$job_id" | jq -r ".jobs[].container.containerInstanceArn")

compute_environment=$(aws batch describe-job-queues --job-queue "$job_queue" | jq -r ".jobQueues[].computeEnvironmentOrder[].computeEnvironment")

ecsCluster=$(aws batch describe-compute-environments --compute-environment "$compute_environment" | jq -r ".computeEnvironments[].ecsClusterArn")

aws ecs describe-container-instances --cluster "$ecsCluster" --container-instances "$ecs_container" | jq -r ".containerInstances[].ec2InstanceId"