#!/usr/bin/env bash
set -ex

declare -a params
while (( "$#" )); do
  case "$1" in
    --workerArtifactUri)
      workerArtifactUri=$2
      shift 2
      ;;
    --) # end argument parsing
      shift
      break
      ;;
#    -*|--*=) # unsupported flags
#      echo "Error: Unsupported flag $1" >&2
#      exit 1
#      ;;
    *) # preserve positional arguments
      params+=("$1")
      shift
      ;;
  esac
done

# download bootstrap jar
aws --region eu-north-1 s3 cp "${workerArtifactUri}" /work/benchmark-worker.jar

cd /work

java "$JAVA_OPTS" -jar benchmark-worker.jar "${params[@]}"
