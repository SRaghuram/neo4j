#!/usr/bin/env bash

# debug-support script, to be invoked by JVM on OOM - captures state of process and OS
# required parameters with values:
# --jvm-pid       this is JVM process id
# --output-dir 		a directory where output of commands will be stored
set -ex

declare -a params
while (( "$#" )); do
  case "$1" in
    --jvm-pid)
      jvmPid=$2
      shift 2
      ;;
    --output-dir)
      outputDir=$2
      shift 2
      ;;
    --) # end argument parsing
      shift
      break
      ;;
    *) # preserve positional arguments
      params+=("$1")
      shift
      ;;
  esac
done

resultsDir="$outputDir/"$(uuidgen)

if [[ ! -d $resultsDir ]]; then
  mkdir -p "$resultsDir"
fi

# dump general memory stats
vmstat -s -S M > "$resultsDir/vmstat.out"

# dump process memory stats
pidstat --human  -r -s -u -w -p "$jvmPid" > "$resultsDir/pidstat.out"

# dump native memory tracking summary
jcmd "$jvmPid" VM.native_memory > "$resultsDir/native_memory.out"

# dump process tree
ps fu > "$resultsDir/processes.out"
