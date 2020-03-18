#!/usr/bin/env bash
#
# this script builds and deploys internal benchmarking artifacts
#

set -eux

version=

while (("$#")); do
  case "$1" in
  --version)
    version=$2
    shift 2
    ;;
  --) # end of argument parsing
    shift
    break
    ;;
  esac
done

if [[ -z $version ]]; then
  version=$(mvn -q help:evaluate -Dexpression="benchmarks.version" -DforceStdout -pl :benchmarks)
fi

if [[ -z $version ]]; then
  echo "benchmarks.version property not set in benchmarks POM or not set from command line"
  exit 1
fi

settings_file=$(realpath "private/benchmarks/settings.xml")

# list of directories we want to deploy from
declare -a dirs=("benchmark-common" "benchmark-results-client" "benchmark-infra/benchmark-infra-common" "jmh-benchmark-api" "benchmark-infra/benchmark-infra-worker" "benchmark-infra/benchmark-infra-scheduler")

# get modules names
modules_names=()
for module_dir in "${dirs[@]}"; do
  modules_names+=(":$(cd "private/benchmarks/$module_dir" && mvn -q help:evaluate -Dexpression="project.artifactId" -DforceStdout)")
done

modules_list=$(IFS=","; echo "${modules_names[*]}")

# set versions, we need to call it on product root, otherwise things will not compile
mvn versions:set -DnewVersion="$version" -DprocessAllModules=true

# compile and package, only the things we need
mvn clean install -Dcheckstyle.skip -Drevapi.skip -DskipTests -Dlicensing.skip -Dlicense.skip -TC2 -pl \
  "$modules_list" -am

# flatten POM structure
mvn org.codehaus.mojo:flatten-maven-plugin:flatten -pl "$modules_list"

# deploy all modules individually
for module_dir in "${dirs[@]}"; do
  (
    cd "private/benchmarks/${module_dir}"
    module_name=$(basename "$module_dir")
    artifact_file="target/$module_name-$version.jar"
    mvn --settings "$settings_file" deploy:deploy-file -Dfile="$artifact_file" -DpomFile=".flattened-pom.xml" -Durl="https://neo.jfrog.io/neo/benchmarking-local" -DrepositoryId="neo4j-internal-releases"
  )
done
