#!/bin/bash
#
# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.
#


set -e
set -u

if [[ $# -eq 0 ]]; then
	echo "no profiling tools dir, call it like this ./install_profilers.sh [installation dir]"
	exit 1
fi

PROFILING_TOOLS_DIR="$1"

if [[ -z "$JAVA_HOME" ]]; then
	echo "JAVA_HOME not set, please configure Java home"
fi

export FLAMEGRAPH_DIR=${PROFILING_TOOLS_DIR}/FlameGraph
export JFR_FLAMEGRAPH_DIR=${PROFILING_TOOLS_DIR}/jfr-flame-graph
export ASYNC_PROFILER_DIR=${PROFILING_TOOLS_DIR}/async-profiler

FLAMEGRAPH_SHA="18c3dea3b2c55ae66768936f1039e36a12b627f6"
JFR_FLAMEGRAPH_DIR_SHA="e14e3d43f23f2ea8ac38b26e46980ba0b784e5d1"

mkdir -p "${PROFILING_TOOLS_DIR}"

echo "installing FlameGraph"
INSTALL_TEMP_DIR=$(mktemp -d)
(
  cd "$INSTALL_TEMP_DIR"
  git clone https://github.com/brendangregg/FlameGraph.git
  cd FlameGraph
  git reset --hard "$FLAMEGRAPH_SHA"
)
mkdir -p "$FLAMEGRAPH_DIR"
cp -R "$INSTALL_TEMP_DIR"/FlameGraph/* "$FLAMEGRAPH_DIR"

echo "installing jfr-flame-graph"
INSTALL_TEMP_DIR=$(mktemp -d)
(
  cd "$INSTALL_TEMP_DIR"
  git clone https://github.com/chrishantha/jfr-flame-graph.git
  cd jfr-flame-graph
  git reset --hard "$JFR_FLAMEGRAPH_DIR_SHA"
  ./gradlew installDist
)
mkdir -p "$JFR_FLAMEGRAPH_DIR"
cp -R "$INSTALL_TEMP_DIR"/jfr-flame-graph/build/install/jfr-flame-graph/* "$JFR_FLAMEGRAPH_DIR"

if [[ ! "$OSTYPE" == *"darwin"* ]]; then
	echo "installing async profiler"
	INSTALL_TEMP_DIR=$(mktemp -d)
	(
	cd "$INSTALL_TEMP_DIR"
	curl --fail --silent --show-error --retry 5 --remote-name --location https://github.com/jvm-profiling-tools/async-profiler/releases/download/v1.8/async-profiler-1.8-linux-x64.tar.gz
	mkdir -p "$ASYNC_PROFILER_DIR"
	tar -C "$ASYNC_PROFILER_DIR" --strip-components=1 -xzf "$INSTALL_TEMP_DIR/async-profiler-1.8-linux-x64.tar.gz"
	cd "$ASYNC_PROFILER_DIR" && make
	)
fi
echo "************************************************************************************"
echo "*all profilers are installed, please add following lines to your shell startup file*"
echo "************************************************************************************"
echo "export FLAMEGRAPH_DIR=${PROFILING_TOOLS_DIR}/FlameGraph"
echo "export JFR_FLAMEGRAPH_DIR=${PROFILING_TOOLS_DIR}/jfr-flame-graph"
echo "export ASYNC_PROFILER_DIR=${PROFILING_TOOLS_DIR}/async-profiler"
