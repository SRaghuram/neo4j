#!/bin/bash -e
#
# Copyright (c) 2002-2019 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.
#

# shellcheck disable=SC2154,SC1117
if [ -z "$JFR_FLAMEGRAPH" ] ; then
    echo "JFR_FLAMEGRAPH is not set"
    exit -1
elif [ ! -d "$JFR_FLAMEGRAPH" ] ; then
    echo "JFR_FLAMEGRAPH is not a directory: ${JFR_FLAMEGRAPH}"
    exit -1
elif [ -z "${FLAMEGRAPH_DIR}" ] ; then
    echo "FLAMEGRAPH_DIR is not set"
    exit -1
elif [ ! -d "${FLAMEGRAPH_DIR}" ] ; then
    echo "FLAMEGRAPH_DIR is not a directory: ${FLAMEGRAPH_DIR}"
    exit -1
elif [ -z "$ASYNC_PROFILER_DIR" ] ; then
    echo "ASYNC_PROFILER_DIR is not set"
    exit -1
elif [ ! -d "$ASYNC_PROFILER_DIR" ] ; then
    echo "ASYNC_PROFILER_DIR is not a directory: ${ASYNC_PROFILER_DIR}"
    exit -1
fi

function generateJfrFlameGraphs {
    jfr_dir=$1
    if [ -d "${jfr_dir}" ] ; then
    	echo "Generating JFR flamegraphs for ${jfr_dir}"
        (
            cd "${jfr_dir}"
            abs_dir=$(pwd)
            for jfr in *.jfr ; do
                svg=${jfr/\.jfr/-jfr.svg}
                (
                    cd "${JFR_FLAMEGRAPH}"
# shellcheck disable=SC2154
                    if [ "${do_not_recreate}" = "true" ] && [ -f "${abs_dir}/${svg}" ] ; then
                        echo -e "\tSVG exists: ${svg}"
                    else
                        echo -e "\tConverting: ${abs_dir}/${jfr}"
                        echo -e "\t        To: ${abs_dir}/${svg}"
                        bash create_flamegraph.sh -f "${abs_dir}/${jfr}" -i > "${abs_dir}/${svg}"
                    fi
                )
            done
        )
    else
        echo "No JFR directory: ${jfr_dir}"
    fi
}

function generateAsyncFlameGraphs {
    async_dir=$1
    if [ -d "${async_dir}" ] ; then
    	echo "Generating Async flamegraphs for ${async_dir}"
        (
            cd "${async_dir}"
            abs_dir=$(pwd)
            for async in *.async ; do
                svg=${async/\.async/-async.svg}
                (
                    cd "${FLAMEGRAPH_DIR}"
                    if [ "${do_not_recreate}" = "true" ] && [ -f "${abs_dir}/${svg}" ] ; then
                        echo -e "\tSVG exists: ${svg}"
                    else
                        echo -e "\tConverting: ${abs_dir}/${async}"
                        echo -e "\t        To: ${abs_dir}/${svg}"
                        ./flamegraph.pl --colors=java "${abs_dir}/${async}" > "${abs_dir}/${svg}"
                    fi
                )
            done
        )
    else
        echo "No Async directory: ${async_dir}"
    fi
}

if [ "$(basename "$0")" = "generate-flamegraphs.sh" ] ; then
	if [ "$#" = "0" ] ; then
		echo "No directories provided"
	else
		for dir in "$@" ; do
			if [ -d "$dir" ] ; then
				generateJfrFlameGraphs "${dir}"
				generateAsyncFlameGraphs "${dir}"
			else
				echo "Not a directory: ${dir}"
			fi
		done
	fi
fi
