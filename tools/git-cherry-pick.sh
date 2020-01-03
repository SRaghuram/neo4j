#!/usr/bin/env bash
#
# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.
#
set -e

if [ $# -lt 5 ] ; then
   echo "Expected at least 5 arguments, but got $#"
   echo "usage: $0 mainRemote mainBranch personalRemote personalBranch numberOfCommits"
   exit 1
fi
# We assume that your personal branch is named in the style of "x.x-text"

mainRemote=$1
mainBranch=$2
personalRemote=$3
personalBranch=$4
numberOfCommits=$5

# place current head where you want to cherry-pick
git fetch "${mainRemote}"
git checkout "${mainRemote}"/"${mainBranch}"
git fetch "${personalRemote}"

# Get the suffix of you personal branch, e.g. "4.0-test" => "test"
personalBranchSuffix="${personalBranch:4}"

# Check out a new branch
git checkout -b "${mainBranch}"-"${personalBranchSuffix}"

# Cherry-pick
git cherry-pick "${personalRemote}"/"${personalBranch}"~"${numberOfCommits}".."${personalRemote}"/"${personalBranch}"
