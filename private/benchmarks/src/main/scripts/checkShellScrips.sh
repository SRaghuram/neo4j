#!/usr/bin/env bash
#
# Copyright (c) 2002-2019 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.
#

set -e

while IFS= read -r -d '' file
do
  echo "${file}"
  shellcheck "${file}"
done <   <(find . -type f -name "*.sh" -print0  )
