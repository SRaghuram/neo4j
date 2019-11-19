#!/bin/bash

[ $# -ne 2 ] && { echo "Usage: $0 base-branch pr-number"; exit 1; }

BASE_BRANCH=$1
PR=$2

# checkout the correct branch
git checkout $BASE_BRANCH
git checkout -b "$BASE_BRANCH-import-pr-$PR"

# apply PR
wget -q -O - "https://patch-diff.githubusercontent.com/raw/neo4j/neo4j/pull/$PR.patch" |  sed "/^---\$/i Closes \$$PR" | git am --signof -3 --directory=public

