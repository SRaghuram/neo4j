#!/usr/bin/env bash
set -e

while IFS= read -r -d '' file
do
  echo "${file}"
  shellcheck "${file}"
done <   <(find . -type f -name "*.sh" -print0  )
