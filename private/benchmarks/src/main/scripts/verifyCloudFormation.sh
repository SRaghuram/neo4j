#!/usr/bin/env bash
#
# Copyright (c) 2002-2019 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.
#

set -e
aws cloudformation validate-template --template-body "$(cat benchmark-infra/src/main/stack/aws-batch-formation.json)"