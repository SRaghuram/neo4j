#!/usr/bin/env bash
#
# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.
#

set -e
aws --region eu-west-1 cloudformation validate-template --template-body "$(cat benchmark-infra/src/main/stack/aws-batch-formation.json)"
