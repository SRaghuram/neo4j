#!/usr/bin/env bash
# Copyright (c) 2002-2018 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.

include(src/main/distribution/shell-scripts/bin/neo4j-shared.m4)

call_main_class "org.neo4j.tooling.ImportTool" "$@"
