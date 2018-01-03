#!/usr/bin/env bash
# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.

include(src/main/distribution/shell-scripts/bin/neo4j-shared.m4)

setup_heap
call_main_class "org.neo4j.commandline.admin.AdminTool" "$@"
