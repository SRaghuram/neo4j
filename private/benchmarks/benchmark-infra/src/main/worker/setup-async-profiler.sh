#!/usr/bin/env bash
# Copyright (c) 2002-2019 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is part of Neo4j internal tooling.
sysctl kernel.perf_event_paranoid=-1
sysctl kernel.kptr_restrict=0
