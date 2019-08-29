/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

/**
 * Used by {@link ScheduledProfiler} to indicate next invocation of profiler by scheduler.
 *
 */
public interface Tick
{
    long counter();
}
