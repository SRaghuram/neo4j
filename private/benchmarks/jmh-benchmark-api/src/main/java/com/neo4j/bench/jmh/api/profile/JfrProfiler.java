/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.profile;

import com.neo4j.bench.common.profiling.ProfilerType;

public class JfrProfiler extends AbstractMicroProfiler
{
    @Override
    ProfilerType profilerType()
    {
        return ProfilerType.JFR;
    }
}
