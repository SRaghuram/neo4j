/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.assist;

import com.neo4j.bench.model.model.Parameters;

import java.util.List;

public class ProfilerPidMappings
{

    private final Parameters ownParameters;
    private final List<ProfilerPidMapping> profilerPidMappings;

    public ProfilerPidMappings( Parameters ownParameters, List<ProfilerPidMapping> profilerPidMappings )
    {
        this.ownParameters = ownParameters;
        this.profilerPidMappings = profilerPidMappings;
    }

    Parameters ownParameters()
    {
        return ownParameters;
    }

    List<ProfilerPidMapping> profilerPidMappings()
    {
        return profilerPidMappings;
    }
}
