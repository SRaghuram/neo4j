/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.assist;

import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.profiling.InternalProfiler;
import com.neo4j.bench.model.model.Parameters;

import java.util.List;

public class ProfilerPidMapping
{
    private final Pid pid;
    private final Parameters parameters;
    private final List<InternalProfiler> profilers;

    public ProfilerPidMapping( Pid pid, Parameters parameters, List<InternalProfiler> profilers )
    {
        this.pid = pid;
        this.parameters = parameters;
        this.profilers = profilers;
    }

    Pid pid()
    {
        return pid;
    }

    Parameters parameters()
    {
        return parameters;
    }

    List<InternalProfiler> profilers()
    {
        return profilers;
    }
}
