package com.neo4j.bench.micro.profile;

import com.neo4j.bench.client.profiling.ProfilerType;

public class JfrProfiler extends AbstractMicroProfiler
{
    @Override
    ProfilerType profilerType()
    {
        return ProfilerType.JFR;
    }
}
