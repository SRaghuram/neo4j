package com.neo4j.bench.micro.profile;

import com.neo4j.bench.client.profiling.ProfilerType;

public class AsyncProfiler extends AbstractMicroProfiler
{
    @Override
    ProfilerType profilerType()
    {
        return ProfilerType.ASYNC;
    }
}
