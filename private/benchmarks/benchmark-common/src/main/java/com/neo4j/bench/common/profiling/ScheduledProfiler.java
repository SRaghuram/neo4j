/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.common.model.Benchmark;
import com.neo4j.bench.common.model.BenchmarkGroup;
import com.neo4j.bench.common.model.Parameters;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.results.ForkDirectory;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

public interface ScheduledProfiler
{
    @Retention( RetentionPolicy.RUNTIME )
    @Target( ElementType.METHOD )
    public @interface Schedule
    {
        int rate() default 5;
        TimeUnit timeUnit() default TimeUnit.SECONDS;
    }

    void onSchedule(
                ForkDirectory forkDirectory,
                BenchmarkGroup benchmarkGroup,
                Benchmark benchmark,
                Parameters additionalParameters,
                Pid pid );
}
