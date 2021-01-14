/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.test;

import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.BaseDatabaseBenchmark;
import com.neo4j.bench.data.DataGeneratorConfig;
import com.neo4j.bench.data.DataGeneratorConfigBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;

public class NoOpBenchmark extends BaseDatabaseBenchmark
{
    private static final String GROUP_NAME = "groupName";

    // long parameters to verify that filenames don't exceed filesystem limit
    @ParamValues( allowed = {"longParameterValue1-111111111-222222222-333333333-444444444-555555555-666666666"},
            base = {"longParameterValue1-111111111-222222222-333333333-444444444-555555555-666666666"} )
    @Param( {} )
    public String longParameterName1;

    @ParamValues( allowed = {"longParameterValue2-111111111-222222222-333333333-444444444-555555555-666666666"},
            base = {"longParameterValue2-111111111-222222222-333333333-444444444-555555555-666666666"} )
    @Param( {} )
    public String longParameterName2;

    @Override
    public String description()
    {
        return "Does nothing, really";
    }

    @Override
    public String benchmarkGroup()
    {
        return "TestOnly";
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }

    @Override
    protected DataGeneratorConfig getConfig()
    {
        return new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .build();
    }

    @Benchmark
    @Group( GROUP_NAME )
    @BenchmarkMode( {Mode.AverageTime} )
    public void methodRead()
    {
    }

    @Benchmark
    @Group( GROUP_NAME )
    @BenchmarkMode( {Mode.AverageTime} )
    public void methodWrite()
    {
    }
}
