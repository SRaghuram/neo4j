/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks;

import com.neo4j.bench.common.model.Benchmark;
import com.neo4j.bench.common.model.BenchmarkGroup;
import com.neo4j.bench.common.model.Neo4jConfig;
import com.neo4j.bench.common.profiling.FullBenchmarkName;
import com.neo4j.bench.jmh.api.BaseBenchmark;
import com.neo4j.bench.jmh.api.config.RunnerParams;
import com.neo4j.bench.micro.data.Stores;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.nio.file.Paths;

@State( Scope.Benchmark )
public abstract class BaseRegularBenchmark extends BaseBenchmark
{
    @Param( {} )
    public String baseNeo4jConfig;

    @Override
    protected final void onSetup( BenchmarkGroup group, Benchmark benchmark, RunnerParams runnerParams, BenchmarkParams benchmarkParams ) throws Throwable
    {
        Stores stores = new Stores( runnerParams.workDir() );
        Neo4jConfig neo4jConfig = Neo4jConfig.fromJson( baseNeo4jConfig );

        stores.writeNeo4jConfigForNoStore( neo4jConfig, FullBenchmarkName.from( group, benchmark ) );
        benchmarkSetup( group, benchmark, stores, neo4jConfig );
    }

    @Override
    protected final void onTearDown() throws Throwable
    {
        benchmarkTearDown();
    }

    /**
     * Rather than using the JMH-provided @Setup(Level.Trial), please use this method.
     * In addition to what JMH does, this tool has a Neo4j-specific life-cycle.
     * It is easier to understand how these two life-cycles interact if this method is used instead of @Setup(Level.Trial).
     */
    protected void benchmarkSetup( BenchmarkGroup group, Benchmark benchmark, Stores stores, Neo4jConfig neo4jConfig ) throws Throwable
    {
    }

    /**
     * Rather than using the JMH-provided @TearDown(Level.Trial), please use this method.
     * In addition to what JMH does, this tool has a Neo4j-specific life-cycle.
     * It is easier to understand how these two life-cycles interact if this method is used instead of @TearDown(Level.Trial).
     */
    protected void benchmarkTearDown() throws Throwable
    {
    }
}
