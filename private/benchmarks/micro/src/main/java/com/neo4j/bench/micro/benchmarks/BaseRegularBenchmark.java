/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks;

import com.neo4j.bench.micro.JMHResultUtil;
import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.profiling.FullBenchmarkName;
import com.neo4j.bench.micro.data.Stores;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.nio.file.Paths;

@State( Scope.Benchmark )
public abstract class BaseRegularBenchmark implements Neo4jBenchmark
{
    @Param( {} )
    public String baseNeo4jConfig;

    @Param( {} )
    public String storesDir;

    @Setup
    public final void setUp( BenchmarkParams params ) throws Throwable
    {
        Stores stores = new Stores( Paths.get( storesDir ) );
        Neo4jConfig neo4jConfig = Neo4jConfig.fromJson( baseNeo4jConfig );
        BenchmarkGroup group = JMHResultUtil.toBenchmarkGroup( params );
        Benchmark benchmark = JMHResultUtil.toBenchmarks( params ).parentBenchmark();

        stores.writeNeo4jConfigForNoStore( neo4jConfig, FullBenchmarkName.from( group, benchmark ) );
        benchmarkSetup( group, benchmark, stores, neo4jConfig );
    }

    @TearDown
    public final void tearDown() throws Throwable
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
