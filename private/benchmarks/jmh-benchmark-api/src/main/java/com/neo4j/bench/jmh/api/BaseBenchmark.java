/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api;

import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;

@State( Scope.Benchmark )
public abstract class BaseBenchmark
{
    @Param( {} )
    public String workDir;

    @Setup
    public final void setUp( BenchmarkParams params )
    {
        BenchmarkGroup group = BenchmarkDiscoveryUtils.toBenchmarkGroup( params );
        Benchmark benchmark = BenchmarkDiscoveryUtils.toBenchmarks( params ).parentBenchmark();
        benchmarkSetup( group, benchmark );
    }

    @TearDown
    public final void tearDown()
    {
        benchmarkTearDown();
    }

    /**
     * Rather than using the JMH-provided @Setup(Level.Trial), please use this method.
     * In addition to what JMH does, this tool has a Neo4j-specific life-cycle.
     * It is easier to understand how these two life-cycles interact if this method is used instead of @Setup(Level.Trial).
     */
    protected void benchmarkSetup( BenchmarkGroup group, Benchmark benchmark )
    {
    }

    /**
     * Rather than using the JMH-provided @TearDown(Level.Trial), please use this method.
     * In addition to what JMH does, this tool has a Neo4j-specific life-cycle.
     * It is easier to understand how these two life-cycles interact if this method is used instead of @TearDown(Level.Trial).
     */
    protected void benchmarkTearDown()
    {
    }

    /**
     * Benchmark description, should be sufficiently detailed to communicate intention of the benchmark, and concise
     * enough to be displayed in the UI
     *
     * @return name
     */
    public abstract String description();

    /**
     * Name of the benchmark group benchmark class belongs to, e.g., 'core', 'kernel', lock manager'
     *
     * @return name
     */
    public abstract String benchmarkGroup();

    /**
     * Specifies if it is safe to execute this benchmark with more than one thread
     *
     * @return name
     */
    public abstract boolean isThreadSafe();
}
