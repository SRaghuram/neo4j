/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api;

import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.common.results.ForkDirectory;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.ThreadParams;

@State( Scope.Benchmark )
public abstract class BaseBenchmark
{
    @Param( {} )
    public String profilers;

    @Param( {} )
    public String workDir;

    @Param( {} )
    public String runId;

    @Param( {} )
    public String runnerParams;

    @Setup
    public final void setUp( BenchmarkParams benchmarkParams ) throws Throwable
    {
        BenchmarkGroup group = BenchmarkDiscoveryUtils.toBenchmarkGroup( benchmarkParams );
        RunnerParams runnerParams = RunnerParams.extractFrom( benchmarkParams );
        Benchmark benchmark = BenchmarkDiscoveryUtils.toBenchmarks( benchmarkParams, runnerParams ).parentBenchmark();

        JmhLifecycleTracker jmhLifecycleTracker = JmhLifecycleTracker.load( runnerParams.workDir() );
        boolean isForking = benchmarkParams.getForks() > 0;
        ForkDirectory forkDirectory = jmhLifecycleTracker.getForkDirectory( runnerParams, isForking, group, benchmark );

        onSetup( group, benchmark, runnerParams, benchmarkParams, forkDirectory );
    }

    @TearDown
    public final void tearDown() throws Throwable
    {
        onTearDown();
    }

    /**
     * Rather than using the JMH-provided @Setup(Level.Trial), please use this method.
     * In addition to what JMH does, this tool has a Neo4j-specific life-cycle.
     * It is easier to understand how these two life-cycles interact if this method is used instead of @Setup(Level.Trial).
     */
    protected void onSetup( BenchmarkGroup group,
                            Benchmark benchmark,
                            RunnerParams runnerParams,
                            BenchmarkParams benchmarkParams,
                            ForkDirectory forkDirectory ) throws Throwable
    {
    }

    /**
     * Rather than using the JMH-provided @TearDown(Level.Trial), please use this method.
     * In addition to what JMH does, this tool has a Neo4j-specific life-cycle.
     * It is easier to understand how these two life-cycles interact if this method is used instead of @TearDown(Level.Trial).
     */
    protected void onTearDown() throws Throwable
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

    /**
     * Calculates Unique Subgroup Thread ID: an ID that can be used to uniquely identify the current thread within the
     * threads executing the same subgroup. ID will be in the range [0..number-of-threads-executing-same-subgroup).
     * <p>
     * There is one @Group per workload execution.
     * Within every group are 'subgroups', defined by the @Benchmark methods in that @Group.
     * Each @Group (workload) is assigned some number of threads, e.g., by BenchmarkRunner.
     * The threads of a @Group are assigned to subgroups according to @GroupThreads, which specifies the thread
     * count ratio between subgroups.
     * <p>
     * Every workload has N @Group instances, where N is dependent on thread count.
     * Number of threads assigned to each @Group instance is equal to the sum of @GroupThreads values of its subgroups.
     * <p>
     * In more detail:
     * <p>
     * Assume, Thread Count: 12
     * <p>
     * For, @Group: GROUP_NAME
     * <p>
     * Group Instance Count (N):
     * ==> Group Instance Thread Count = 6 (1 x createDeleteLabel, 5 x readNodesWithLabel)
     * <p>
     * ==> Group Instance Count = Total Thread Count / Group Instance Thread Count
     * <p>
     * ==> 12 / 6 --> 2
     * <p>
     * Group ID: ID of a Group Instance within all group instances, in this case [0..1]
     * <p>
     * Subgroup: @Benchmark methods in a @Group, e.g., createDeleteLabel & readNodesWithLabel
     * <p>
     * Subgroup Count: Number of @Benchmark methods per @Group, e.g., 2
     * <p>
     * Subgroup Threads: Defined by @GroupThreads, e.g., createDeleteLabel = 1, readNodesWithLabel = 5
     * <p>
     * We want to know the number of threads across all instances of a given subgroup, e.g., readNodesWithLabel.
     * <p>
     * Total Subgroup Thread Count = Subgroup Threads * Group Instances (N)
     * <p>
     * ==> createDeleteLabel = 1 * 2 (N) --> 2
     * <p>
     * ==> readNodesWithLabel = 5 * 2 (N) --> 10
     * <p>
     * Finally, for a given thread, we want to know its unique thread ID from across all Instances of a given subgroup.
     * <p>
     * Useful, e.g., to ensure that only one createDeleteLabel thread modifies any node at one time (avoids deadlocks).
     * <p>
     * As there may be many (Subgroup Count x Group Instance Count) subgroup instances,
     * Subgroup Thread ID is not be unique.
     * <p>
     * ==> Unique Subgroup Thread ID = Subgroup Thread ID + (Group ID * Subgroup Threads)
     * E.g.,
     * <p>
     * * createDeleteLabel (Subgroup Thread ID = 0 [0], Group ID = 0 [0..1])
     * ==> 0 + (0 * 1) --> 0
     * <p>
     * * createDeleteLabel (Subgroup Thread ID = 0 [0], Group ID = 1 [0..1])
     * ==> 0 + (1 * 1) --> 1
     * <p>
     * * readNodesWithLabel (Subgroup Thread ID = 3 [0..4], Group ID = 0 [0..1])
     * ==> 3 + (0 * 5) --> 3
     * <p>
     * * readNodesWithLabel (Subgroup Thread ID = 0 [0..4], Group ID = 1 [0..1])
     * ==> 0 + (1 * 5) --> 5
     */
    protected static int uniqueSubgroupThreadIdFor( ThreadParams params )
    {
        return params.getSubgroupThreadIndex() + (params.getGroupIndex() * params.getSubgroupThreadCount());
    }

    /**
     * Calculates number of threads across all instances of a subgroup.
     */
    protected static int threadCountForSubgroupInstancesOf( ThreadParams params )
    {
        return params.getSubgroupThreadCount() * params.getGroupCount();
    }
}
