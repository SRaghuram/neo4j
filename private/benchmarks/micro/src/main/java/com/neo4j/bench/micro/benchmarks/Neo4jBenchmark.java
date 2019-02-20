/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks;

import org.openjdk.jmh.infra.ThreadParams;

public interface Neo4jBenchmark
{
    /**
     * Benchmark description, should be sufficiently detailed to communicate intention of the benchmark, and concise
     * enough to be displayed in the UI
     *
     * @return name
     */
    String description();

    /**
     * Name of the benchmark group benchmark class belongs to, e.g., 'core', 'kernel', lock manager'
     *
     * @return name
     */
    String benchmarkGroup();

    /**
     * Specifies if it is safe to execute this benchmark with more than one thread
     *
     * @return name
     */
    boolean isThreadSafe();

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
    static int uniqueSubgroupThreadIdFor( ThreadParams params )
    {
        return params.getSubgroupThreadIndex() + (params.getGroupIndex() * params.getSubgroupThreadCount());
    }

    /**
     * Calculates number of threads across all instances of a subgroup.
     */
    static int threadCountForSubgroupInstancesOf( ThreadParams params )
    {
        return params.getSubgroupThreadCount() * params.getGroupCount();
    }
}
