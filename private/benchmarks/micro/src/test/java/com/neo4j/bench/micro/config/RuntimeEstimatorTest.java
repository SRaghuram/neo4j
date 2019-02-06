/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.config;

import com.neo4j.bench.micro.benchmarks.test_only.ValidEnabledBenchmark1;
import com.neo4j.bench.micro.benchmarks.test_only.ValidEnabledBenchmark2;
import org.junit.Test;

import java.time.Duration;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static com.neo4j.bench.micro.config.BenchmarkDescription.of;
import static com.neo4j.bench.micro.config.RuntimeEstimator.DEFAULT_STORE_GENERATION_DURATION;
import static com.neo4j.bench.micro.config.RuntimeEstimator.MISCELLANEOUS_OVERHEAD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class RuntimeEstimatorTest
{
    @Test
    public void shouldEstimateExecutionCountForOneBenchmark1InSingleThreadedMode()
    {
        Validation validation = new Validation();
        List<BenchmarkDescription> benchmarks = newArrayList( of( ValidEnabledBenchmark1.class, validation ) );
        int[] threadCounts = new int[]{1};
        String[] jmhArgs = "-i 5 -wi 5 -r 5 -w 5 -f 3".split( " " );

        int estimatedExecutionCount = RuntimeEstimator.benchmarksExecutionCount( benchmarks, threadCounts );
        assertThat( estimatedExecutionCount, equalTo( 6 ) );

        long expectedStoreGenerationSeconds = (estimatedExecutionCount / threadCounts.length) * DEFAULT_STORE_GENERATION_DURATION.getSeconds();
        Duration estimatedStoreGenerationDuration = RuntimeEstimator.storeGenerationFor( benchmarks );
        assertThat( estimatedStoreGenerationDuration.getSeconds(), equalTo( expectedStoreGenerationSeconds ) );

        long expectedExecutionSeconds = estimatedExecutionCount * 3 * ((5 * 5) + (5 * 5) + MISCELLANEOUS_OVERHEAD.getSeconds());
        Duration estimatedRuntime = RuntimeEstimator.estimatedRuntimeFor( benchmarks, threadCounts, jmhArgs );
        assertThat( estimatedRuntime.getSeconds(), equalTo( expectedExecutionSeconds + expectedStoreGenerationSeconds ) );
    }

    @Test
    public void shouldEstimateExecutionCountForOneBenchmark2InSingleThreadedMode()
    {
        Validation validation = new Validation();
        List<BenchmarkDescription> benchmarks = newArrayList( of( ValidEnabledBenchmark2.class, validation ) );
        int[] threadCounts = new int[]{1};
        String[] jmhArgs = "-i 2 -wi 2 -r 5 -w 5 -f 3".split( " " );

        int estimatedExecutionCount = RuntimeEstimator.benchmarksExecutionCount( benchmarks, threadCounts );
        assertThat( estimatedExecutionCount, equalTo( 1 ) );

        long expectedStoreGenerationSeconds = (estimatedExecutionCount / threadCounts.length) * DEFAULT_STORE_GENERATION_DURATION.getSeconds();
        Duration estimatedStoreGenerationDuration = RuntimeEstimator.storeGenerationFor( benchmarks );
        assertThat( estimatedStoreGenerationDuration.getSeconds(), equalTo( expectedStoreGenerationSeconds ) );

        long expectedExecutionSeconds = estimatedExecutionCount * 3 * ((2 * 5) + (2 * 5) + MISCELLANEOUS_OVERHEAD.getSeconds());
        Duration estimatedRuntime = RuntimeEstimator.estimatedRuntimeFor( benchmarks, threadCounts, jmhArgs );
        assertThat( estimatedRuntime.getSeconds(), equalTo( expectedExecutionSeconds + expectedStoreGenerationSeconds ) );
    }

    @Test
    public void shouldEstimateExecutionCountForOneBenchmark2InMultiThreadedMode()
    {
        Validation validation = new Validation();
        List<BenchmarkDescription> benchmarks = newArrayList( of( ValidEnabledBenchmark2.class, validation ) );
        int[] threadCounts = new int[]{1, 8};
        String[] jmhArgs = "-i 2 -wi 2 -r 5 -w 5 -f 3".split( " " );

        int estimatedExecutionCount = RuntimeEstimator.benchmarksExecutionCount( benchmarks, threadCounts );
        assertThat( estimatedExecutionCount, equalTo( 2 ) );

        long expectedStoreGenerationSeconds = (estimatedExecutionCount / threadCounts.length) * DEFAULT_STORE_GENERATION_DURATION.getSeconds();
        Duration estimatedStoreGenerationDuration = RuntimeEstimator.storeGenerationFor( benchmarks );
        assertThat( estimatedStoreGenerationDuration.getSeconds(), equalTo( expectedStoreGenerationSeconds ) );

        long expectedExecutionSeconds = estimatedExecutionCount * 3 * ((2 * 5) + (2 * 5) + MISCELLANEOUS_OVERHEAD.getSeconds());
        Duration estimatedRuntime = RuntimeEstimator.estimatedRuntimeFor( benchmarks, threadCounts, jmhArgs );
        assertThat( estimatedRuntime.getSeconds(), equalTo( expectedExecutionSeconds + expectedStoreGenerationSeconds ) );
    }

    @Test
    public void shouldEstimateExecutionCountForTwoBenchmarksInSingleThreadedMode()
    {
        Validation validation = new Validation();
        List<BenchmarkDescription> benchmarks = newArrayList(
                of( ValidEnabledBenchmark1.class, validation ), // 6 && not thread safe --> 6
                of( ValidEnabledBenchmark2.class, validation )  // 1 && thread safe     --> 1
        );
        int[] threadCounts = new int[]{1};
        String[] jmhArgs = "-i 2 -wi 2 -r 5 -w 5 -f 3".split( " " );

        int estimatedExecutionCount = RuntimeEstimator.benchmarksExecutionCount( benchmarks, threadCounts );
        assertThat( estimatedExecutionCount, equalTo( 7 ) );

        long expectedStoreGenerationSeconds = (estimatedExecutionCount / threadCounts.length) * DEFAULT_STORE_GENERATION_DURATION.getSeconds();
        Duration estimatedStoreGenerationDuration = RuntimeEstimator.storeGenerationFor( benchmarks );
        assertThat( estimatedStoreGenerationDuration.getSeconds(), equalTo( expectedStoreGenerationSeconds ) );

        long expectedExecutionSeconds = estimatedExecutionCount * 3 * ((2 * 5) + (2 * 5) + MISCELLANEOUS_OVERHEAD.getSeconds());
        Duration estimatedRuntime = RuntimeEstimator.estimatedRuntimeFor( benchmarks, threadCounts, jmhArgs );
        assertThat( estimatedRuntime.getSeconds(), equalTo( expectedExecutionSeconds + expectedStoreGenerationSeconds ) );
    }

    @Test
    public void shouldEstimateExecutionCountForTwoBenchmarksInMultiThreadedMode()
    {
        Validation validation = new Validation();
        List<BenchmarkDescription> benchmarks = newArrayList(
                of( ValidEnabledBenchmark1.class, validation ), // 6 && not thread safe --> 0
                of( ValidEnabledBenchmark2.class, validation )  // 1 && thread safe     --> 1
        );
        int[] threadCounts = new int[]{8};

        int estimatedExecutionCount = RuntimeEstimator.benchmarksExecutionCount( benchmarks, threadCounts );
        assertThat( estimatedExecutionCount, equalTo( 1 ) );
    }

    @Test
    public void shouldEstimateExecutionCountForTwoBenchmarksInMixedThreadedMode()
    {
        Validation validation = new Validation();
        List<BenchmarkDescription> benchmarks = newArrayList(
                of( ValidEnabledBenchmark1.class, validation ), // 6 && not thread safe --> 6 <-- 6 * 1
                of( ValidEnabledBenchmark2.class, validation )  // 1 && thread safe     --> 5 <-- 1 * 5 (thread modes)
        );
        int[] threadCounts = new int[]{1, 2, 3, 4, 5};

        int estimatedExecutionCount = RuntimeEstimator.benchmarksExecutionCount( benchmarks, threadCounts );
        assertThat( estimatedExecutionCount, equalTo( 11 ) );
    }
}
