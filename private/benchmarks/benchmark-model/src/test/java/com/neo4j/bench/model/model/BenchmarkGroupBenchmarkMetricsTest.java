/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.neo4j.bench.model.model.Benchmark.Mode.LATENCY;
import static com.neo4j.bench.model.model.Benchmark.Mode.SINGLE_SHOT;
import static com.neo4j.bench.model.model.Benchmark.Mode.THROUGHPUT;
import static com.neo4j.bench.model.model.Benchmark.benchmarkFor;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BenchmarkGroupBenchmarkMetricsTest
{
    @Test
    public void shouldDoEquality()
    {
        assertThat(
                benchmarkFor( "description", "a", LATENCY, new HashMap<>() ),
                equalTo( benchmarkFor( "description", "a", LATENCY, new HashMap<>() ) ) );
        assertThat(
                benchmarkFor( "description", "a", THROUGHPUT, new HashMap<>() ),
                equalTo( benchmarkFor( "description", "a", THROUGHPUT, new HashMap<>() ) ) );
        assertThat(
                benchmarkFor( "description", "a", LATENCY, new HashMap<>() ),
                equalTo( benchmarkFor( "description", "a", LATENCY, new HashMap<>() ) ) );
        assertThat(
                benchmarkFor(
                        "description",
                        "a",
                        Benchmark.Mode.SINGLE_SHOT,
                        Map.of("k1", "v1", "k2", "v2" ) ),
                equalTo( Benchmark.benchmarkFor(
                        "description",
                        "a",
                        Benchmark.Mode.SINGLE_SHOT,
                        Map.of("k1", "v1", "k2", "v2" ) ) ) );
}

    @Test
    public void shouldDoInequality()
    {
        assertThat(
                benchmarkFor( "description_1", "a", LATENCY, new HashMap<>() ),
                not( equalTo( benchmarkFor( "description_2", "a", LATENCY, new HashMap<>() ) ) ) );
        assertThat(
                benchmarkFor( "description_1", "a", THROUGHPUT, new HashMap<>() ),
                not( equalTo( benchmarkFor( "description_2", "a", THROUGHPUT, new HashMap<>() ) ) ) );
        assertThat(
                benchmarkFor( "description", "a", LATENCY, new HashMap<>() ),
                not( equalTo( benchmarkFor( "description", "ab", LATENCY, new HashMap<>() ) ) ) );
        assertThat(
                benchmarkFor(
                        "description",
                        "a",
                        Benchmark.Mode.SINGLE_SHOT,
                        Map.of("k1", "v1","k2", "v2" ) ),
                not( equalTo( Benchmark.benchmarkFor(
                        "description",
                        "a",
                        Benchmark.Mode.SINGLE_SHOT,
                        Map.of("k1", "v1", "k3", "v3" ) ) ) ) );
    }

    @Test
    // uniqueness is calculating using the following to create composite key: group.name:benchmark.name:metrics.mode
    public void addThrowsWhenAddingMultipleResultsForSameBenchmark()
    {
        BenchmarkGroupBenchmarkMetrics result = new BenchmarkGroupBenchmarkMetrics();

        result.add(
                new BenchmarkGroup( "A" ),
                benchmarkFor( "description", "test1", LATENCY, new HashMap<>() ),
                metrics(),
                auxiliaryMetrics(),
                config() );
        result.add(
                new BenchmarkGroup( "A" ),
                benchmarkFor( "description", "test1", THROUGHPUT, new HashMap<>() ),
                metrics(),
                auxiliaryMetrics(),
                config() );
        result.add(
                new BenchmarkGroup( "A" ),
                benchmarkFor( "description", "test1full", SINGLE_SHOT, new HashMap<>() ),
                metrics(),
                auxiliaryMetrics(),
                config() );
        result.add(
                new BenchmarkGroup( "A" ),
                benchmarkFor( "description", "test2", LATENCY, new HashMap<>() ),
                metrics(),
                auxiliaryMetrics(),
                config() );
        result.add(
                new BenchmarkGroup( "B" ),
                benchmarkFor( "description", "test1", LATENCY, new HashMap<>() ),
                metrics(),
                auxiliaryMetrics(),
                config() );

        assertThrows( IllegalStateException.class, () -> result.add(
                new BenchmarkGroup( "A" ),
                Benchmark.benchmarkFor( "description", "test1", Benchmark.Mode.LATENCY, new HashMap<>() ),
                metrics(),
                auxiliaryMetrics(),
                config() ) );
    }

    private static Metrics metrics()
    {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        Metrics.MetricsUnit unit = Metrics.MetricsUnit.latency( unit() );
        long minNs = rng.nextLong();
        long maxNs = rng.nextLong();
        long meanNs = rng.nextLong();
        long sampleSize = rng.nextLong();
        long percentile25Ns = rng.nextLong();
        long percentile50Ns = rng.nextLong();
        long percentile75Ns = rng.nextLong();
        long percentile90Ns = rng.nextLong();
        long percentile95Ns = rng.nextLong();
        long percentile99Ns = rng.nextLong();
        long percentile999Ns = rng.nextLong();
        return new Metrics(
                unit,
                minNs,
                maxNs,
                meanNs,
                sampleSize,
                percentile25Ns,
                percentile50Ns,
                percentile75Ns,
                percentile90Ns,
                percentile95Ns,
                percentile99Ns,
                percentile999Ns );
    }

    private static Metrics auxiliaryMetrics()
    {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        Metrics.MetricsUnit unit = Metrics.MetricsUnit.rows();
        long minNs = rng.nextLong();
        long maxNs = rng.nextLong();
        long meanNs = rng.nextLong();
        long sampleSize = rng.nextLong();
        long percentile25Ns = rng.nextLong();
        long percentile50Ns = rng.nextLong();
        long percentile75Ns = rng.nextLong();
        long percentile90Ns = rng.nextLong();
        long percentile95Ns = rng.nextLong();
        long percentile99Ns = rng.nextLong();
        long percentile999Ns = rng.nextLong();
        return new Metrics(
                unit,
                minNs,
                maxNs,
                meanNs,
                sampleSize,
                percentile25Ns,
                percentile50Ns,
                percentile75Ns,
                percentile90Ns,
                percentile95Ns,
                percentile99Ns,
                percentile999Ns );
    }

    private static TimeUnit unit()
    {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        TimeUnit[] timeUnits = new TimeUnit[]{SECONDS, MILLISECONDS, MICROSECONDS, NANOSECONDS};
        return timeUnits[rng.nextInt( timeUnits.length )];
    }

    private static Neo4jConfig config()
    {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        return new Neo4jConfig( Map.of( "a", Long.toString( rng.nextLong() ),
                                        "b", Long.toString( rng.nextLong() ),
                                        "c", Long.toString( rng.nextLong() ) ) );
    }
}
