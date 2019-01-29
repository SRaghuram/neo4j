package com.neo4j.bench.client.model;

import com.neo4j.bench.client.model.Benchmark.Mode;
import org.junit.Test;

import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class BenchmarkGroupBenchmarkMetricsTest
{
    @Test
    public void shouldDoEquality()
    {
        assertThat(
                Benchmark.benchmarkFor( "description", "a", Mode.LATENCY, new HashMap<>() ),
                equalTo( Benchmark.benchmarkFor( "description", "a", Mode.LATENCY, new HashMap<>() ) ) );
        assertThat(
                Benchmark.benchmarkFor( "description", "a", Mode.THROUGHPUT, new HashMap<>() ),
                equalTo( Benchmark.benchmarkFor( "description", "a", Mode.THROUGHPUT, new HashMap<>() ) ) );
        assertThat(
                Benchmark.benchmarkFor( "description", "a", Mode.LATENCY, new HashMap<>() ),
                equalTo( Benchmark.benchmarkFor( "description", "a", Mode.LATENCY, new HashMap<>() ) ) );
        assertThat(
                Benchmark.benchmarkFor(
                        "description",
                        "a",
                        Mode.SINGLE_SHOT,
                        new HashMap<String,String>()
                        {{
                            put( "k1", "v1" );
                            put( "k2", "v2" );
                        }} ),
                equalTo( Benchmark.benchmarkFor(
                        "description",
                        "a",
                        Mode.SINGLE_SHOT,
                        new HashMap<String,String>()
                        {{
                            put( "k1", "v1" );
                            put( "k2", "v2" );
                        }} ) ) );
    }

    @Test
    public void shouldDoInequality()
    {
        assertThat(
                Benchmark.benchmarkFor( "description_1", "a", Mode.LATENCY, new HashMap<>() ),
                not( equalTo( Benchmark.benchmarkFor( "description_2", "a", Mode.LATENCY, new HashMap<>() ) ) ) );
        assertThat(
                Benchmark.benchmarkFor( "description_1", "a", Mode.THROUGHPUT, new HashMap<>() ),
                not( equalTo( Benchmark.benchmarkFor( "description_2", "a", Mode.THROUGHPUT, new HashMap<>() ) ) ) );
        assertThat(
                Benchmark.benchmarkFor( "description", "a", Mode.LATENCY, new HashMap<>() ),
                not( equalTo( Benchmark.benchmarkFor( "description", "ab", Mode.LATENCY, new HashMap<>() ) ) ) );
        assertThat(
                Benchmark.benchmarkFor(
                        "description",
                        "a",
                        Mode.SINGLE_SHOT,
                        new HashMap<String,String>()
                        {{
                            put( "k1", "v1" );
                            put( "k2", "v2" );
                        }} ),
                not( equalTo( Benchmark.benchmarkFor(
                        "description",
                        "a",
                        Mode.SINGLE_SHOT,
                        new HashMap<String,String>()
                        {{
                            put( "k1", "v1" );
                            put( "k3", "v3" );
                        }} ) ) ) );
    }

    @Test
    // uniqueness is calculating using the following to create composite key: group.name:benchmark.name:metrics.mode
    public void addThrowsWhenAddingMultipleResultsForSameBenchmark()
    {
        BenchmarkGroupBenchmarkMetrics result = new BenchmarkGroupBenchmarkMetrics();

        result.add(
                new BenchmarkGroup( "A" ),
                Benchmark.benchmarkFor( "description", "test1", Mode.LATENCY, new HashMap<>() ),
                metrics(),
                config() );
        result.add(
                new BenchmarkGroup( "A" ),
                Benchmark.benchmarkFor( "description", "test1", Mode.THROUGHPUT, new HashMap<>() ),
                metrics(),
                config() );
        result.add(
                new BenchmarkGroup( "A" ),
                Benchmark.benchmarkFor( "description", "test1full", Mode.SINGLE_SHOT, new HashMap<>() ),
                metrics(),
                config() );
        result.add(
                new BenchmarkGroup( "A" ),
                Benchmark.benchmarkFor( "description", "test2", Mode.LATENCY, new HashMap<>() ),
                metrics(),
                config() );
        result.add(
                new BenchmarkGroup( "B" ),
                Benchmark.benchmarkFor( "description", "test1", Mode.LATENCY, new HashMap<>() ),
                metrics(),
                config() );
        try
        {
            result.add(
                    new BenchmarkGroup( "A" ),
                    Benchmark.benchmarkFor( "description", "test1", Mode.LATENCY, new HashMap<>() ),
                    metrics(),
                    config() );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    private static Metrics metrics()
    {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        TimeUnit unit = unit();
        long minNs = rng.nextLong();
        long maxNs = rng.nextLong();
        long meanNs = rng.nextLong();
        long errorNs = rng.nextLong();
        double errorConfidence = rng.nextDouble();
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
                errorNs,
                errorConfidence,
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
        return new Neo4jConfig(
                new HashMap<String,String>()
                {{
                    put( "a", Long.toString( rng.nextLong() ) );
                    put( "b", Long.toString( rng.nextLong() ) );
                    put( "c", Long.toString( rng.nextLong() ) );
                }} );
    }
}
