package com.neo4j.bench.micro.config;

import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.micro.data.Stores;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;

import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.neo4j.bench.micro.config.JmhOptionsUtil.applyOptions;
import static com.neo4j.bench.micro.config.JmhOptionsUtil.baseBuilder;

import static java.util.Collections.singletonList;

public class RuntimeEstimator
{
    public static Duration estimatedRuntimeFor(
            Collection<BenchmarkDescription> benchmarks,
            int[] threadCounts,
            String[] jmhArgs )
    {
        return Duration.of( totalMsFor( benchmarks, threadCounts, jmhArgs ), ChronoUnit.MILLIS );
    }

    /**
     * Estimates the duration of a complete benchmark execution, with a few simplifying assumptions:
     * <pre>
     * (1) Every benchmark runs at every thread count. Not strictly true. A few are single threaded only.
     * (2) Time spent on data generation is ignored.
     * </pre>
     *
     * @return Duration in milliseconds
     */
    private static long totalMsFor(
            Collection<BenchmarkDescription> benchmarks,
            int[] threadCounts,
            String[] jmhArgs )
    {
        Stores dummyStores = new Stores( Paths.get( "." ) );
        Jvm jvm = Jvm.defaultJvm();
        return benchmarks.stream()
                         .filter( BenchmarkDescription::isEnabled )
                         // --- IMPORTANT ---
                         // Explode ensures every benchmark represents exactly one benchmark (single parameter combination, single method).
                         // This gets around the JMH limitation of having a single namespace for all benchmark param names.
                         // E.g. two benchmarks named DuplicateBenchmark can be run in the same build, but JMH will never see them both together
                         .flatMap( benchmark -> benchmark.explode().stream() )
                         .mapToLong( b ->
                                     {
                                         ChainedOptionsBuilder builder = baseBuilder( "", dummyStores, b, 1, jvm, "" );
                                         try
                                         {
                                             applyOptions( builder, new CommandLineOptions( jmhArgs ) );
                                         }
                                         catch ( CommandLineOptionException e )
                                         {
                                             throw new RuntimeException( "Error parsing JMH CLI options", e );
                                         }
                                         Options jmhOptions = builder.build();
                                         return totalMsFor(
                                                 singletonList( b ),
                                                 threadCounts,
                                                 jmhOptions.getForkCount().get(),
                                                 jmhOptions.getWarmupIterations().get(),
                                                 jmhOptions.getWarmupTime().get().convertTo( TimeUnit.SECONDS ),
                                                 jmhOptions.getMeasurementIterations().get(),
                                                 jmhOptions.getMeasurementTime().get().convertTo( TimeUnit.SECONDS ) );
                                     } )
                         .sum();
    }

    /**
     * Estimates the duration of a complete benchmark execution, with a few simplifying assumptions:
     * <pre>
     * (1) Every benchmark runs at every thread count. Not strictly true. A few are single threaded only.
     * (2) Time spent on data generation is ignored.
     * </pre>
     *
     * @return Duration in milliseconds
     */
    private static long totalMsFor(
            Collection<BenchmarkDescription> benchmarks,
            int[] threadCounts,
            int forks,
            int warmupIterationCount,
            long warmupIterationSeconds,
            int measureIterationCount,
            long measureIterationSeconds )
    {
        return
                // all database stores are generated once, before benchmarking begins
                //  * assumes every store takes same amount of time
                storeGenerationFor( benchmarks ).toMillis() +
                // number of unique benchmarks to run
                // includes every parameter/mode/thread combination of every benchmark method on every benchmark class
                benchmarksExecutionCount( benchmarks, threadCounts ) *
                // each benchmark execution consists of multiple different forked processes, run one after the other
                forks *
                // each fork executes a series of warmup iterations and then a series of measurement iterations
                (
                        // time to execute all warmup iterations
                        (warmupIterationCount * TimeUnit.SECONDS.toMillis( warmupIterationSeconds )) +
                        // time to execute all measurement iterations
                        (measureIterationCount * TimeUnit.SECONDS.toMillis( measureIterationSeconds )) +
                        // time to perform miscellaneous tasks, e.g., store copy between forks
                        // current value of 3 seconds is simply based on observations from TeamCity build logs
                        MISCELLANEOUS_OVERHEAD.toMillis()
                );
    }

    static int benchmarksExecutionCount( Collection<BenchmarkDescription> benchmarks, int[] threadCounts )
    {
        return benchmarks.stream()
                         .filter( BenchmarkDescription::isEnabled )
                         .mapToInt( benchmark -> benchmarkExecutionCount( benchmark, threadCounts ) )
                         .sum();
    }

    private static int benchmarkExecutionCount( BenchmarkDescription benchmark, int[] threadCounts )
    {
        return Arrays.stream( threadCounts )
                     .map( benchmark::executionCount )
                     .sum();
    }

    // time to perform miscellaneous tasks, e.g., store copy between forks
    // current value of 3 seconds is simply based on observations from TeamCity build logs
    public static Duration MISCELLANEOUS_OVERHEAD = Duration.of( 3, ChronoUnit.SECONDS );
    public static Duration DEFAULT_STORE_GENERATION_DURATION = Duration.of( 10, ChronoUnit.SECONDS );

    public static Duration storeGenerationFor( Collection<BenchmarkDescription> benchmarks )
    {
        return Duration.of(
                benchmarksExecutionCount( benchmarks, new int[]{1} ) * DEFAULT_STORE_GENERATION_DURATION.toMillis(),
                ChronoUnit.MILLIS );
    }
}
