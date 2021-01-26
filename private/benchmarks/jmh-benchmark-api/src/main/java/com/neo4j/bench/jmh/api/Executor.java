/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api;

import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.util.ErrorReporter;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.jmh.api.config.BenchmarkDescription;
import com.neo4j.bench.jmh.api.config.JmhOptionsUtil;
import com.neo4j.bench.jmh.api.profile.AbstractMicroProfiler;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class Executor
{
    private final Jvm jvm;
    private final String[] jvmArgs;
    private final ErrorReporter errorReporter;
    private final RunnerParams runnerParams;
    private final int[] threadCounts;

    public Executor( Jvm jvm,
                     String[] jvmArgs,
                     ErrorReporter errorReporter,
                     RunnerParams runnerParams,
                     int[] threadCounts )
    {
        this.jvm = jvm;
        this.jvmArgs = jvmArgs;
        this.errorReporter = errorReporter;
        this.runnerParams = runnerParams;
        this.threadCounts = threadCounts;
    }

    public interface AugmentOptions
    {
        ChainedOptionsBuilder augment( ChainedOptionsBuilder builder, BenchmarkDescription benchmark, ParameterizedProfiler profiler );
    }

    public interface AfterRun
    {
        void run( BenchmarkDescription benchmark, ParameterizedProfiler profiler );
    }

    public ExecutionResult runWithProfilers( Collection<BenchmarkDescription> benchmarks,
                                             List<ParameterizedProfiler> profilers,
                                             AugmentOptions augmentOptions,
                                             AfterRun afterRun )
    {
        List<BenchmarkDescription> successfulBenchmarks = new ArrayList<>( benchmarks );
        Collection<RunResult> runResults = new ArrayList<>();

        for ( ParameterizedProfiler profiler : profilers )
        {
            for ( BenchmarkDescription benchmark : benchmarks )
            {
                for ( int threadCount : extractSafeThreadCounts( benchmark ) )
                {
                    try
                    {
                        Collection<RunResult> singleRunResult = runSingle( benchmark, augmentOptions, profiler, threadCount );
                        runResults.addAll( singleRunResult );
                    }
                    catch ( Exception e )
                    {
                        successfulBenchmarks.remove( benchmark );
                        errorReporter.recordOrThrow( e, benchmark.group(), benchmark.guessSingleName() );
                    }
                    finally
                    {
                        afterRun.run( benchmark, profiler );
                    }
                }
            }
        }
        return new ExecutionResult( successfulBenchmarks, runResults );
    }

    private int[] extractSafeThreadCounts( BenchmarkDescription benchmark )
    {
        return Arrays.stream( threadCounts )
                     .filter( threadCount -> threadCount == 1 || benchmark.isThreadSafe() )
                     .toArray();
    }

    private Collection<RunResult> runSingle( BenchmarkDescription benchmark,
                                             AugmentOptions augmentOptions,
                                             ParameterizedProfiler profiler,
                                             int threadCount ) throws RunnerException
    {
        RunnerParams finalRunnerParams = runnerParams.copyWithProfilers( Collections.singletonList( profiler ) )
                                                     .copyWithNewRunId();
        Class<? extends AbstractMicroProfiler> microProfiler = AbstractMicroProfiler.toJmhProfiler( profiler.profilerType() );
        ChainedOptionsBuilder baseBuilder = JmhOptionsUtil.baseBuilder( finalRunnerParams,
                                                                        benchmark,
                                                                        threadCount,
                                                                        jvm,
                                                                        jvmArgs )
                                                          .addProfiler( microProfiler );

        ChainedOptionsBuilder builder = augmentOptions.augment( baseBuilder, benchmark, profiler );

        Options options = builder.build();
        // sanity check, make sure provided benchmarks were correctly exploded
        JmhOptionsUtil.assertExactlyOneBenchmarkIsEnabled( options );

        // Clear the JMH lifecycle event log for every new execution
        JmhLifecycleTracker.load( runnerParams.workDir() ).reset();

        return new Runner( options ).run();
    }

    public static class ExecutionResult
    {
        private final List<BenchmarkDescription> successfulBenchmarks;
        private final Collection<RunResult> runResults;

        public ExecutionResult( List<BenchmarkDescription> successfulBenchmarks, Collection<RunResult> runResults )
        {
            this.successfulBenchmarks = successfulBenchmarks;
            this.runResults = runResults;
        }

        public List<BenchmarkDescription> successfulBenchmarks()
        {
            return successfulBenchmarks;
        }

        public Collection<RunResult> runResults()
        {
            return runResults;
        }

        @Override
        public boolean equals( Object that )
        {
            return EqualsBuilder.reflectionEquals( this, that );
        }

        @Override
        public int hashCode()
        {
            return HashCodeBuilder.reflectionHashCode( this );
        }

        @Override
        public String toString()
        {
            return ToStringBuilder.reflectionToString( this, ToStringStyle.SHORT_PREFIX_STYLE );
        }
    }
}
