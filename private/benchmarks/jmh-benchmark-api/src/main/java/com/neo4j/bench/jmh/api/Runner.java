/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api;

import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.util.BenchmarkGroupBenchmarkMetricsPrinter;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.ErrorReporter;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.jmh.api.config.BenchmarkDescription;
import com.neo4j.bench.jmh.api.config.BenchmarksFinder;
import com.neo4j.bench.jmh.api.config.BenchmarksValidator.BenchmarkValidationResult;
import com.neo4j.bench.jmh.api.config.JmhOptionsUtil;
import com.neo4j.bench.jmh.api.config.SuiteDescription;
import com.neo4j.bench.jmh.api.config.Validation;
import com.neo4j.bench.jmh.api.profile.AbstractMicroProfiler;
import com.neo4j.bench.jmh.api.profile.NoOpProfiler;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.model.model.Metrics;
import com.neo4j.bench.model.model.Neo4jConfig;
import org.apache.commons.lang3.StringUtils;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.neo4j.bench.common.profiling.ProfilerType.NO_OP;
import static com.neo4j.bench.common.util.BenchmarkUtil.durationToString;
import static com.neo4j.bench.jmh.api.config.BenchmarkConfigFile.fromFile;
import static com.neo4j.bench.jmh.api.config.JmhOptionsUtil.baseBuilder;
import static com.neo4j.bench.jmh.api.config.SuiteDescription.fromConfig;
import static com.neo4j.bench.jmh.api.config.Validation.assertValid;
import static java.lang.String.format;

/**
 * This abstract runner defines the execution life-cycle of benchmarks written in JMH.
 * To extend th life-cycle, implementors override call-back methods that are invoked at specific phases of execution.
 * The pseudo-code below explains when each of these call-backs are invoked.
 * <p>
 * prepare()
 * <p>
 * for( benchmark <- benchmarks)
 * beforeProfilerRun(benchmark)
 * // profile
 * afterProfilerRun(benchmark)
 * <p>
 * for( benchmark <- benchmarks)
 * beforeMeasurementRun(benchmark)
 * // measure
 * afterMeasurementRun(benchmark)
 */
public abstract class Runner
{

    private static final Logger LOG = LoggerFactory.getLogger( Runner.class );
    private static final int STAGE_HEADER_WIDTH = 72;

    public static SuiteDescription createSuiteDescriptionFor( String packageName, Path benchConfigFile )
    {
        BenchmarksFinder benchmarksFinder = new BenchmarksFinder( packageName );
        BenchmarkValidationResult benchmarkValidationResult = benchmarksFinder.validate();
        BenchmarkValidationResult.assertValid( benchmarkValidationResult );

        Validation validation = new Validation();
        SuiteDescription defaultBenchmarks = SuiteDescription.fromAnnotations( benchmarksFinder, validation );
        assertValid( validation );

        SuiteDescription finalBenchmarks = (benchConfigFile == null)
                                           // no config file provided, execute defaults
                                           ? defaultBenchmarks
                                           // config file provided, execute exact contents of config file
                                           : fromConfig(
                                                   defaultBenchmarks,
                                                   fromFile( benchConfigFile, validation, benchmarksFinder ),
                                                   validation );
        assertValid( validation );
        return finalBenchmarks;
    }

    /**
     * This method is intended for running individual Benchmark classes from your dev. environment.
     * It therefore honours more JMH annotations than full standalone execution, and also allows selection of only
     * some of the benchmarks within a class.
     */
    public static SuiteDescription createSuiteDescriptionFor( Class<? extends BaseBenchmark> benchmarkClass, String... methods )
    {
        String packageName = benchmarkClass.getPackage().getName();

        BenchmarksFinder benchmarksFinder = new BenchmarksFinder( packageName );
        BenchmarkValidationResult benchmarkValidationResult = benchmarksFinder.validate();
        BenchmarkValidationResult.assertValid( benchmarkValidationResult );

        Validation validation = new Validation();
        // force enable the benchmark, in case it is disabled by default
        BenchmarkDescription benchmark = BenchmarkDescription.of( benchmarkClass, validation, benchmarksFinder ).copyAndEnable();
        assertValid( validation );
        if ( methods != null && methods.length != 0 )
        {
            benchmark = benchmark.copyRetainingMethods( methods );
        }

        return SuiteDescription.fromBenchmarkDescription( benchmark );
    }

    public BenchmarkGroupBenchmarkMetrics run(
            SuiteDescription suiteDescription,
            List<ParameterizedProfiler> profilers,
            String[] jvmArgs,
            int[] threadCounts,
            Path workDir,
            ErrorReporter errorReporter,
            String[] jmhArgs,
            Jvm jvm )
    {
        BenchmarkUtil.assertDirectoryExists( workDir );
        JmhLifecycleTracker.init( workDir );
        RunnerParams runnerParams = runnerParams( RunnerParams.create( workDir ) );

        Instant start = Instant.now();

        try ( Resources resources = new Resources( workDir ) )
        {
            BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics = new BenchmarkGroupBenchmarkMetrics();

            // each benchmark description will represent 1 method and 1 combination of param values, i.e., one benchmark
            // necessary to ensure only specific failing benchmarks are excluded from results, not all for a given class
            List<BenchmarkDescription> enabledExplodedBenchmarks = suiteDescription.explodeEnabledBenchmarks();

            if ( enabledExplodedBenchmarks.isEmpty() )
            {
                throw new RuntimeException( "Expected at least one benchmark description, but found none" );
            }

            List<BenchmarkDescription> benchmarksAfterPrepare = prepare( enabledExplodedBenchmarks, runnerParams, jvm, errorReporter, jvmArgs );

            if ( benchmarksAfterPrepare.isEmpty() )
            {
                LOG.debug( "\n\nNo benchmarks were successfully prepared, no benchmarks can be run\n\n" );
            }
            else
            {
                List<BenchmarkDescription> benchmarksAfterProfiling = profileBenchmarks(
                        benchmarksAfterPrepare,
                        jvm,
                        jvmArgs,
                        jmhArgs,
                        threadCounts,
                        profilers,
                        runnerParams,
                        errorReporter );

                if ( benchmarksAfterProfiling.isEmpty() )
                {
                    LOG.debug( "\n\n No benchmarks were successfully profiled, no benchmarks can be run\n\n" );
                }
                else
                {
                    executeBenchmarks(
                            benchmarkGroupBenchmarkMetrics,
                            benchmarksAfterProfiling,
                            jvm,
                            jvmArgs,
                            jmhArgs,
                            threadCounts,
                            runnerParams,
                            errorReporter );

                    // Print Pretty Results Summary
                    boolean verbose = true;
                    String prettyResultsString = new BenchmarkGroupBenchmarkMetricsPrinter( verbose )
                            .toPrettyString( benchmarkGroupBenchmarkMetrics, errorReporter.errors() );
                    LOG.debug( prettyResultsString );
                }
            }
            Instant finish = Instant.now();

            LOG.debug( format( "Complete benchmark execution took: %s\n", durationToString( Duration.between( start, finish ) ) ) );

            return benchmarkGroupBenchmarkMetrics;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    //------------------------------------- CALLBACKS -------------------------------------

    /**
     * This method is invoked exactly once, at the beginning of benchmark execution, before the profiling phase.
     * The method is allowed to do more-or-less anything it wishes, including running benchmarks.
     * The contract is that it must return the benchmarks that should be executed in later phases.
     * These benchmarks must be a subset of those provided as input parameter.
     *
     * @return benchmarks to execute in later phases
     */
    protected abstract List<BenchmarkDescription> prepare(
            List<BenchmarkDescription> benchmarks,
            RunnerParams runnerParams,
            Jvm jvm,
            ErrorReporter errorReporter,
            String[] jvmArgs );

    /**
     * This method is invoked once for each benchmark that is profiled, before profiling begins.
     * Although there is no restriction on what can be done in this method, its main purpose is to perform any setup required for profiling.
     * E.g., defining additional JMH parameters via the ChainedOptionsBuilder API.
     *
     * @return possibly modified version of the input options builder
     */
    protected abstract ChainedOptionsBuilder beforeProfilerRun( BenchmarkDescription benchmark,
                                                                ProfilerType profilerType,
                                                                RunnerParams runnerParams,
                                                                ChainedOptionsBuilder optionsBuilder );

    /**
     * This method is invoked once for each benchmark that is profiled, after profiling of the benchmark completes.
     */
    protected abstract void afterProfilerRun( BenchmarkDescription benchmark,
                                              ProfilerType profilerType,
                                              RunnerParams runnerParams,
                                              ErrorReporter errorReporter );

    /**
     * This method is invoked once for each benchmark that is executed, before execution begins.
     * Although there is no restriction on what can be done in this method, its main purpose is to perform any setup required for execution.
     * E.g., defining additional JMH parameters via the ChainedOptionsBuilder API.
     *
     * @return possibly modified version of the input options builder
     */
    protected abstract ChainedOptionsBuilder beforeMeasurementRun( BenchmarkDescription benchmark,
                                                                   RunnerParams runnerParams,
                                                                   ChainedOptionsBuilder optionsBuilder );

    /**
     * This method is invoked once for each benchmark that is executed, after execution of the benchmark completes.
     */
    protected abstract void afterMeasurementRun( BenchmarkDescription benchmark,
                                                 RunnerParams runnerParams,
                                                 ErrorReporter errorReporter );

    /**
     * This method is invoked once for each benchmark that was executed, but only after all benchmarks have been executed.
     * Returns the system (not necessarily Neo4j) configuration used to execute the given benchmark.
     * The return value is reported attached to the benchmark results, so it can be reported along with the results.
     *
     * @return the configuration that was used to execute this benchmark
     */
    protected abstract Neo4jConfig systemConfigFor( BenchmarkGroup group, Benchmark benchmark, RunnerParams runnerParams );

    /**
     * Runner parameters is the mechanism by which {@link Runner} implementations can share state (e.g., location of configuration files).
     *
     * @return tool-specific runner parameters
     */
    protected abstract RunnerParams runnerParams( RunnerParams baseRunnerParams );

    private List<BenchmarkDescription> profileBenchmarks(
            Collection<BenchmarkDescription> benchmarks,
            Jvm jvm,
            String[] jvmArgs,
            String[] jmhArgs,
            int[] threadCounts,
            List<ParameterizedProfiler> profilers,
            RunnerParams runnerParams,
            ErrorReporter errorReporter )
    {
        logStageHeader( "PROFILING" );

        List<BenchmarkDescription> benchmarksWithProfiles = new ArrayList<>( benchmarks );

        for ( ParameterizedProfiler profiler : profilers )
        {
            for ( int threadCount : threadCounts )
            {
                for ( BenchmarkDescription benchmark : benchmarks )
                {
                    if ( threadCount == 1 || benchmark.isThreadSafe() )
                    {
                        try
                        {
                            RunnerParams finalRunnerParams = runnerParams.copyWithNewRunId()
                                                                         .copyWithProfilers( Collections.singletonList( profiler ) );

                            Class<? extends AbstractMicroProfiler> microProfiler = AbstractMicroProfiler.toJmhProfiler( profiler.profilerType() );

                            ChainedOptionsBuilder builder = baseBuilder(
                                    finalRunnerParams,
                                    benchmark,
                                    threadCount,
                                    jvm,
                                    jvmArgs );
                            // profile using exactly one profiler
                            builder = builder.addProfiler( microProfiler )
                                             .forks( 1 );
                            // allow Runner implementation to override/enrich JMH configuration
                            builder = beforeProfilerRun( benchmark, profiler.profilerType(), runnerParams, builder );
                            // user provided 'additional' JMH arguments these take precedence over all other configurations. apply then last.
                            builder = JmhOptionsUtil.applyOptions( builder, new CommandLineOptions( jmhArgs ) );
                            Options options = builder.build();
                            // sanity check, make sure provided benchmarks were correctly exploded
                            JmhOptionsUtil.assertExactlyOneBenchmarkIsEnabled( options );

                            // Clear the JMH lifecycle event log for every new execution
                            JmhLifecycleTracker.load( runnerParams.workDir() ).reset();

                            // not interested in results from profiling run, the profiling artifacts will be generated however, and collected later
                            new org.openjdk.jmh.runner.Runner( options ).run();
                        }
                        catch ( Exception e )
                        {
                            // may occur multiple times for same benchmark, once per thread count, this is ok
                            // if benchmark profiling fails at any thread count it will not be executed in next phase
                            benchmarksWithProfiles.remove( benchmark );
                            errorReporter.recordOrThrow( e, benchmark.group(), benchmark.guessSingleName() );
                        }
                        finally
                        {
                            afterProfilerRun( benchmark, profiler.profilerType(), runnerParams, errorReporter );
                        }
                    }
                }
            }
        }
        return benchmarksWithProfiles;
    }

    private void executeBenchmarks(
            BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics,
            Collection<BenchmarkDescription> benchmarks,
            Jvm jvm,
            String[] jvmArgs,
            String[] jmhArgs,
            int[] threadCounts,
            RunnerParams runnerParams,
            ErrorReporter errorReporter )
    {
        logStageHeader( "EXECUTION" );

        RunnerParams finalRunnerParams = runnerParams.copyWithProfilers( ParameterizedProfiler.defaultProfilers( NO_OP ) );
        for ( int threadCount : threadCounts )
        {
            for ( BenchmarkDescription benchmark : benchmarks )
            {
                if ( threadCount == 1 || benchmark.isThreadSafe() )
                {
                    try
                    {
                        ChainedOptionsBuilder builder = baseBuilder(
                                finalRunnerParams.copyWithNewRunId(),
                                benchmark,
                                threadCount,
                                jvm,
                                jvmArgs );
                        // necessary for robust fork directory creation
                        builder = builder.addProfiler( NoOpProfiler.class );
                        // allow Runner implementation to override/enrich JMH configuration
                        builder = beforeMeasurementRun( benchmark, runnerParams, builder );
                        // user provided 'additional' JMH arguments these take precedence over all other configurations. apply then last.
                        builder = JmhOptionsUtil.applyOptions( builder, new CommandLineOptions( jmhArgs ) );
                        Options options = builder.build();
                        // sanity check, make sure provided benchmarks were correctly exploded
                        JmhOptionsUtil.assertExactlyOneBenchmarkIsEnabled( options );

                        executeBenchmarksForConfig( options, benchmarkGroupBenchmarkMetrics, runnerParams );
                    }
                    catch ( Exception e )
                    {
                        errorReporter.recordOrThrow( e, benchmark.group(), benchmark.guessSingleName() );
                    }
                    finally
                    {
                        afterMeasurementRun( benchmark, runnerParams, errorReporter );
                    }
                }
            }
        }
    }

    protected void logStageHeader( String header )
    {
        LOG.info( "\n\n" );
        LOG.info( StringUtils.repeat( '-', STAGE_HEADER_WIDTH ) );
        LOG.info( StringUtils.center( " " + header + " ", STAGE_HEADER_WIDTH, '-' ) );
        LOG.info( StringUtils.repeat( '-', STAGE_HEADER_WIDTH ) );
        LOG.info( "\n\n" );
    }

    private void executeBenchmarksForConfig(
            Options options,
            BenchmarkGroupBenchmarkMetrics metrics,
            RunnerParams runnerParams ) throws RunnerException
    {
        // Clear the JMH lifecycle event log for every new execution
        JmhLifecycleTracker.load( runnerParams.workDir() ).reset();

        for ( RunResult runResult : new org.openjdk.jmh.runner.Runner( options ).run() )
        {
            BenchmarkParams params = runResult.getParams();
            BenchmarkGroup benchmarkGroup = BenchmarkDiscoveryUtils.toBenchmarkGroup( params );
            Benchmarks benchmarks = BenchmarkDiscoveryUtils.toBenchmarks( params, runnerParams );
            Benchmark benchmark = benchmarks.parentBenchmark();

            Neo4jConfig neo4jConfig = systemConfigFor( benchmarkGroup, benchmark, runnerParams );

            if ( !benchmarks.isGroup() )
            {
                Result<?> primaryResult = runResult.getPrimaryResult();
                Metrics parentBenchmarkMetrics = BenchmarkDiscoveryUtils.toMetrics(
                        primaryResult.getStatistics(),
                        params.getTimeUnit() );
                metrics.add(
                        benchmarkGroup,
                        benchmark,
                        parentBenchmarkMetrics,
                        null /*no auxiliary metrics*/,
                        neo4jConfig );
            }
            else
            {
                // Secondary results will be empty for 'symmetric' (non-@Group) benchmarks
                //
                // JMH will return secondary results for both:
                //   (1) ConcurrentReadWriteLabels.readNodesWithLabel:readNodesWithLabel
                //   (2) ConcurrentReadWriteLabels.readNodesWithLabel:readNodesWithLabel·p0.00
                // But only (1) is needed, and only (1) is in Benchmarks.
                // This check simply omits all percentile-specific secondary results.
                // Percentile data is also available in (1).
                runResult.getSecondaryResults().values().stream()
                         .filter( result -> benchmarks.hasChildBenchmarkWith( result.getLabel() ) )
                         .forEach( result ->
                                   {
                                       Benchmark childBenchmark = benchmarks.childBenchmarkWith( result.getLabel() );
                                       Metrics childBenchmarkMetrics = BenchmarkDiscoveryUtils.toMetrics(
                                               result.getStatistics(),
                                               params.getTimeUnit() );
                                       metrics.add(
                                               benchmarkGroup,
                                               childBenchmark,
                                               childBenchmarkMetrics,
                                               null /*no auxiliary metrics*/,
                                               neo4jConfig );
                                   } );
            }
        }
    }
}
