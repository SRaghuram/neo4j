/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api;

import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.client.model.BenchmarkGroupBenchmarkMetricsPrinter;
import com.neo4j.bench.client.model.Benchmarks;
import com.neo4j.bench.client.model.Metrics;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.results.BenchmarkGroupDirectory;
import com.neo4j.bench.client.util.BenchmarkUtil;
import com.neo4j.bench.client.util.ErrorReporter;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.jmh.api.config.Annotations;
import com.neo4j.bench.jmh.api.config.AnnotationsValidator.AnnotationsValidationResult;
import com.neo4j.bench.jmh.api.config.BenchmarkDescription;
import com.neo4j.bench.jmh.api.config.JmhOptionsUtil;
import com.neo4j.bench.jmh.api.config.SuiteDescription;
import com.neo4j.bench.jmh.api.config.Validation;
import com.neo4j.bench.jmh.api.profile.AbstractMicroProfiler;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.neo4j.bench.client.util.BenchmarkUtil.durationToString;
import static com.neo4j.bench.jmh.api.config.BenchmarkConfigFile.fromFile;
import static com.neo4j.bench.jmh.api.config.JmhOptionsUtil.baseBuilder;
import static com.neo4j.bench.jmh.api.config.SuiteDescription.fromConfig;
import static com.neo4j.bench.jmh.api.config.Validation.assertValid;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

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
    public static SuiteDescription createSuiteDescriptionFor( String packageName, Path benchConfigFile )
    {
        Annotations annotations = new Annotations( packageName );
        AnnotationsValidationResult annotationsValidationResult = annotations.validate();
        AnnotationsValidationResult.assertValid( annotationsValidationResult );

        Validation validation = new Validation();
        SuiteDescription defaultBenchmarks = SuiteDescription.fromAnnotations( annotations, validation );
        assertValid( validation );

        SuiteDescription finalBenchmarks = (benchConfigFile == null)
                                           // no config file provided, execute defaults
                                           ? defaultBenchmarks
                                           // config file provided, execute exact contents of config file
                                           : fromConfig(
                                                   defaultBenchmarks,
                                                   fromFile( benchConfigFile, validation, annotations ),
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

        Annotations annotations = new Annotations( packageName );
        AnnotationsValidationResult annotationsValidationResult = annotations.validate();
        AnnotationsValidationResult.assertValid( annotationsValidationResult );

        Validation validation = new Validation();
        // force enable the benchmark, in case it is disabled by default
        BenchmarkDescription benchmark = BenchmarkDescription.of( benchmarkClass, validation, annotations ).copyAndEnable();
        assertValid( validation );
        if ( methods != null && methods.length != 0 )
        {
            benchmark = benchmark.copyRetainingMethods( methods );
        }

        return SuiteDescription.fromBenchmarkDescription( benchmark );
    }

    public BenchmarkGroupBenchmarkMetrics run(
            SuiteDescription suiteDescription,
            List<ProfilerType> profilerTypes,
            String[] jvmArgs,
            int[] threadCounts,
            Path workDir,
            ErrorReporter errorReporter,
            String[] jmhArgs,
            Jvm jvm,
            Path profilerRecordingsOutputDir )
    {
        BenchmarkUtil.assertDirectoryExists( workDir );

        Instant start = Instant.now();

        try
        {
            BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics = new BenchmarkGroupBenchmarkMetrics();

            // each benchmark description will represent 1 method and 1 combination of param values, i.e., one benchmark
            // necessary to ensure only specific failing benchmarks are excluded from results, not all for a given class
            List<BenchmarkDescription> enabledExplodedBenchmarks = suiteDescription.benchmarks().stream()
                                                                                   .filter( BenchmarkDescription::isEnabled )
                                                                                   .flatMap( benchmark -> benchmark.explode().stream() )
                                                                                   .collect( toList() );

            if ( enabledExplodedBenchmarks.isEmpty() )
            {
                throw new RuntimeException( "Expected at least one benchmark description, but found none" );
            }

            List<BenchmarkDescription> benchmarksAfterPrepare = prepare( enabledExplodedBenchmarks, workDir, jvm, errorReporter, jvmArgs );

            if ( benchmarksAfterPrepare.isEmpty() )
            {
                System.out.println( "\n\nNo benchmarks were successfully prepared, no benchmarks can be run\n\n" );
            }
            else
            {
                List<BenchmarkDescription> benchmarksWithProfiles = profileBenchmarks(
                        benchmarksAfterPrepare,
                        jvm,
                        jvmArgs,
                        jmhArgs,
                        threadCounts,
                        profilerTypes,
                        workDir,
                        errorReporter );

                if ( benchmarksWithProfiles.isEmpty() )
                {
                    System.out.println( "\n\n No benchmarks were successfully profiled, no benchmarks can be run\n\n" );
                }
                else
                {
                    executeBenchmarks(
                            benchmarkGroupBenchmarkMetrics,
                            benchmarksWithProfiles,
                            jvm,
                            jvmArgs,
                            jmhArgs,
                            threadCounts,
                            workDir,
                            errorReporter );

                    if ( !profilerTypes.isEmpty() )
                    {
                        moveProfilerRecordingsTo( profilerRecordingsOutputDir, workDir );
                    }

                    // Print Pretty Results Summary
                    boolean verbose = true;
                    String prettyResultsString = new BenchmarkGroupBenchmarkMetricsPrinter( verbose )
                            .toPrettyString( benchmarkGroupBenchmarkMetrics, errorReporter.errors() );
                    System.out.println( prettyResultsString );
                }
            }
            Instant finish = Instant.now();

            System.out.println( format( "Complete benchmark execution took: %s\n", durationToString( Duration.between( start, finish ) ) ) );

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
            Path workDir,
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
                                                                Path workDir,
                                                                ChainedOptionsBuilder optionsBuilder );

    /**
     * This method is invoked once for each benchmark that is profiled, after profiling of the benchmark completes.
     */
    protected abstract void afterProfilerRun( BenchmarkDescription benchmark,
                                              ProfilerType profilerType,
                                              Path workDir,
                                              ErrorReporter errorReporter );

    /**
     * This method is invoked once for each benchmark that is executed, before execution begins.
     * Although there is no restriction on what can be done in this method, its main purpose is to perform any setup required for execution.
     * E.g., defining additional JMH parameters via the ChainedOptionsBuilder API.
     *
     * @return possibly modified version of the input options builder
     */
    protected abstract ChainedOptionsBuilder beforeMeasurementRun( BenchmarkDescription benchmark,
                                                                   Path workDir,
                                                                   ChainedOptionsBuilder optionsBuilder );

    /**
     * This method is invoked once for each benchmark that is executed, after execution of the benchmark completes.
     */
    protected abstract void afterMeasurementRun( BenchmarkDescription benchmark,
                                                 Path workDir,
                                                 ErrorReporter errorReporter );

    /**
     * This method is invoked once for each benchmark that was executed, but only after all benchmarks have been executed.
     * Returns the system (not necessarily Neo4j) configuration used to execute the given benchmark.
     * The return value is reported attached to the benchmark results, so it can be reported along with the results.
     *
     * @return the configuration that was used to execute this benchmark
     */
    protected abstract Neo4jConfig systemConfigFor( BenchmarkGroup group, Benchmark benchmark, Path workDir );

    private List<BenchmarkDescription> profileBenchmarks(
            Collection<BenchmarkDescription> benchmarks,
            Jvm jvm,
            String[] jvmArgs,
            String[] jmhArgs,
            int[] threadCounts,
            List<ProfilerType> profilerTypes,
            Path workDir,
            ErrorReporter errorReporter )
    {
        System.out.println( "\n\n" );
        System.out.println( "------------------------------------------------------------------------" );
        System.out.println( "---------------------------  PROFILING ---------------------------------" );
        System.out.println( "------------------------------------------------------------------------" );
        System.out.println( "\n\n" );

        List<BenchmarkDescription> benchmarksWithProfiles = new ArrayList<>( benchmarks );

        for ( ProfilerType profilerType : profilerTypes )
        {
            for ( int threadCount : threadCounts )
            {
                for ( BenchmarkDescription benchmark : benchmarks )
                {
                    if ( threadCount == 1 || benchmark.isThreadSafe() )
                    {
                        try
                        {
                            Class<? extends AbstractMicroProfiler> profiler = AbstractMicroProfiler.toJmhProfiler( profilerType );

                            ChainedOptionsBuilder builder = baseBuilder(
                                    workDir,
                                    benchmark,
                                    threadCount,
                                    jvm,
                                    jvmArgs );
                            // profile using exactly one profiler
                            builder = builder.addProfiler( profiler )
                                             .forks( 1 );
                            // allow Runner implementation to override/enrich JMH configuration
                            builder = beforeProfilerRun( benchmark, profilerType, workDir, builder );
                            // user provided 'additional' JMH arguments these take precedence over all other configurations. apply then last.
                            builder = JmhOptionsUtil.applyOptions( builder, new CommandLineOptions( jmhArgs ) );
                            Options options = builder.build();
                            // sanity check, make sure provided benchmarks were correctly exploded
                            JmhOptionsUtil.assertExactlyOneBenchmarkIsEnabled( options );

                            executeBenchmarksForConfig( options, new BenchmarkGroupBenchmarkMetrics(), workDir );
                        }
                        catch ( Exception e )
                        {
                            // may occur multiple times for same benchmark, once per thread count, this is ok
                            // if benchmark profiling fails at any thread count it will not be executed in next phase
                            benchmarksWithProfiles.remove( benchmark );
                            errorReporter.recordOrThrow( e, benchmark.group(), benchmark.className() );
                        }
                        finally
                        {
                            afterProfilerRun( benchmark, profilerType, workDir, errorReporter );
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
            Path workDir,
            ErrorReporter errorReporter )
    {
        System.out.println( "\n\n" );
        System.out.println( "-------------------------------------------------------------------------" );
        System.out.println( "---------------------------  EXECUTION  ---------------------------------" );
        System.out.println( "-------------------------------------------------------------------------" );
        System.out.println( "\n\n" );

        for ( int threadCount : threadCounts )
        {
            for ( BenchmarkDescription benchmark : benchmarks )
            {
                if ( threadCount == 1 || benchmark.isThreadSafe() )
                {
                    try
                    {
                        ChainedOptionsBuilder builder = baseBuilder(
                                workDir,
                                benchmark,
                                threadCount,
                                jvm,
                                jvmArgs );
                        // allow Runner implementation to override/enrich JMH configuration
                        builder = beforeMeasurementRun( benchmark, workDir, builder );
                        // user provided 'additional' JMH arguments these take precedence over all other configurations. apply then last.
                        builder = JmhOptionsUtil.applyOptions( builder, new CommandLineOptions( jmhArgs ) );
                        Options options = builder.build();
                        // sanity check, make sure provided benchmarks were correctly exploded
                        JmhOptionsUtil.assertExactlyOneBenchmarkIsEnabled( options );

                        executeBenchmarksForConfig( options, benchmarkGroupBenchmarkMetrics, workDir );
                    }
                    catch ( Exception e )
                    {
                        errorReporter.recordOrThrow( e, benchmark.group(), benchmark.className() );
                    }
                    finally
                    {
                        afterMeasurementRun( benchmark, workDir, errorReporter );
                    }
                }
            }
        }
    }

    private void executeBenchmarksForConfig(
            Options options,
            BenchmarkGroupBenchmarkMetrics metrics,
            Path workDir ) throws RunnerException
    {
        for ( RunResult runResult : new org.openjdk.jmh.runner.Runner( options ).run() )
        {
            // Neo4jConfig neo4jConfig = stores.neo4jConfigFor( benchmarkGroup, benchmarks.parentBenchmark() );
            BenchmarkParams params = runResult.getParams();
            BenchmarkGroup benchmarkGroup = BenchmarkDiscoveryUtils.toBenchmarkGroup( params );
            Benchmarks benchmarks = BenchmarkDiscoveryUtils.toBenchmarks( params );
            Benchmark benchmark = benchmarks.parentBenchmark();

            Neo4jConfig neo4jConfig = systemConfigFor( benchmarkGroup, benchmark, workDir );

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
                                               neo4jConfig );
                                   } );
            }
        }
    }

    private static void moveProfilerRecordingsTo( Path profilerRecordingsDir, Path workDir )
    {
        System.out.println( "Moving profile recordings to: " + profilerRecordingsDir.toAbsolutePath() );
        BenchmarkUtil.tryMkDir( profilerRecordingsDir );
        List<BenchmarkGroupDirectory> groupDirs = BenchmarkGroupDirectory.searchAllIn( workDir );
        for ( BenchmarkGroupDirectory groupDir : groupDirs )
        {
            groupDir.copyProfilerRecordings( profilerRecordingsDir );
        }
    }
}
