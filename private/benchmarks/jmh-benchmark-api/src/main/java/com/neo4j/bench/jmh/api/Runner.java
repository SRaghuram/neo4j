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
import java.util.function.BiConsumer;

import static com.neo4j.bench.client.util.BenchmarkUtil.durationToString;
import static com.neo4j.bench.jmh.api.config.BenchmarkConfigFile.fromFile;
import static com.neo4j.bench.jmh.api.config.JmhOptionsUtil.baseBuilder;
import static com.neo4j.bench.jmh.api.config.RuntimeEstimator.estimatedRuntimeFor;
import static com.neo4j.bench.jmh.api.config.RuntimeEstimator.storeGenerationFor;
import static com.neo4j.bench.jmh.api.config.SuiteDescription.fromConfig;
import static com.neo4j.bench.jmh.api.config.Validation.assertValid;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

abstract class Runner
{
    public static SuiteDescription createSuiteDescriptionFor( String packageName, Path benchConfigFile )
    {
        Annotations annotations = new Annotations( packageName );

        AnnotationsValidationResult annotationsValidationResult = annotations.validate();

        if ( !annotationsValidationResult.isValid() )
        {
            throw new RuntimeException( annotationsValidationResult.message() );
        }

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

        // TODO maybe move that into Validation
        if ( finalBenchmarks.count() == 0 )
        {
            throw new RuntimeException( "No benchmarks were enabled" );
        }
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
        if ( !annotationsValidationResult.isValid() )
        {
            throw new RuntimeException( annotationsValidationResult.message() );
        }

        Validation validation = new Validation();
        // force enable the benchmark, in case it is disabled by default
        BenchmarkDescription benchmark = BenchmarkDescription.of( benchmarkClass, validation, annotations ).copyAndEnable();
        assertValid( validation );
        if ( methods != null && methods.length != 0 )
        {
            benchmark = benchmark.copyRetainingMethods( methods );
        }

        SuiteDescription finalBenchmarks = SuiteDescription.fromBenchmarkDescription( benchmark );

        // TODO maybe move that into Validation
        if ( finalBenchmarks.count() == 0 )
        {
            throw new RuntimeException( "No benchmarks were enabled" );
        }
        return finalBenchmarks;
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
            Path recordingsOutputDir )
    {
        Duration durationEstimate = estimatedRuntimeFor( suiteDescription.benchmarks(), threadCounts, jmhArgs );
        Duration storeGenerationDurationEstimate = storeGenerationFor( suiteDescription.benchmarks() );
        Duration benchmarkDurationEstimateMs = durationEstimate.minus( storeGenerationDurationEstimate );
        System.out.println( format( "ESTIMATED RUNTIME:     %s\n" +
                                    "\t* Store Generation:     %s\n" +
                                    "\t* Benchmark Execution:  %s\n",
                                    durationToString( durationEstimate ),
                                    durationToString( storeGenerationDurationEstimate ),
                                    durationToString( benchmarkDurationEstimateMs ) ) );

        Instant start = Instant.now();

        try
        {
            BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics = new BenchmarkGroupBenchmarkMetrics();

            // build modifier only used in interactive mode, to apply benchmark annotations to JMH configuration
            BiConsumer<ChainedOptionsBuilder,String> builderModifier = ( builder, className ) ->
            {/* do nothing*/};

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

            List<BenchmarkDescription> benchmarksAfterPrepare = prepare( enabledExplodedBenchmarks, workDir, jvm, errorReporter );

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
                        errorReporter,
                        builderModifier );

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
                            errorReporter,
                            builderModifier );

                    if ( !profilerTypes.isEmpty() )
                    {
                        moveProfilerRecordingsTo( recordingsOutputDir, workDir );
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

    // TODO java doc
    protected abstract List<BenchmarkDescription> prepare(
            List<BenchmarkDescription> benchmarks,
            Path workDir,
            Jvm jvm,
            ErrorReporter errorReporter );

    // TODO java doc
    protected abstract ChainedOptionsBuilder beforeProfilerRun( BenchmarkDescription benchmark,
                                                                ProfilerType profilerType,
                                                                Path workDir,
                                                                ErrorReporter errorReporter,
                                                                ChainedOptionsBuilder optionsBuilder );

    // TODO java doc
    protected abstract void afterProfilerRun( BenchmarkDescription benchmark,
                                              ProfilerType profilerType,
                                              Path workDir,
                                              ErrorReporter errorReporter );

    // TODO java doc
    protected abstract ChainedOptionsBuilder beforeMeasurementRun( BenchmarkDescription benchmark,
                                                                   Path workDir,
                                                                   ErrorReporter errorReporter,
                                                                   ChainedOptionsBuilder optionsBuilder );

    // TODO java doc
    protected abstract void afterMeasurementRun( BenchmarkDescription benchmark,
                                                 Path workDir,
                                                 ErrorReporter errorReporter );

    private List<BenchmarkDescription> profileBenchmarks(
            Collection<BenchmarkDescription> benchmarks,
            Jvm jvm,
            String[] jvmArgs,
            String[] jmhArgs,
            int[] threadCounts,
            List<ProfilerType> profilerTypes,
            Path workDir,
            ErrorReporter errorReporter,
            BiConsumer<ChainedOptionsBuilder,String> builderModifier )
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
                            JmhOptionsUtil
                                    .applyOptions( builder, new CommandLineOptions( jmhArgs ) )
                                    .addProfiler( profiler )
                                    .forks( 1 );
                            builderModifier.accept( builder, benchmark.className() );

                            ChainedOptionsBuilder modifiedBuilder = beforeProfilerRun( benchmark, profilerType, workDir, errorReporter, builder );

                            Options options = modifiedBuilder.build();
                            // sanity check, make sure provided benchmarks were correctly exploded
                            assertExactlyOneBenchmarkIsEnabled( options );

                            executeBenchmarksForConfig( options, new BenchmarkGroupBenchmarkMetrics() );
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
            ErrorReporter errorReporter,
            BiConsumer<ChainedOptionsBuilder,String> builderModifier )
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
                        JmhOptionsUtil.applyOptions( builder, new CommandLineOptions( jmhArgs ) );
                        builderModifier.accept( builder, benchmark.className() );

                        ChainedOptionsBuilder modifiedBuilder = beforeMeasurementRun( benchmark, workDir, errorReporter, builder );

                        Options options = modifiedBuilder.build();
                        // sanity check, make sure provided benchmarks were correctly exploded
                        assertExactlyOneBenchmarkIsEnabled( options );

                        executeBenchmarksForConfig( options, benchmarkGroupBenchmarkMetrics );
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

    private static void assertExactlyOneBenchmarkIsEnabled( Options options )
    {
        if ( options.getIncludes().size() != 1 )
        {
            throw new RuntimeException( "Expected one enabled benchmark but found: " + options.getIncludes() );
        }
    }

    private static void executeBenchmarksForConfig(
            Options options,
            BenchmarkGroupBenchmarkMetrics metrics ) throws RunnerException
    {
        for ( RunResult runResult : new org.openjdk.jmh.runner.Runner( options ).run() )
        {
            // Neo4jConfig neo4jConfig = stores.neo4jConfigFor( benchmarkGroup, benchmarks.parentBenchmark() );
            BenchmarkParams params = runResult.getParams();
            BenchmarkGroup benchmarkGroup = BenchmarkDiscoveryUtils.toBenchmarkGroup( params );
            Benchmarks benchmarks = BenchmarkDiscoveryUtils.toBenchmarks( params );

            // TODO Max complains again, maybe we fix it so he less sad
            // TODO invent a means to retrieve benchmark/suite-specific artifacts for each benchmark
            Neo4jConfig neo4jConfig = Neo4jConfig.empty();

            if ( !benchmarks.isGroup() )
            {
                Result<?> primaryResult = runResult.getPrimaryResult();
                Metrics parentBenchmarkMetrics = BenchmarkDiscoveryUtils.toMetrics(
                        primaryResult.getStatistics(),
                        params.getTimeUnit() );
                metrics.add(
                        benchmarkGroup,
                        benchmarks.parentBenchmark(),
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
