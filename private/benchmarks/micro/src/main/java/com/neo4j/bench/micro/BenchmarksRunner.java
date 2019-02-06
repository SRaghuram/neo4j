/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.client.ClientUtil;
import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkConfig;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.client.model.BenchmarkTool;
import com.neo4j.bench.client.model.Benchmarks;
import com.neo4j.bench.client.model.Environment;
import com.neo4j.bench.client.model.Java;
import com.neo4j.bench.client.model.Metrics;
import com.neo4j.bench.client.model.Neo4j;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.model.Repository;
import com.neo4j.bench.client.model.TestRun;
import com.neo4j.bench.client.model.TestRunReport;
import com.neo4j.bench.client.util.BenchmarkUtil;
import com.neo4j.bench.client.util.ErrorReporter;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.client.util.ErrorReporter.ErrorPolicy;
import com.neo4j.bench.micro.config.Annotations;
import com.neo4j.bench.micro.config.BenchmarkDescription;
import com.neo4j.bench.micro.config.JmhOptionsUtil;
import com.neo4j.bench.micro.config.Neo4jArchive;
import com.neo4j.bench.micro.config.SuiteDescription;
import com.neo4j.bench.micro.config.Validation;
import com.neo4j.bench.micro.data.Stores;
import com.neo4j.bench.micro.profile.ProfileDescriptor;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.profile.Profiler;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.TimeValue;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.neo4j.bench.client.util.Args.concatArgs;
import static com.neo4j.bench.client.util.BenchmarkUtil.durationToString;
import static com.neo4j.bench.micro.config.Annotations.benchmarkClassForName;
import static com.neo4j.bench.micro.config.BenchmarkConfigFile.fromFile;
import static com.neo4j.bench.micro.config.JmhOptionsUtil.applyAnnotations;
import static com.neo4j.bench.micro.config.JmhOptionsUtil.baseBuilder;
import static com.neo4j.bench.micro.config.RuntimeEstimator.estimatedRuntimeFor;
import static com.neo4j.bench.micro.config.RuntimeEstimator.storeGenerationFor;
import static com.neo4j.bench.micro.config.SuiteDescription.fromConfig;
import static com.neo4j.bench.micro.config.Validation.assertValid;

import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

class BenchmarksRunner
{
    public TestRunReport run(
            File neo4jConfigFile,
            Neo4j neo4j,
            String toolCommit,
            String toolOwner,
            String toolBranch,
            long build,
            long parentBuild,
            String[] additionalJvmArgs,
            File neo4jPackageForJvmArgs,
            File benchConfigFile,
            int[] threadCounts,
            ProfileDescriptor profileDescriptor,
            String[] jmhArgs,
            Path storesDir,
            ErrorPolicy errorPolicy,
            Jvm jvm,
            String triggeredBy )
    {
        System.out.println( format( "\n-- Running With --\n" +
                                    "\tNeo4j Config:              %s\n" +
                                    "\tNeo4j Commit:              %s\n" +
                                    "\tNeo4j Version:             %s\n" +
                                    "\tNeo4j Edition:             %s\n" +
                                    "\tNeo4j Branch:              %s\n" +
                                    "\tNeo4j Branch Owner:        %s\n" +
                                    "\tTool Commit:               %s\n" +
                                    "\tTeamCity Build ID:         %s\n" +
                                    "\tTeamCity Parent Build ID:  %s\n" +
                                    "\tNeo4j Package:             %s\n" +
                                    "\tJVM Args:                  %s\n" +
                                    "\tThread Counts:             %s\n" +
                                    "\tBenchmark Config:          %s\n" +
                                    "\tProfile:                   %s\n" +
                                    "\tJMH Args:                  %s\n" +
                                    "\tStore Directory:           %s\n" +
                                    "\tError Policy:              %s\n" +
                                    "\tJVM:                       %s",
                                    (null == neo4jConfigFile) ? null : neo4jConfigFile.getAbsolutePath(),
                                    neo4j.commit(),
                                    neo4j.version(),
                                    neo4j.edition(),
                                    neo4j.branch(),
                                    neo4j.owner(),
                                    toolCommit,
                                    build,
                                    parentBuild,
                                    (null == neo4jPackageForJvmArgs) ? null : neo4jPackageForJvmArgs.getAbsolutePath(),
                                    Arrays.toString( additionalJvmArgs ),
                                    Arrays.toString( threadCounts ),
                                    (null == benchConfigFile) ? null : benchConfigFile.getAbsolutePath(),
                                    profileDescriptor,
                                    Arrays.toString( jmhArgs ),
                                    storesDir.toFile().getAbsolutePath(),
                                    errorPolicy,
                                    jvm.launchJava() ) );

        // TODO uncomment once we pass in the correct neo4j.conf
//        if ( neo4jConfigFile != null )
//        {
//            BenchmarkUtil.assertFileNotEmpty( neo4jConfigFile.toPath() );
//        }

        Annotations.AnnotationsValidationResult annotationsValidationResult = new Annotations().validate();
        if ( !annotationsValidationResult.isValid() )
        {
            throw new RuntimeException( annotationsValidationResult.message() );
        }

        Validation validation = new Validation();
        Validation.assertValid( validation );
        SuiteDescription defaultBenchmarks = SuiteDescription.byReflection( validation );
        Validation.assertValid( validation );

        SuiteDescription finalBenchmarks = (benchConfigFile == null)
                                           // no config file provided, execute defaults
                                           ? defaultBenchmarks
                                           // config file provided, execute exact contents of config file
                                           : fromConfig(
                                                   defaultBenchmarks,
                                                   fromFile( benchConfigFile.toPath(), validation ),
                                                   validation );
        assertValid( validation );
        if ( finalBenchmarks.count() == 0 )
        {
            throw new RuntimeException( "No benchmarks were enabled" );
        }

        Neo4jConfig neo4jConfigDef = (neo4jConfigFile == null)
                ? Neo4jConfig.withDefaults()
                : Neo4jConfig.withDefaults().mergeWith( Neo4jConfig.fromFile( neo4jConfigFile ) );

        String[] defaultJvmArgs = (null == neo4jPackageForJvmArgs)
                                  ? new String[0]
                                  : Neo4jArchive.extractJvmArgsFrom( neo4jPackageForJvmArgs );
        String[] jvmArgs = concatArgs( additionalJvmArgs, defaultJvmArgs );

        Duration durationEstimate = estimatedRuntimeFor( finalBenchmarks.benchmarks(), threadCounts, jmhArgs );
        Duration storeGenerationDurationEstimate = storeGenerationFor( finalBenchmarks.benchmarks() );
        Duration benchmarkDurationEstimateMs = durationEstimate.minus( storeGenerationDurationEstimate );
        System.out.println( format( "\tExtracted JVM Args:    %s\n" +
                                    "\tAll JVM Args:          %s\n" +
                                    "\t--------------------------------------------------------\n" +
                                    "\tESTIMATED RUNTIME:     %s\n" +
                                    "\t\t* Store Generation:     %s\n" +
                                    "\t\t* Benchmark Execution:  %s\n" +
                                    "\t--------------------------------------------------------",
                                    Arrays.toString( defaultJvmArgs ),
                                    Stream.of( jvmArgs ).collect( Collectors.joining( " " ) ),
                                    durationToString( durationEstimate ),
                                    durationToString( storeGenerationDurationEstimate ),
                                    durationToString( benchmarkDurationEstimateMs ) ) );

        Stores stores = new Stores( storesDir );

        Instant start = Instant.now();

        try
        {
            stores.deleteStoresDir();
            ErrorReporter errorReporter = new ErrorReporter( errorPolicy );
            String testRunId = ClientUtil.generateUniqueId();
            BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics = new BenchmarkGroupBenchmarkMetrics();
            boolean generateStoresInFork = true;

            // build modifier only used in interactive mode, to apply benchmark annotations to JMH configuration
            BiConsumer<ChainedOptionsBuilder,String> builderModifier = ( builder, className ) ->
            {/* do nothing*/};

            runBenchmarks(
                    benchmarkGroupBenchmarkMetrics,
                    finalBenchmarks.benchmarks(),
                    profileDescriptor,
                    neo4jConfigDef,
                    threadCounts,
                    jvm,
                    jvmArgs,
                    jmhArgs,
                    stores,
                    generateStoresInFork,
                    errorReporter,
                    builderModifier );

            // Print details about error in benchmark measurements
            System.out.println( benchmarkGroupBenchmarkMetrics.errorDetails() );

            Instant finish = Instant.now();

            System.out.println( format( "Complete benchmark execution took: %s\n",
                                        durationToString( Duration.between( start, finish ) ) ) );

            TestRun testRun = new TestRun(
                    testRunId,
                    Duration.between( start, finish ).toMillis(),
                    start.toEpochMilli(),
                    build,
                    parentBuild,
                    triggeredBy );
            BenchmarkConfig benchmarkConfig = finalBenchmarks.toBenchmarkConfig();
            Neo4jConfig neo4jConfig = (null == neo4jConfigFile) ? Neo4jConfig.empty()
                                                                : Neo4jConfig.fromFile( neo4jConfigFile );
            BenchmarkTool tool = new BenchmarkTool( Repository.MICRO_BENCH, toolCommit, toolOwner, toolBranch );
            Java java = Java.current( Stream.of( jvmArgs ).collect( joining( " " ) ) );

            return new TestRunReport(
                    testRun,
                    benchmarkConfig,
                    Sets.newHashSet( neo4j ),
                    neo4jConfig,
                    Environment.current(),
                    benchmarkGroupBenchmarkMetrics,
                    tool,
                    java,
                    Lists.newArrayList(),
                    errorReporter.errors() );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            stores.deleteStoresDir();
        }
    }

    /**
     * This method is intended for running individual Benchmark classes from your dev. environment.
     * It therefore honours more JMH annotations than full standalone execution, and also allows selection of only
     * some of the benchmarks within a class.
     */
    static void runBenchmark(
            Class<?> benchmarkClass,
            ProfileDescriptor profileDescriptor,
            Neo4jConfig neo4jConfig,
            Stores stores,
            boolean generateStoresInFork,
            int executeForks,
            ErrorPolicy errorPolicy,
            Jvm jvm,
            String... methods ) throws Exception
    {
        Annotations.AnnotationsValidationResult annotationsValidationResult = new Annotations().validate();
        if ( !annotationsValidationResult.isValid() )
        {
            throw new RuntimeException( annotationsValidationResult.message() );
        }

        Validation validation = new Validation();
        // force enable the benchmark, in case it is disabled by default
        BenchmarkDescription benchmark = BenchmarkDescription.of( benchmarkClass, validation ).copyAndEnable();
        assertValid( validation );
        if ( methods != null && methods.length != 0 )
        {
            benchmark = benchmark.copyRetainingMethods( methods );
        }
        List<BenchmarkDescription> benchmarks = singletonList( benchmark );

        String[] noJvmArgs = {};
        ErrorReporter errorReporter = new ErrorReporter( errorPolicy );
        int[] threadCounts = {1};
        String[] jmhArgs = new String[]{"-f", Integer.toString( executeForks )};

        runBenchmarks(
                new BenchmarkGroupBenchmarkMetrics(),
                benchmarks,
                profileDescriptor,
                neo4jConfig,
                threadCounts,
                jvm,
                noJvmArgs,
                jmhArgs,
                stores,
                generateStoresInFork,
                errorReporter,
                ( builder, className ) -> applyAnnotations( benchmarkClassForName( className ), builder ) );
    }

    private static void runBenchmarks(
            BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics,
            Collection<BenchmarkDescription> benchmarks,
            ProfileDescriptor profileDescriptor,
            Neo4jConfig neo4jConfig,
            int[] threadCounts,
            Jvm jvm,
            String[] jvmArgs,
            String[] jmhArgs,
            Stores stores,
            boolean generateStoresInFork,
            ErrorReporter errorReporter,
            BiConsumer<ChainedOptionsBuilder,String> builderModifier ) throws Exception
    {
        // each benchmark description will represent 1 method and 1 combination of param values, i.e., one benchmark
        // necessary to ensure only specific failing benchmarks are excluded from results, not all for a given class
        List<BenchmarkDescription> enabledExplodedBenchmarks = benchmarks.stream()
                                                                         .filter( BenchmarkDescription::isEnabled )
                                                                         .flatMap( benchmark -> benchmark.explode().stream() )
                                                                         .collect( toList() );

        if ( enabledExplodedBenchmarks.isEmpty() )
        {
            throw new RuntimeException( "Expected at least one benchmark description, but found none" );
        }

        List<BenchmarkDescription> benchmarksWithStores = generateStores(
                enabledExplodedBenchmarks,
                neo4jConfig,
                stores,
                jvm,
                jvmArgs,
                generateStoresInFork,
                errorReporter );

        if ( benchmarksWithStores.isEmpty() )
        {
            System.out.println( "\n\nNo stores were successfully generated, no benchmarks can be run\n\n" );
            return;
        }

        List<BenchmarkDescription> benchmarksWithProfiles = profileBenchmarks(
                benchmarksWithStores,
                neo4jConfig,
                jvm,
                jvmArgs,
                jmhArgs,
                threadCounts,
                profileDescriptor,
                stores,
                errorReporter,
                builderModifier );

        if ( benchmarksWithProfiles.isEmpty() )
        {
            System.out.println( "\n\n No benchmarks were successfully profiled, no benchmarks can be run\n\n" );
            return;
        }

        executeBenchmarks(
                benchmarkGroupBenchmarkMetrics,
                benchmarksWithProfiles,
                neo4jConfig,
                jvm,
                jvmArgs,
                jmhArgs,
                threadCounts,
                stores,
                errorReporter,
                builderModifier );

        if ( profileDescriptor.doProfile() )
        {
            moveProfilerRecordingsTo( profileDescriptor.targetDirectory(), stores );
        }
    }

    private static List<BenchmarkDescription> generateStores(
            Collection<BenchmarkDescription> benchmarks,
            Neo4jConfig neo4jConfig,
            Stores stores,
            Jvm jvm,
            String[] jvmArgs,
            boolean runInFork,
            ErrorReporter errorReporter ) throws Exception
    {
        // Run every benchmark once to create stores -- triggers store generation in benchmark setups
        // Ensures generation does not occur in benchmark setup later, which would, for example, pollute the heap
        System.out.println( "\n\n" );
        System.out.println( "-----------------------------------------------------------------------------------" );
        System.out.println( "------------------------------  STORE GENERATION  ---------------------------------" );
        System.out.println( "-----------------------------------------------------------------------------------" );
        System.out.println( "\n\n" );

        long storeGenerationStart = System.currentTimeMillis();

        stores.createStoresDir();

        List<BenchmarkDescription> benchmarksWithStores = new ArrayList<>( benchmarks );
        try
        {
            for ( BenchmarkDescription benchmark : benchmarks )
            {
                try
                {
                    ChainedOptionsBuilder builder = baseBuilder(
                            neo4jConfig.toJson(),
                            stores,
                            benchmark,
                            1,
                            jvm,
                            jvmArgs );
                    builder = builder
                            .warmupIterations( 0 )
                            .warmupTime( TimeValue.NONE )
                            .measurementIterations( 1 )
                            .measurementTime( TimeValue.NONE )
                            .verbosity( VerboseMode.SILENT )
                            .forks( runInFork ? 1 : 0 );
                    Options options = builder.build();
                    // sanity check, make sure provided benchmarks were correctly exploded
                    assertExactlyOneBenchmarkIsEnabled( options );
                    new Runner( options ).run();
                }
                catch ( Exception e )
                {
                    benchmarksWithStores.remove( benchmark );
                    errorReporter.recordOrThrow( e, benchmark.group(), benchmark.className() );
                }
                finally
                {
                    // make sure any temporary store copies are cleaned up
                    stores.deleteTemporaryStoreCopies();
                }
            }
            return benchmarksWithStores;
        }
        finally
        {
            long storeGenerationFinish = System.currentTimeMillis();
            Duration storeGenerationDuration = Duration.of( storeGenerationFinish - storeGenerationStart, MILLIS );
            // Print details of storage directory
            System.out.println( "\nStore generation took: " + durationToString( storeGenerationDuration ) + "\n" );
            System.out.println( stores.details() );
        }
    }

    private static List<BenchmarkDescription> profileBenchmarks(
            Collection<BenchmarkDescription> benchmarks,
            Neo4jConfig neo4jConfig,
            Jvm jvm,
            String[] jvmArgs,
            String[] jmhArgs,
            int[] threadCounts,
            ProfileDescriptor profileDescriptor,
            Stores stores,
            ErrorReporter errorReporter,
            BiConsumer<ChainedOptionsBuilder,String> builderModifier ) throws Exception
    {
        System.out.println( "\n\n" );
        System.out.println( "------------------------------------------------------------------------" );
        System.out.println( "---------------------------  PROFILING ---------------------------------" );
        System.out.println( "------------------------------------------------------------------------" );
        System.out.println( "\n\n" );

        List<BenchmarkDescription> benchmarksWithProfiles = new ArrayList<>( benchmarks );
        for ( Class<? extends Profiler> profiler : profileDescriptor.profilers() )
        {
            for ( int threadCount : threadCounts )
            {
                for ( BenchmarkDescription benchmark : benchmarks )
                {
                    if ( threadCount == 1 || benchmark.isThreadSafe() )
                    {
                        try
                        {
                            ChainedOptionsBuilder builder = JmhOptionsUtil.baseBuilder(
                                    neo4jConfig.toJson(),
                                    stores,
                                    benchmark,
                                    threadCount,
                                    jvm,
                                    jvmArgs );
                            JmhOptionsUtil
                                    .applyOptions( builder, new CommandLineOptions( jmhArgs ) )
                                    .addProfiler( profiler )
                                    .forks( 1 );
                            builderModifier.accept( builder, benchmark.className() );
                            Options options = builder.build();
                            // sanity check, make sure provided benchmarks were correctly exploded
                            assertExactlyOneBenchmarkIsEnabled( options );
                            executeBenchmarksForConfig( options, new BenchmarkGroupBenchmarkMetrics(), stores );
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
                            // make sure any temporary store copies are cleaned up
                            stores.deleteTemporaryStoreCopies();
                        }
                    }
                }
            }
        }
        return benchmarksWithProfiles;
    }

    private static void executeBenchmarks(
            BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics,
            Collection<BenchmarkDescription> benchmarks,
            Neo4jConfig neo4jConfig,
            Jvm jvm,
            String[] jvmArgs,
            String[] jmhArgs,
            int[] threadCounts,
            Stores stores,
            ErrorReporter errorReporter,
            BiConsumer<ChainedOptionsBuilder,String> builderModifier ) throws Exception
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
                        ChainedOptionsBuilder builder = JmhOptionsUtil.baseBuilder(
                                neo4jConfig.toJson(),
                                stores,
                                benchmark,
                                threadCount,
                                jvm,
                                jvmArgs );
                        JmhOptionsUtil.applyOptions( builder, new CommandLineOptions( jmhArgs ) );
                        builderModifier.accept( builder, benchmark.className() );
                        Options options = builder.build();
                        // sanity check, make sure provided benchmarks were correctly exploded
                        assertExactlyOneBenchmarkIsEnabled( options );
                        executeBenchmarksForConfig( options, benchmarkGroupBenchmarkMetrics, stores );
                    }
                    catch ( Exception e )
                    {
                        errorReporter.recordOrThrow( e, benchmark.group(), benchmark.className() );
                    }
                    finally
                    {
                        // make sure any temporary store copies are cleaned up
                        stores.deleteTemporaryStoreCopies();
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
            BenchmarkGroupBenchmarkMetrics metrics,
            Stores stores ) throws RunnerException
    {
        for ( RunResult runResult : new Runner( options ).run() )
        {
            BenchmarkParams params = runResult.getParams();
            BenchmarkGroup benchmarkGroup = JMHResultUtil.toBenchmarkGroup( params );
            Benchmarks benchmarks = JMHResultUtil.toBenchmarks( params );
            Neo4jConfig neo4jConfig = stores.neo4jConfigFor( benchmarkGroup, benchmarks.parentBenchmark() );
            if ( !benchmarks.isGroup() )
            {
                Result<?> primaryResult = runResult.getPrimaryResult();
                Metrics parentBenchmarkMetrics = JMHResultUtil.toMetrics(
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
                                       Metrics childBenchmarkMetrics = JMHResultUtil.toMetrics(
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

    private static void moveProfilerRecordingsTo( Path profilesDir, Stores stores )
    {
        System.out.println( "Moving profile recordings to: " + profilesDir.toAbsolutePath() );
        BenchmarkUtil.tryMkDir( profilesDir );
        stores.copyProfilerRecordingsTo( profilesDir );
    }
}
