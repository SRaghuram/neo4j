/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.model.model.AuxiliaryMetrics;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkConfig;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.model.model.BenchmarkPlan;
import com.neo4j.bench.model.model.BenchmarkTool;
import com.neo4j.bench.model.model.Environment;
import com.neo4j.bench.model.model.Java;
import com.neo4j.bench.model.model.Metrics;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.model.Plan;
import com.neo4j.bench.model.model.Project;
import com.neo4j.bench.model.model.Repository;
import com.neo4j.bench.model.model.TestRun;
import com.neo4j.bench.model.model.TestRunError;
import com.neo4j.bench.model.model.TestRunReport;
import com.neo4j.bench.model.profiling.ProfilerRecordings;
import com.neo4j.bench.model.profiling.RecordingType;
import com.neo4j.bench.model.util.JsonUtil;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.common.results.RunPhase.MEASUREMENT;
import static com.neo4j.bench.model.model.Benchmark.Mode.LATENCY;
import static com.neo4j.bench.model.model.Parameters.CLIENT;
import static com.neo4j.bench.model.model.Parameters.NONE;
import static com.neo4j.bench.model.model.Parameters.SERVER;
import static com.neo4j.bench.model.options.Edition.COMMUNITY;
import static com.neo4j.bench.model.profiling.RecordingType.ASYNC;
import static com.neo4j.bench.model.profiling.RecordingType.ASYNC_FLAMEGRAPH;
import static com.neo4j.bench.model.profiling.RecordingType.GC_CSV;
import static com.neo4j.bench.model.profiling.RecordingType.GC_LOG;
import static com.neo4j.bench.model.profiling.RecordingType.GC_SUMMARY;
import static com.neo4j.bench.model.profiling.RecordingType.JFR;
import static com.neo4j.bench.model.profiling.RecordingType.JFR_FLAMEGRAPH;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
public class AddProfilesIT
{
    @Inject
    public TestDirectory temporaryFolder;

    private static final Map<String,String> PARAMS = new HashMap<>( Map.of( "key", "value" ) );
    private static final BenchmarkGroup GROUP_1 = new BenchmarkGroup( "name1" );
    private static final BenchmarkGroup GROUP_2 = new BenchmarkGroup( "name2" );
    private static final Benchmark BENCHMARK_1_A = Benchmark.benchmarkFor( "desc1a", "name1a", LATENCY, PARAMS );
    private static final Benchmark BENCHMARK_1_B = Benchmark.benchmarkFor( "desc1b", "name1b", LATENCY, PARAMS );
    private static final Benchmark BENCHMARK_2_A = Benchmark.benchmarkFor( "desc2a", "name2a", LATENCY, PARAMS );

    private static String filename( BenchmarkGroup benchmarkGroup,
                                    Benchmark benchmark,
                                    Parameters additionalParams,
                                    RecordingType recordingType )
    {
        return new ProfilerRecordingDescriptor( benchmarkGroup,
                                                benchmark,
                                                MEASUREMENT,
                                                recordingType,
                                                emptyList(),
                                                additionalParams ).filename();
    }

    private File createJfrAndAsyncAndGcProfilesForBenchmark1a( Parameters parameters ) throws IOException
    {
        Path absolutePath = temporaryFolder.absolutePath();
        Path topLevelDir = absolutePath.resolve( "benchmark1a" );
        Files.createDirectory( topLevelDir );

        Files.createFile( absolutePath.resolve( "benchmark1a/" + filename( GROUP_1, BENCHMARK_1_A, parameters, JFR ) ) );
        Files.createFile( absolutePath.resolve( "benchmark1a/" + filename( GROUP_1, BENCHMARK_1_A, parameters, JFR_FLAMEGRAPH ) ) );
        Files.createFile( absolutePath.resolve( "benchmark1a/" + filename( GROUP_1, BENCHMARK_1_A, parameters, ASYNC ) ) );
        Files.createFile( absolutePath.resolve( "benchmark1a/" + filename( GROUP_1, BENCHMARK_1_A, parameters, ASYNC_FLAMEGRAPH ) ) );
        Files.createFile( absolutePath.resolve( "benchmark1a/" + filename( GROUP_1, BENCHMARK_1_A, parameters, GC_LOG ) ) );
        Files.createFile( absolutePath.resolve( "benchmark1a/" + filename( GROUP_1, BENCHMARK_1_A, parameters, GC_SUMMARY ) ) );
        Files.createFile( absolutePath.resolve( "benchmark1a/" + filename( GROUP_1, BENCHMARK_1_A, parameters, GC_CSV ) ) );

        // these should be ignored by profile loader
        Files.createFile( absolutePath.resolve( "benchmark1a/archive.tar.gz" ) );
        Files.createFile( absolutePath.resolve( "benchmark1a/" + GROUP_1.name() + "." + "INVALID" + JFR_FLAMEGRAPH.extension() ) );
        Files.createFile( absolutePath.resolve( "benchmark1a/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + ".invalid_suffix" ) );
        Files.createFile( absolutePath.resolve( "benchmark1a/" + GROUP_1.name() + "." + "WRONG" + GC_SUMMARY.extension() ) );
        return topLevelDir.toFile();
    }

    private File createAsyncProfilesForBenchmark1b( Parameters parameters ) throws IOException
    {
        Path absolutePath = temporaryFolder.absolutePath();
        Path topLevelDir = absolutePath.resolve( "benchmark1b" );
        Files.createDirectories( topLevelDir );
        Files.createFile( absolutePath.resolve( "benchmark1b/" + filename( GROUP_1, BENCHMARK_1_B, parameters, ASYNC ) ) );
        Files.createFile( absolutePath.resolve( "benchmark1b/" + filename( GROUP_1, BENCHMARK_1_B, parameters, ASYNC_FLAMEGRAPH ) ) );

        // these should be ignored by profile loader
        Files.createFile( absolutePath.resolve( "benchmark1b/archive.tar.gz" ) );
        Files.createFile( absolutePath.resolve( "benchmark1b/" + GROUP_1.name() + "." + "INVALID" + ASYNC_FLAMEGRAPH.extension() ) );
        Files.createFile( absolutePath.resolve( "benchmark1b/" + GROUP_1.name() + "." + BENCHMARK_1_B.name() + ".invalid_suffix" ) );
        Files.createFile( absolutePath.resolve( "benchmark1b/" + GROUP_1.name() + "." + "WRONG" + GC_LOG.extension() ) );
        Files.createFile( absolutePath.resolve( "benchmark1b/" + GROUP_1.name() + "." + "WRONG" + GC_CSV.extension() ) );
        return topLevelDir.toFile();
    }

    private File createJfrProfilesBenchmark2a( Parameters parameters ) throws IOException
    {
        Path absolutePath = temporaryFolder.absolutePath();
        Path topLevelDir = absolutePath.resolve( "benchmark2a" );
        Files.createDirectories( topLevelDir );
        Files.createFile( absolutePath.resolve( "benchmark2a/" + filename( GROUP_2, BENCHMARK_2_A, parameters, JFR ) ) );
        Files.createFile( absolutePath.resolve( "benchmark2a/" + filename( GROUP_2, BENCHMARK_2_A, parameters, JFR_FLAMEGRAPH ) ) );

        // these should be ignored by profile loader
        Files.createFile( absolutePath.resolve( "benchmark2a/archive.tar.gz" ) );
        Files.createFile( absolutePath.resolve( "benchmark2a/" + GROUP_2.name() + "." + "INVALID" + JFR_FLAMEGRAPH.extension() ) );
        Files.createFile( absolutePath.resolve( "benchmark2a/" + GROUP_2.name() + "." + BENCHMARK_2_A.name() + ".invalid_suffix" ) );
        Files.createFile( absolutePath.resolve( "benchmark2a/" + GROUP_2.name() + "." + "INVALID" + GC_LOG.extension() ) );
        return topLevelDir.toFile();
    }

    @Test
    void shouldAddProfiles() throws Exception
    {
        List<File> profileDirs = Lists.newArrayList(
                createJfrAndAsyncAndGcProfilesForBenchmark1a( NONE ),
                createAsyncProfilesForBenchmark1b( CLIENT ),
                createJfrProfilesBenchmark2a( SERVER ) );

        Consumer<TestRunReport> afterAsserts = testRunReportAfter ->
        {
            ProfilerRecordings actualProfilerRecordings1A = testRunReportAfter
                    .benchmarkGroupBenchmarkMetrics()
                    .getMetricsFor( GROUP_1, BENCHMARK_1_A )
                    .profilerRecordings();
            ProfilerRecordings expectedProfilerRecordings1A = new ProfilerRecordings()
                    .with( JFR, NONE, "some-s3-bucket/" + filename( GROUP_1, BENCHMARK_1_A, NONE, JFR ) )
                    // skip in 4.0
                    // .with( JFR_FLAMEGRAPH, NONE, "some-s3-bucket/" + filename( GROUP_1, BENCHMARK_1_A, NONE, JFR_FLAMEGRAPH ) )
                    .with( ASYNC, NONE, "some-s3-bucket/" + filename( GROUP_1, BENCHMARK_1_A, NONE, ASYNC ) )
                    .with( ASYNC_FLAMEGRAPH, NONE, "some-s3-bucket/" + filename( GROUP_1, BENCHMARK_1_A, NONE, ASYNC_FLAMEGRAPH ) )
                    .with( GC_LOG, NONE, "some-s3-bucket/" + filename( GROUP_1, BENCHMARK_1_A, NONE, GC_LOG ) )
                    .with( GC_SUMMARY, NONE, "some-s3-bucket/" + filename( GROUP_1, BENCHMARK_1_A, NONE, GC_SUMMARY ) )
                    .with( GC_CSV, NONE, "some-s3-bucket/" + filename( GROUP_1, BENCHMARK_1_A, NONE, GC_CSV ) );
            assertThat( actualProfilerRecordings1A, equalTo( expectedProfilerRecordings1A ) );

            ProfilerRecordings actualProfilerRecordings1B = testRunReportAfter
                    .benchmarkGroupBenchmarkMetrics()
                    .getMetricsFor( GROUP_1, BENCHMARK_1_B )
                    .profilerRecordings();
            ProfilerRecordings expectedProfilerRecordings1B = new ProfilerRecordings()
                    .with( ASYNC, CLIENT, "some-s3-bucket/" + filename( GROUP_1, BENCHMARK_1_B, CLIENT, ASYNC ) )
                    .with( ASYNC_FLAMEGRAPH, CLIENT, "some-s3-bucket/" + filename( GROUP_1, BENCHMARK_1_B, CLIENT, ASYNC_FLAMEGRAPH ) );
            assertThat( actualProfilerRecordings1B, equalTo( expectedProfilerRecordings1B ) );

            ProfilerRecordings actualProfilerRecordings2A = testRunReportAfter
                    .benchmarkGroupBenchmarkMetrics()
                    .getMetricsFor( GROUP_2, BENCHMARK_2_A )
                    .profilerRecordings();
            ProfilerRecordings expectedProfilerRecordings2A = new ProfilerRecordings()
                    // skip in 4.0
                    // .with( JFR_FLAMEGRAPH, SERVER, "some-s3-bucket/" + filename( GROUP_2, BENCHMARK_2_A, SERVER, JFR_FLAMEGRAPH ) );
                    .with( JFR, SERVER, "some-s3-bucket/" + filename( GROUP_2, BENCHMARK_2_A, SERVER, JFR ) );
            assertThat( actualProfilerRecordings2A, equalTo( expectedProfilerRecordings2A ) );

            // there should be exactly two profiles
            long actualProfileCount = testRunReportAfter.benchmarkGroupBenchmarks().stream()
                                                        .map( bgb -> testRunReportAfter.benchmarkGroupBenchmarkMetrics().getMetricsFor( bgb ) )
                                                        .filter( m -> !m.profilerRecordings().toMap().isEmpty() )
                                                        .count();
            assertThat( actualProfileCount, equalTo( 3L ) );
        };

        doShouldAddProfiles( afterAsserts, profileDirs, true );
        BenchmarkUtil.assertException( RuntimeException.class,
                                       () -> doShouldAddProfiles( afterAsserts, profileDirs, false ) );
    }

    @Test
    void shouldAddRequestedProfilerRecordings() throws Exception
    {
        List<File> profileDirs = Lists.newArrayList(
                createJfrAndAsyncAndGcProfilesForBenchmark1a( NONE ),
                createAsyncProfilesForBenchmark1b( CLIENT ) );

        Consumer<TestRunReport> afterAsserts = testRunReportAfter ->
        {
            ProfilerRecordings actualProfilerRecordings1A = testRunReportAfter
                    .benchmarkGroupBenchmarkMetrics()
                    .getMetricsFor( GROUP_1, BENCHMARK_1_A )
                    .profilerRecordings();
            ProfilerRecordings expectedProfilerRecordings1A = new ProfilerRecordings()
                    .with( JFR, NONE, "some-s3-bucket/" + filename( GROUP_1, BENCHMARK_1_A, NONE, JFR ) )
                    // skip in 4.0
                    // .with( JFR_FLAMEGRAPH, NONE, "some-s3-bucket/" + filename( GROUP_1, BENCHMARK_1_A, NONE, JFR_FLAMEGRAPH ) )
                    .with( ASYNC, NONE, "some-s3-bucket/" + filename( GROUP_1, BENCHMARK_1_A, NONE, ASYNC ) )
                    .with( ASYNC_FLAMEGRAPH, NONE, "some-s3-bucket/" + filename( GROUP_1, BENCHMARK_1_A, NONE, ASYNC_FLAMEGRAPH ) )
                    .with( GC_LOG, NONE, "some-s3-bucket/" + filename( GROUP_1, BENCHMARK_1_A, NONE, GC_LOG ) )
                    .with( GC_SUMMARY, NONE, "some-s3-bucket/" + filename( GROUP_1, BENCHMARK_1_A, NONE, GC_SUMMARY ) )
                    .with( GC_CSV, NONE, "some-s3-bucket/" + filename( GROUP_1, BENCHMARK_1_A, NONE, GC_CSV ) );
            assertThat( actualProfilerRecordings1A, equalTo( expectedProfilerRecordings1A ) );

            ProfilerRecordings actualProfilerRecordings1B = testRunReportAfter
                    .benchmarkGroupBenchmarkMetrics()
                    .getMetricsFor( GROUP_1, BENCHMARK_1_B )
                    .profilerRecordings();
            ProfilerRecordings expectedProfilerRecordings1B = new ProfilerRecordings()
                    .with( ASYNC, CLIENT, "some-s3-bucket/" + filename( GROUP_1, BENCHMARK_1_B, CLIENT, ASYNC ) )
                    .with( ASYNC_FLAMEGRAPH, CLIENT, "some-s3-bucket/" + filename( GROUP_1, BENCHMARK_1_B, CLIENT, ASYNC_FLAMEGRAPH ) );
            assertThat( actualProfilerRecordings1B, equalTo( expectedProfilerRecordings1B ) );

            // there should be exactly two profiles
            long actualProfileCount = testRunReportAfter.benchmarkGroupBenchmarks().stream()
                                                        .map( bgb -> testRunReportAfter.benchmarkGroupBenchmarkMetrics().getMetricsFor( bgb ) )
                                                        .filter( m -> !m.profilerRecordings().toMap().isEmpty() )
                                                        .count();
            assertThat( actualProfileCount, equalTo( 2L ) );
        };

        doShouldAddProfiles( afterAsserts, profileDirs, true );
        BenchmarkUtil.assertException( RuntimeException.class,
                                       () -> doShouldAddProfiles( afterAsserts, profileDirs, false ) );
    }

    @Test
    void shouldNotAddProfilesFilesThatAreNoRequests() throws Exception
    {
        List<File> profileDirs = Lists.newArrayList(
                createJfrAndAsyncAndGcProfilesForBenchmark1a( CLIENT ) );

        Consumer<TestRunReport> afterAsserts = testRunReportAfter ->
        {
            ProfilerRecordings actualProfilerRecordings1A = testRunReportAfter
                    .benchmarkGroupBenchmarkMetrics()
                    .getMetricsFor( GROUP_1, BENCHMARK_1_A )
                    .profilerRecordings();
            ProfilerRecordings expectedProfilerRecordings1A = new ProfilerRecordings()
                    // no .jfr files will be found due to edited mapping file
                    .with( JFR_FLAMEGRAPH, CLIENT, "some-s3-bucket/" + filename( GROUP_1, BENCHMARK_1_A, CLIENT, JFR_FLAMEGRAPH ) )
                    .with( ASYNC, CLIENT, "some-s3-bucket/" + filename( GROUP_1, BENCHMARK_1_A, CLIENT, ASYNC ) )
                    .with( ASYNC_FLAMEGRAPH, CLIENT, "some-s3-bucket/" + filename( GROUP_1, BENCHMARK_1_A, CLIENT, ASYNC_FLAMEGRAPH ) )
                    .with( GC_LOG, CLIENT, "some-s3-bucket/" + filename( GROUP_1, BENCHMARK_1_A, CLIENT, GC_LOG ) )
                    .with( GC_CSV, CLIENT, "some-s3-bucket/" + filename( GROUP_1, BENCHMARK_1_A, CLIENT, GC_CSV ) );
            assertThat( actualProfilerRecordings1A, equalTo( expectedProfilerRecordings1A ) );

            // there should be exactly two profiles
            long actualProfileCount = testRunReportAfter.benchmarkGroupBenchmarks().stream()
                                                        .map( bgb -> testRunReportAfter.benchmarkGroupBenchmarkMetrics().getMetricsFor( bgb ) )
                                                        .filter( m -> !m.profilerRecordings().toMap().isEmpty() )
                                                        .count();
            assertThat( actualProfileCount, equalTo( 1L ) );
        };

        BenchmarkUtil.assertException( RuntimeException.class,
                                       () -> doShouldAddProfiles( afterAsserts, profileDirs, false ) );
    }

    private void doShouldAddProfiles(
            Consumer<TestRunReport> afterAsserts,
            List<File> profilesDirs,
            boolean ignoreUnrecognizedFiles ) throws Exception
    {
        // given
        Map<String,String> params = new HashMap<>();
        params.put( "key", "value" );

        TestRun testRun = new TestRun( "id", 1, 2, 3, 1, "user" );
        BenchmarkConfig benchmarkConfig = new BenchmarkConfig( params );
        HashSet<Project> projects =
                Sets.newHashSet( new Project( Repository.NEO4J, "commit", "3.3.1", COMMUNITY, "branch", "owner" ) );

        Neo4jConfig neo4jConfig = new Neo4jConfig( params );
        Metrics metrics = new Metrics( SECONDS, 1, 10, 5.0, 42, 2.5, 5.0, 7.5, 9.0, 9.5, 9.9, 9.99 );
        AuxiliaryMetrics auxiliaryMetrics = new AuxiliaryMetrics( "rows", 1, 10, 5.0, 42, 2.5, 5.0, 7.5, 9.0, 9.5, 9.9, 9.99 );
        Environment environment = new Environment( "operating system", "server" );
        BenchmarkTool benchmarkTool = new BenchmarkTool( Repository.LDBC_BENCH, "commit", Repository.LDBC_BENCH.defaultOwner(), "3.2" );
        Java java = new Java( "jvm", "version", "jvm args" );
        Plan plan = SubmitTestRunsAndPlansIT.plan( "plan description" );

        BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics = new BenchmarkGroupBenchmarkMetrics();
        benchmarkGroupBenchmarkMetrics.add( GROUP_1, BENCHMARK_1_A, metrics, auxiliaryMetrics, neo4jConfig );
        benchmarkGroupBenchmarkMetrics.add( GROUP_1, BENCHMARK_1_B, metrics, auxiliaryMetrics, neo4jConfig );
        benchmarkGroupBenchmarkMetrics.add( GROUP_2, BENCHMARK_2_A, metrics, auxiliaryMetrics, neo4jConfig );

        BenchmarkPlan benchmarkPlan1a = new BenchmarkPlan( GROUP_1, BENCHMARK_1_A, plan );
        BenchmarkPlan benchmarkPlan1b = new BenchmarkPlan( GROUP_1, BENCHMARK_1_B, plan );

        TestRunReport testRunReportBefore = new TestRunReport(
                testRun,
                benchmarkConfig,
                projects,
                neo4jConfig,
                environment,
                benchmarkGroupBenchmarkMetrics,
                benchmarkTool,
                java,
                Lists.newArrayList( benchmarkPlan1a, benchmarkPlan1b ),
                Lists.newArrayList( new TestRunError( "group", "name", "an error message\n\n" ) ) );

        File testRunReportJson = temporaryFolder.file( "test-run-report.json" ).toFile();
        JsonUtil.serializeJson( testRunReportJson.toPath(), testRunReportBefore );

        // no profiles should exist yet
        testRunReportBefore.benchmarkGroupBenchmarks().stream()
                           .map( bgb -> testRunReportBefore.benchmarkGroupBenchmarkMetrics().getMetricsFor( bgb ) )
                           .forEach( m -> assertTrue( m.profilerRecordings().toMap().isEmpty() ) );
        assertThat( testRunReportBefore.testRun().archive(), is( nullValue() ) );

        for ( File profilesDir : profilesDirs )
        {
            List<String> args = AddProfilesCommand.argsFor( profilesDir.toPath(),
                                                            testRunReportJson.toPath(),
                                                            "some-s3-bucket",
                                                            "other-s3-bucket/archive.tar.gz",
                                                            ignoreUnrecognizedFiles );
            Main.main( args.stream().toArray( String[]::new ) );
        }

        TestRunReport testRunReportAfter = JsonUtil.deserializeJson( testRunReportJson.toPath(), TestRunReport.class );

        afterAsserts.accept( testRunReportAfter );
        assertThat( testRunReportAfter.testRun().archive(), equalTo( "other-s3-bucket/archive.tar.gz" ) );
    }
}
