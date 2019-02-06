/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkConfig;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.client.model.BenchmarkPlan;
import com.neo4j.bench.client.model.BenchmarkTool;
import com.neo4j.bench.client.model.Environment;
import com.neo4j.bench.client.model.Java;
import com.neo4j.bench.client.model.Metrics;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.model.Plan;
import com.neo4j.bench.client.model.ProfilerRecordings;
import com.neo4j.bench.client.model.Project;
import com.neo4j.bench.client.model.Repository;
import com.neo4j.bench.client.model.TestRun;
import com.neo4j.bench.client.model.TestRunError;
import com.neo4j.bench.client.model.TestRunReport;
import com.neo4j.bench.client.profiling.RecordingType;
import com.neo4j.bench.client.util.BenchmarkUtil;
import com.neo4j.bench.client.util.JsonUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.neo4j.bench.client.model.Benchmark.Mode.LATENCY;
import static com.neo4j.bench.client.model.Edition.COMMUNITY;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

import static java.util.concurrent.TimeUnit.SECONDS;

public class AddProfilesIT
{
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private static final Map<String,String> PARAMS = new HashMap<String,String>()
    {{
        put( "key", "value" );
    }};
    private static final BenchmarkGroup GROUP_1 = new BenchmarkGroup( "name1" );
    private static final BenchmarkGroup GROUP_2 = new BenchmarkGroup( "name2" );
    private static final Benchmark BENCHMARK_1_A = Benchmark.benchmarkFor( "desc1a", "name1a", LATENCY, PARAMS );
    private static final Benchmark BENCHMARK_1_B = Benchmark.benchmarkFor( "desc1b", "name1b", LATENCY, PARAMS );
    private static final Benchmark BENCHMARK_2_A = Benchmark.benchmarkFor( "desc2a", "name2a", LATENCY, PARAMS );

    private File createJfrAndAsyncAndGcProfilesForBenchmark1a() throws IOException
    {
        File topLevelDir = temporaryFolder.newFolder( "benchmark1a" );
        temporaryFolder.newFile( "benchmark1a/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + ".jfr" );
        temporaryFolder.newFile( "benchmark1a/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + "-jfr.svg" );
        temporaryFolder.newFile( "benchmark1a/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + ".async" );
        temporaryFolder.newFile( "benchmark1a/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + "-async.svg" );
        temporaryFolder.newFile( "benchmark1a/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + ".gc" );
        temporaryFolder.newFile( "benchmark1a/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + "-gc.json" );
        temporaryFolder.newFile( "benchmark1a/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + "-gc.csv" );

        // these should be ignored by profile loader
        temporaryFolder.newFile( "benchmark1a/archive.tar.gz" );
        temporaryFolder.newFile( "benchmark1a/" + GROUP_1.name() + "." + "INVALID" + "-jfr.svg" );
        temporaryFolder.newFile( "benchmark1a/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + ".invalid_suffix" );
        temporaryFolder.newFile( "benchmark1a/" + GROUP_1.name() + "." + "WRONG" + "-gc-json" );
        return topLevelDir;
    }

    private File createAsyncProfilesForBenchmark1b() throws IOException
    {
        File topLevelDir = temporaryFolder.newFolder( "benchmark1b" );
        temporaryFolder.newFile( "benchmark1b/" + GROUP_1.name() + "." + BENCHMARK_1_B.name() + ".async" );
        temporaryFolder.newFile( "benchmark1b/" + GROUP_1.name() + "." + BENCHMARK_1_B.name() + "-async.svg" );

        // these should be ignored by profile loader
        temporaryFolder.newFile( "benchmark1b/archive.tar.gz" );
        temporaryFolder.newFile( "benchmark1b/" + GROUP_1.name() + "." + "INVALID" + "-async.svg" );
        temporaryFolder.newFile( "benchmark1b/" + GROUP_1.name() + "." + BENCHMARK_1_B.name() + ".invalid_suffix" );
        temporaryFolder.newFile( "benchmark1b/" + GROUP_1.name() + "." + "WRONG" + ".gc" );
        temporaryFolder.newFile( "benchmark1b/" + GROUP_1.name() + "." + "WRONG" + "-gc.csv" );
        return topLevelDir;
    }

    private File createJfrProfilesBenchmark2a() throws IOException
    {
        File topLevelDir = temporaryFolder.newFolder( "benchmark2a" );
        temporaryFolder.newFile( "benchmark2a/" + GROUP_2.name() + "." + BENCHMARK_2_A.name() + ".jfr" );
        temporaryFolder.newFile( "benchmark2a/" + GROUP_2.name() + "." + BENCHMARK_2_A.name() + "-jfr.svg" );

        // these should be ignored by profile loader
        temporaryFolder.newFile( "benchmark2a/archive.tar.gz" );
        temporaryFolder.newFile( "benchmark2a/" + GROUP_2.name() + "." + "INVALID" + "-jfr.svg" );
        temporaryFolder.newFile( "benchmark2a/" + GROUP_2.name() + "." + BENCHMARK_2_A.name() + ".invalid_suffix" );
        temporaryFolder.newFile( "benchmark2a/" + GROUP_2.name() + "." + "INVALID" + ".gc" );
        return topLevelDir;
    }

    @Test
    public void shouldAddProfiles() throws Exception
    {
        List<File> profileDirs = Lists.newArrayList(
                createJfrAndAsyncAndGcProfilesForBenchmark1a(),
                createAsyncProfilesForBenchmark1b(),
                createJfrProfilesBenchmark2a() );

        Consumer<TestRunReport> afterAsserts = testRunReportAfter ->
        {
            ProfilerRecordings actualProfilerRecordings1A = testRunReportAfter
                    .benchmarkGroupBenchmarkMetrics()
                    .getMetricsFor( GROUP_1, BENCHMARK_1_A )
                    .profilerRecordings();
            ProfilerRecordings expectedProfilerRecordings1A = new ProfilerRecordings()
                    .with( RecordingType.JFR, "some-s3-bucket/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + ".jfr" )
                    .with( RecordingType.JFR_FLAMEGRAPH, "some-s3-bucket/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + "-jfr.svg" )
                    .with( RecordingType.ASYNC, "some-s3-bucket/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + ".async" )
                    .with( RecordingType.ASYNC_FLAMEGRAPH, "some-s3-bucket/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + "-async.svg" )
                    .with( RecordingType.GC_LOG, "some-s3-bucket/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + ".gc" )
                    .with( RecordingType.GC_SUMMARY, "some-s3-bucket/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + "-gc.json" )
                    .with( RecordingType.GC_CSV, "some-s3-bucket/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + "-gc.csv" );
            assertThat( actualProfilerRecordings1A, equalTo( expectedProfilerRecordings1A ) );

            ProfilerRecordings actualProfilerRecordings1B = testRunReportAfter
                    .benchmarkGroupBenchmarkMetrics()
                    .getMetricsFor( GROUP_1, BENCHMARK_1_B )
                    .profilerRecordings();
            ProfilerRecordings expectedProfilerRecordings1B = new ProfilerRecordings()
                    .with( RecordingType.ASYNC, "some-s3-bucket/" + GROUP_1.name() + "." + BENCHMARK_1_B.name() + ".async" )
                    .with( RecordingType.ASYNC_FLAMEGRAPH, "some-s3-bucket/" + GROUP_1.name() + "." + BENCHMARK_1_B.name() + "-async.svg" );
            assertThat( actualProfilerRecordings1B, equalTo( expectedProfilerRecordings1B ) );

            ProfilerRecordings actualProfilerRecordings2A = testRunReportAfter
                    .benchmarkGroupBenchmarkMetrics()
                    .getMetricsFor( GROUP_2, BENCHMARK_2_A )
                    .profilerRecordings();
            ProfilerRecordings expectedProfilerRecordings2A = new ProfilerRecordings()
                    .with( RecordingType.JFR, "some-s3-bucket/" + GROUP_2.name() + "." + BENCHMARK_2_A.name() + ".jfr" )
                    .with( RecordingType.JFR_FLAMEGRAPH, "some-s3-bucket/" + GROUP_2.name() + "." + BENCHMARK_2_A.name() + "-jfr.svg" );
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
    public void shouldAddRequestedProfilerRecordings() throws Exception
    {
        List<File> profileDirs = Lists.newArrayList(
                createJfrAndAsyncAndGcProfilesForBenchmark1a(),
                createAsyncProfilesForBenchmark1b() );

        Consumer<TestRunReport> afterAsserts = testRunReportAfter ->
        {
            ProfilerRecordings actualProfilerRecordings1A = testRunReportAfter
                    .benchmarkGroupBenchmarkMetrics()
                    .getMetricsFor( GROUP_1, BENCHMARK_1_A )
                    .profilerRecordings();
            ProfilerRecordings expectedProfilerRecordings1A = new ProfilerRecordings()
                    .with( RecordingType.JFR, "some-s3-bucket/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + ".jfr" )
                    .with( RecordingType.JFR_FLAMEGRAPH, "some-s3-bucket/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + "-jfr.svg" )
                    .with( RecordingType.ASYNC, "some-s3-bucket/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + ".async" )
                    .with( RecordingType.ASYNC_FLAMEGRAPH, "some-s3-bucket/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + "-async.svg" )
                    .with( RecordingType.GC_LOG, "some-s3-bucket/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + ".gc" )
                    .with( RecordingType.GC_SUMMARY, "some-s3-bucket/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + "-gc.json" )
                    .with( RecordingType.GC_CSV, "some-s3-bucket/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + "-gc.csv" );
            assertThat( actualProfilerRecordings1A, equalTo( expectedProfilerRecordings1A ) );

            ProfilerRecordings actualProfilerRecordings1B = testRunReportAfter
                    .benchmarkGroupBenchmarkMetrics()
                    .getMetricsFor( GROUP_1, BENCHMARK_1_B )
                    .profilerRecordings();
            ProfilerRecordings expectedProfilerRecordings1B = new ProfilerRecordings()
                    .with( RecordingType.ASYNC, "some-s3-bucket/" + GROUP_1.name() + "." + BENCHMARK_1_B.name() + ".async" )
                    .with( RecordingType.ASYNC_FLAMEGRAPH, "some-s3-bucket/" + GROUP_1.name() + "." + BENCHMARK_1_B.name() + "-async.svg" );
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
    public void shouldNotAddProfilesFilesThatAreNoRequests() throws Exception
    {
        List<File> profileDirs = Lists.newArrayList(
                createJfrAndAsyncAndGcProfilesForBenchmark1a() );

        Consumer<TestRunReport> afterAsserts = testRunReportAfter ->
        {
            ProfilerRecordings actualProfilerRecordings1A = testRunReportAfter
                    .benchmarkGroupBenchmarkMetrics()
                    .getMetricsFor( GROUP_1, BENCHMARK_1_A )
                    .profilerRecordings();
            ProfilerRecordings expectedProfilerRecordings1A = new ProfilerRecordings()
                    // no .jfr files will be found due to edited mapping file
                    .with( RecordingType.JFR_FLAMEGRAPH, "some-s3-bucket/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + "-jfr.svg" )
                    .with( RecordingType.ASYNC, "some-s3-bucket/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + ".async" )
                    .with( RecordingType.ASYNC_FLAMEGRAPH, "some-s3-bucket/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + "-async.svg" )
                    .with( RecordingType.GC_LOG, "some-s3-bucket/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + ".gc" )
                    .with( RecordingType.GC_CSV, "some-s3-bucket/" + GROUP_1.name() + "." + BENCHMARK_1_A.name() + "-gc.csv" );
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
        Metrics metrics = new Metrics( SECONDS, 1, 10, 5.0, 1.5, 0.1, 42, 2.5, 5.0, 7.5, 9.0, 9.5, 9.9, 9.99 );
        Environment environment = new Environment( "operating system", "server" );
        BenchmarkTool benchmarkTool = new BenchmarkTool( Repository.LDBC_BENCH, "commit", Repository.LDBC_BENCH.defaultOwner(), "3.2" );
        Java java = new Java( "jvm", "version", "jvm args" );
        Plan plan = SubmitTestRunsAndPlansIT.plan( "plan description" );

        BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics = new BenchmarkGroupBenchmarkMetrics();
        benchmarkGroupBenchmarkMetrics.add( GROUP_1, BENCHMARK_1_A, metrics, neo4jConfig );
        benchmarkGroupBenchmarkMetrics.add( GROUP_1, BENCHMARK_1_B, metrics, neo4jConfig );
        benchmarkGroupBenchmarkMetrics.add( GROUP_2, BENCHMARK_2_A, metrics, neo4jConfig );

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

        File testRunReportJson = temporaryFolder.newFile();
        JsonUtil.serializeJson( testRunReportJson.toPath(), testRunReportBefore );

        // no profiles should exist yet
        testRunReportBefore.benchmarkGroupBenchmarks().stream()
                           .map( bgb -> testRunReportBefore.benchmarkGroupBenchmarkMetrics().getMetricsFor( bgb ) )
                           .forEach( m -> assertTrue( m.profilerRecordings().toMap().isEmpty() ) );
        assertThat( testRunReportBefore.testRun().archive(), is( nullValue() ) );

        for ( File profilesDir : profilesDirs )
        {
            List<String> args = Lists.newArrayList(
                    "add-profiles",
                    AddProfilesCommand.CMD_DIR, profilesDir.getAbsolutePath(),
                    AddProfilesCommand.CMD_TEST_RUN_RESULTS, testRunReportJson.getAbsolutePath(),
                    AddProfilesCommand.CMD_S3_BUCKET, "some-s3-bucket",
                    AddProfilesCommand.CMD_ARCHIVE, "other-s3-bucket/archive.tar.gz" );
            if ( ignoreUnrecognizedFiles )
            {
                args.add( AddProfilesCommand.CMD_IGNORE_UNRECOGNIZED_FILES );
            }
            Main.main( args.stream().toArray( String[]::new ) );
        }

        TestRunReport testRunReportAfter = JsonUtil.deserializeJson( testRunReportJson.toPath(), TestRunReport.class );

        afterAsserts.accept( testRunReportAfter );
        assertThat( testRunReportAfter.testRun().archive(), equalTo( "other-s3-bucket/archive.tar.gz" ) );
    }
}
