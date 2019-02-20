/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.model;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.client.SubmitTestRunsAndPlansIT;
import com.neo4j.bench.client.profiling.RecordingType;
import com.neo4j.bench.client.util.JsonUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static com.neo4j.bench.client.model.Benchmark.Mode.LATENCY;
import static com.neo4j.bench.client.model.Edition.COMMUNITY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SerializationTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void shouldSerializeBenchmark() throws IOException
    {
        // given
        Map<String,String> params = new HashMap<>();
        params.put( "key", "value" );
        Benchmark before = Benchmark.benchmarkFor( "desc", "name", LATENCY, params );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    public void shouldSerializeBenchmarkWithQuery() throws IOException
    {
        // given
        Map<String,String> params = new HashMap<>();
        params.put( "key", "value" );
        Benchmark before = Benchmark.benchmarkFor( "desc", "name", LATENCY, params, "RETURN 1" );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    public void shouldSerializeBenchmarkConfig() throws IOException
    {
        // given
        Map<String,String> params = new HashMap<>();
        params.put( "key", "value" );
        BenchmarkConfig before = new BenchmarkConfig( params );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    public void shouldSerializeBenchmarkConfigFromFile() throws IOException
    {
        // given
        File benchmarkConfig = temporaryFolder.newFile();
        FileWriter fileWriter = new FileWriter( benchmarkConfig );
        fileWriter.append( "key1=value1" );
        fileWriter.append( "\n" );
        fileWriter.append( "key2=value2" );
        fileWriter.flush();
        fileWriter.close();

        BenchmarkConfig before = BenchmarkConfig.from( benchmarkConfig.toPath() );
        // then

        BenchmarkConfig after = (BenchmarkConfig) shouldSerializeAndDeserialize( before );

        assertThat( before.toMap().get( "key1" ), equalTo( after.toMap().get( "key1" ) ) );
        assertThat( before.toMap().get( "key2" ), equalTo( after.toMap().get( "key2" ) ) );
    }

    @Test
    public void shouldSerializeBenchmarkGroup() throws IOException
    {
        // given
        BenchmarkGroup before = new BenchmarkGroup( "name" );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    public void shouldSerializeBenchmarkGroupBenchmark() throws IOException
    {
        // given
        BenchmarkGroup benchmarkGroup = new BenchmarkGroup( "name" );
        Map<String,String> params = new HashMap<>();
        params.put( "key", "value" );
        Benchmark benchmark = Benchmark.benchmarkFor( "desc", "name", LATENCY, params );
        BenchmarkGroupBenchmark before = new BenchmarkGroupBenchmark( benchmarkGroup, benchmark );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    public void shouldSerializeBenchmarkGroupBenchmarkMetrics() throws IOException
    {
        // given
        BenchmarkGroup benchmarkGroup = new BenchmarkGroup( "name" );
        Map<String,String> params = new HashMap<>();
        params.put( "key", "value" );
        Benchmark benchmark = Benchmark.benchmarkFor( "desc", "name", LATENCY, params );
        Metrics metrics = new Metrics( SECONDS, 1, 10, 5.0, 1.5, 0.1, 42, 2.5, 5.0, 7.5, 9.0, 9.5, 9.9, 9.99 );
        Neo4jConfig neo4jConfig = new Neo4jConfig( params );
        BenchmarkGroupBenchmarkMetrics before = new BenchmarkGroupBenchmarkMetrics();
        before.add( benchmarkGroup, benchmark, metrics, neo4jConfig );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    public void shouldSerializeBenchmarkGroupBenchmarkPlans() throws IOException
    {
        // given
        BenchmarkGroupBenchmarkPlans before = new BenchmarkGroupBenchmarkPlans();
        BenchmarkGroup benchmarkGroup = new BenchmarkGroup( "group" );
        HashMap<String,String> params = new HashMap<>();
        params.put( "key", "value" );
        Benchmark benchmark = Benchmark.benchmarkFor( "desc", "name", LATENCY, params );
        Plan plan = SubmitTestRunsAndPlansIT.plan( "plan description" );
        before.add( benchmarkGroup, benchmark, plan );
        assertThat( before.benchmarkPlans().size(), equalTo( 1 ) );
        assertThat( before.benchmarkPlans().get( 0 ).benchmarkGroup(), equalTo( benchmarkGroup ) );
        assertThat( before.benchmarkPlans().get( 0 ).benchmark(), equalTo( benchmark ) );

        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    public void shouldSerializeBenchmarkMetrics() throws IOException
    {
        // given
        Map<String,String> params = new HashMap<>();
        params.put( "key", "value" );
        Metrics metrics = new Metrics( SECONDS, 1, 10, 5.0, 1.5, 0.1, 42, 2.5, 5.0, 7.5, 9.0, 9.5, 9.9, 9.99 );
        BenchmarkMetrics before = new BenchmarkMetrics(
                "name",
                "this is simple",
                Benchmark.Mode.LATENCY,
                params,
                metrics.toMap() );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    public void shouldSerializeBenchmarkPlan() throws IOException
    {
        // given
        Map<String,String> params = new HashMap<>();
        params.put( "key", "value" );
        Benchmark benchmark = Benchmark.benchmarkFor( "desc", "name", LATENCY, params );
        Plan plan = SubmitTestRunsAndPlansIT.plan( "plan description" );
        BenchmarkPlan before = new BenchmarkPlan( new BenchmarkGroup( "name_full" ), benchmark, plan );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    public void shouldSerializeBenchmarks() throws IOException
    {
        // given
        Map<String,String> params = new HashMap<>();
        params.put( "key", "value" );
        Benchmark benchmark1 = Benchmark.benchmarkFor( "desc", "name", LATENCY, params );
        Benchmarks before = new Benchmarks( benchmark1 );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    public void shouldSerializeBenchmarkTool() throws IOException
    {
        // given
        BenchmarkTool before = new BenchmarkTool( Repository.LDBC_BENCH, "commit", Repository.LDBC_BENCH.defaultOwner(), "3.2" );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    public void shouldSerializeEnvironment() throws IOException
    {
        // given
        Environment before = new Environment( "operating system", "server" );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    public void shouldSerializeMetrics() throws IOException
    {
        // given
        Metrics before = new Metrics( SECONDS, 1, 10, 5.0, 1.5, 0.1, 42, 2.5, 5.0, 7.5, 9.0, 9.5, 9.9, 9.99 );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    public void shouldSerializeNeo4j() throws IOException
    {
        // given
        Project before = new Project( Repository.NEO4J, "commit", "3.3.3", COMMUNITY, "branch", "owner" );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    public void shouldSerializeNeo4jConfig() throws IOException
    {
        // given
        HashMap<String,String> params = new HashMap<>();
        params.put( "key", "value" );
        Neo4jConfig before = new Neo4jConfig( params );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    public void shouldSerializeNeo4jConfigFromFile() throws IOException
    {
        // given
        File neo4jConfig = temporaryFolder.newFile();
        FileWriter fileWriter = new FileWriter( neo4jConfig );
        fileWriter.append( "key1=value1" );
        fileWriter.append( "\n" );
        fileWriter.append( "key2=value2" );
        fileWriter.flush();
        fileWriter.close();

        Neo4jConfig before = Neo4jConfig.fromFile( neo4jConfig );
        // then
        Neo4jConfig after = (Neo4jConfig) shouldSerializeAndDeserialize( before );
        assertThat( before.toMap().get( "key1" ), equalTo( after.toMap().get( "key1" ) ) );
        assertThat( before.toMap().get( "key2" ), equalTo( after.toMap().get( "key2" ) ) );
    }

    @Test
    public void shouldSerializePlan() throws IOException
    {
        // given
        Plan before = SubmitTestRunsAndPlansIT.plan( "plan description" );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    public void shouldSerializePlanCompilationMetrics() throws IOException
    {
        // given
        Map<String,Long> compilationTimes = new HashMap<>();
        compilationTimes.put( "AST_REWRITE", 1L );
        compilationTimes.put( "SEMANTIC_CHECK", 2L );
        PlanCompilationMetrics before = new PlanCompilationMetrics( compilationTimes );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    public void shouldSerializePlanOperator() throws IOException
    {
        // given
        Map<String,String> arguments = new HashMap<>();
        arguments.put( "key", "value" );
        PlanOperator before = new PlanOperator( "operator type1", 1, 2, 3, arguments,
                                                Lists.newArrayList( new PlanOperator( "operator type2", 4, 5, 6, arguments, emptyList() ) ) );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    public void shouldSerializePlanTree() throws IOException
    {
        // given
        PlanTree before = SubmitTestRunsAndPlansIT.plan( "plan description" ).planTree();
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    public void shouldSerializeJava() throws IOException
    {
        // given
        Java before = new Java( "jvm", "version", "jvm args" );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    public void shouldSerializeTestRun() throws IOException
    {
        // given
        TestRun before = new TestRun( "id", 1, 2, 3, 1, "user" );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    public void shouldSerializeTestRunWithProfile() throws IOException
    {
        // given
        TestRun before = new TestRun( "id", 1, 2, 3, 1, "user" );
        before.setArchive( "bucket/profile" );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    public void shouldSerializeTestRunReport() throws IOException
    {
        // given
        Map<String,String> params = new HashMap<>();
        params.put( "key", "value" );

        TestRun testRun = new TestRun( "id", 1, 2, 3, 1, "user" );
        BenchmarkConfig benchmarkConfig = new BenchmarkConfig( params );
        HashSet<Project> projects =
                Sets.newHashSet( new Project( Repository.NEO4J, "commit", "3.3.3", COMMUNITY, "branch", "owner" ) );

        Neo4jConfig neo4jConfig = new Neo4jConfig( params );
        Metrics metrics = new Metrics( SECONDS, 1, 10, 5.0, 1.5, 0.1, 42, 2.5, 5.0, 7.5, 9.0, 9.5, 9.9, 9.99 );
        Environment environment = new Environment( "operating system", "server" );
        BenchmarkTool benchmarkTool = new BenchmarkTool( Repository.LDBC_BENCH, "commit", "neo-technology", "3.2" );
        Java java = new Java( "jvm", "version", "jvm args" );
        Plan plan = SubmitTestRunsAndPlansIT.plan( "plan description" );

        BenchmarkGroup benchmarkGroup1 = new BenchmarkGroup( "name1" );
        BenchmarkGroup benchmarkGroup2 = new BenchmarkGroup( "name2" );

        Benchmark benchmark1a = Benchmark.benchmarkFor( "desc1a", "name1a", LATENCY, params );
        Benchmark benchmark1b = Benchmark.benchmarkFor( "desc1b", "name1b", LATENCY, params );
        Benchmark benchmark2a = Benchmark.benchmarkFor( "desc2a", "name2a", LATENCY, params );

        BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics = new BenchmarkGroupBenchmarkMetrics();
        benchmarkGroupBenchmarkMetrics.add( benchmarkGroup1, benchmark1a, metrics, neo4jConfig );
        benchmarkGroupBenchmarkMetrics.add( benchmarkGroup1, benchmark1b, metrics, neo4jConfig );
        benchmarkGroupBenchmarkMetrics.add( benchmarkGroup2, benchmark2a, metrics, neo4jConfig );

        ProfilerRecordings profilerRecordings1A = new ProfilerRecordings()
                .with( RecordingType.JFR, "bucket/jfrName1a" )
                .with( RecordingType.ASYNC, "bucket/asyncName1a" )
                .with( RecordingType.JFR_FLAMEGRAPH, "bucket/jfrFlamegraph1a" )
                .with( RecordingType.ASYNC_FLAMEGRAPH, "bucket/asyncFlamegraph1a" );
        ProfilerRecordings profilerRecordings2A = new ProfilerRecordings()
                .with( RecordingType.JFR, "bucket/jfrName2a" )
                .with( RecordingType.ASYNC, "bucket/asyncName2a" )
                .with( RecordingType.JFR_FLAMEGRAPH, "bucket/jfrFlamegraph2a" )
                .with( RecordingType.ASYNC_FLAMEGRAPH, "bucket/asyncFlamegraph2a" );

        benchmarkGroupBenchmarkMetrics.attachProfilerRecording( benchmarkGroup1, benchmark1a, profilerRecordings1A );
        benchmarkGroupBenchmarkMetrics.attachProfilerRecording( benchmarkGroup2, benchmark2a, profilerRecordings2A );

        BenchmarkPlan benchmarkPlan1a = new BenchmarkPlan( benchmarkGroup1, benchmark1a, plan );
        BenchmarkPlan benchmarkPlan1b = new BenchmarkPlan( benchmarkGroup1, benchmark1b, plan );

        TestRunReport before = new TestRunReport(
                testRun,
                benchmarkConfig,
                projects,
                neo4jConfig,
                environment,
                benchmarkGroupBenchmarkMetrics,
                benchmarkTool,
                java,
                Lists.newArrayList( benchmarkPlan1a, benchmarkPlan1b ),
                Lists.newArrayList(
                        new TestRunError( "group 1", "", "" ),
                        new TestRunError( "group 2", "name", "a bad thing\nhappened" ),
                        new TestRunError( "group 3", "boom", "also\nthis other thing is not good" ) ) );

        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    public void shouldSerializeProfiles() throws IOException
    {
        // given
        ProfilerRecordings profilerRecordings = new ProfilerRecordings()
                .with( RecordingType.JFR, "bucket/jfrName" )
                .with( RecordingType.ASYNC, "bucket/asyncName" )
                .with( RecordingType.JFR_FLAMEGRAPH, "bucket/jfrFlamegraph" )
                .with( RecordingType.ASYNC_FLAMEGRAPH, "bucket/asyncFlamegraph" );

        // then
        shouldSerializeAndDeserialize( profilerRecordings );
    }

    @Test
    public void shouldSerializeAnnotation() throws IOException
    {
        // given
        Annotation before = new Annotation( "Comment", 0, "id", "Robert" );
        // then
        shouldSerializeAndDeserialize( before );
    }

    private Object shouldSerializeAndDeserialize( Object before ) throws IOException
    {
        File jsonFile = temporaryFolder.newFile();
        JsonUtil.serializeJson( jsonFile.toPath(), before );
        Object after = JsonUtil.deserializeJson( jsonFile.toPath(), before.getClass() );
        assertThat( before, equalTo( after ) );
        return after;
    }
}
