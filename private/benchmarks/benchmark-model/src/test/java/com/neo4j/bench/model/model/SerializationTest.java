/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.model.profiling.ProfilerRecordings;
import com.neo4j.bench.model.profiling.RecordingType;
import com.neo4j.bench.model.util.JsonUtil;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.model.model.Benchmark.Mode.LATENCY;
import static com.neo4j.bench.model.model.Benchmark.benchmarkFor;
import static com.neo4j.bench.model.options.Edition.COMMUNITY;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@TestDirectoryExtension
public class SerializationTest
{
    @Inject
    public TestDirectory temporaryFolder;

    @Test
    void shouldSerializeTestRunReport()
    {
        // given
        Map<String,String> params = new HashMap<>();
        params.put( "key", "value" );

        TestRun testRun = new TestRun( "id", 1, 2, 3, 1, "user" );
        BenchmarkConfig benchmarkConfig = new BenchmarkConfig( params );
        HashSet<Project> projects =
                Sets.newHashSet( new Project( Repository.NEO4J, "commit", "3.3.3", COMMUNITY, "branch", "owner" ) );

        Neo4jConfig neo4jConfig = new Neo4jConfig( params );
        Metrics metrics = getMetrics();
        AuxiliaryMetrics auxiliaryMetrics = getAuxiliaryMetrics();
        Environment environment = new Environment( "operating system", "server" );
        BenchmarkTool benchmarkTool = new BenchmarkTool( Repository.LDBC_BENCH, "commit", "neo-technology", "3.2" );
        Java java = new Java( "jvm", "version", "jvm args" );
        Plan plan = testPlan();

        BenchmarkGroup benchmarkGroup1 = new BenchmarkGroup( "name1" );
        BenchmarkGroup benchmarkGroup2 = new BenchmarkGroup( "name2" );

        Benchmark benchmark1a = benchmarkFor( "desc1a", "name1a", LATENCY, params );
        Benchmark benchmark1b = benchmarkFor( "desc1b", "name1b", LATENCY, params );
        Benchmark benchmark2a = benchmarkFor( "desc2a", "name2a", LATENCY, params );

        BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics = new BenchmarkGroupBenchmarkMetrics();
        benchmarkGroupBenchmarkMetrics.add( benchmarkGroup1, benchmark1a, metrics, auxiliaryMetrics, neo4jConfig );
        benchmarkGroupBenchmarkMetrics.add( benchmarkGroup1, benchmark1b, metrics, auxiliaryMetrics, neo4jConfig );
        benchmarkGroupBenchmarkMetrics.add( benchmarkGroup2, benchmark2a, metrics, auxiliaryMetrics, neo4jConfig );

        ProfilerRecordings profilerRecordings1A = new ProfilerRecordings()
                .with( RecordingType.JFR, Parameters.NONE, "bucket/jfrName1a" )
                .with( RecordingType.ASYNC, Parameters.NONE, "bucket/asyncName1a" )
                .with( RecordingType.JFR_FLAMEGRAPH, Parameters.NONE, "bucket/jfrFlamegraph1a" )
                .with( RecordingType.ASYNC_FLAMEGRAPH, Parameters.NONE, "bucket/asyncFlamegraph1a" );
        ProfilerRecordings profilerRecordings2A = new ProfilerRecordings()
                .with( RecordingType.JFR, Parameters.NONE, "bucket/jfrName2a" )
                .with( RecordingType.ASYNC, Parameters.NONE, "bucket/asyncName2a" )
                .with( RecordingType.JFR_FLAMEGRAPH, Parameters.NONE, "bucket/jfrFlamegraph2a" )
                .with( RecordingType.ASYNC_FLAMEGRAPH, Parameters.NONE, "bucket/asyncFlamegraph2a" );

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
    void shouldSerializeBenchmarkPlan()
    {
        // given
        Map<String,String> params = new HashMap<>();
        params.put( "key", "value" );
        Benchmark benchmark = benchmarkFor( "desc", "name", LATENCY, params );
        Plan plan = testPlan();
        BenchmarkPlan before = new BenchmarkPlan( new BenchmarkGroup( "name_full" ), benchmark, plan );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    void shouldSerializePlan()
    {
        // given
        Plan before = testPlan();
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    void shouldSerializePlanTree()
    {
        // given
        PlanTree before = testPlan().planTree();
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    void shouldSerializeBenchmarkGroupBenchmarkPlans()
    {
        // given
        BenchmarkGroupBenchmarkPlans before = new BenchmarkGroupBenchmarkPlans();
        BenchmarkGroup benchmarkGroup = new BenchmarkGroup( "group" );
        HashMap<String,String> params = new HashMap<>();
        params.put( "key", "value" );
        Benchmark benchmark = benchmarkFor( "desc", "name", LATENCY, params );
        Plan plan = testPlan();
        before.add( benchmarkGroup, benchmark, plan );
        assertThat( before.benchmarkPlans().size(), equalTo( 1 ) );
        assertThat( before.benchmarkPlans().get( 0 ).benchmarkGroup(), equalTo( benchmarkGroup ) );
        assertThat( before.benchmarkPlans().get( 0 ).benchmark(), equalTo( benchmark ) );

        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    void shouldSerializeBenchmark()
    {
        // given
        Map<String,String> params = new HashMap<>();
        params.put( "key", "value" );
        Benchmark before = benchmarkFor( "desc", "name", LATENCY, params );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    void shouldSerializeBenchmarkWithQuery()
    {
        // given
        Map<String,String> params = new HashMap<>();
        params.put( "key", "value" );
        Benchmark before = benchmarkFor( "desc", "name", LATENCY, params, "RETURN 1" );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    void shouldSerializeBenchmarkConfig()
    {
        // given
        Map<String,String> params = new HashMap<>();
        params.put( "key", "value" );
        BenchmarkConfig before = new BenchmarkConfig( params );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    void shouldSerializeBenchmarkConfigFromFile() throws IOException
    {
        // given
        File benchmarkConfig = temporaryFolder.file( "benchmark.config" ).toFile();
        try ( FileWriter fileWriter = new FileWriter( benchmarkConfig ) )
        {
            fileWriter.append( "key1=value1" );
            fileWriter.append( "\n" );
            fileWriter.append( "key2=value2" );
            fileWriter.flush();
        }

        BenchmarkConfig before = BenchmarkConfig.from( benchmarkConfig.toPath() );
        // then

        BenchmarkConfig after = (BenchmarkConfig) shouldSerializeAndDeserialize( before );

        assertThat( before.toMap().get( "key1" ), equalTo( after.toMap().get( "key1" ) ) );
        assertThat( before.toMap().get( "key2" ), equalTo( after.toMap().get( "key2" ) ) );
    }

    @Test
    void shouldSerializeBenchmarkGroup()
    {
        // given
        BenchmarkGroup before = new BenchmarkGroup( "name" );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    void shouldSerializeBenchmarkGroupBenchmark()
    {
        // given
        BenchmarkGroup benchmarkGroup = new BenchmarkGroup( "name" );
        Map<String,String> params = new HashMap<>();
        params.put( "key", "value" );
        Benchmark benchmark = benchmarkFor( "desc", "name", LATENCY, params );
        BenchmarkGroupBenchmark before = new BenchmarkGroupBenchmark( benchmarkGroup, benchmark );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    void shouldSerializeBenchmarkGroupBenchmarkMetrics()
    {
        // given
        BenchmarkGroup benchmarkGroup = new BenchmarkGroup( "name" );
        Map<String,String> params = new HashMap<>();
        params.put( "key", "value" );
        Benchmark benchmark = benchmarkFor( "desc", "name", LATENCY, params );
        Metrics metrics = getMetrics();
        AuxiliaryMetrics auxiliaryMetrics = getAuxiliaryMetrics();
        Neo4jConfig neo4jConfig = new Neo4jConfig( params );
        BenchmarkGroupBenchmarkMetrics before = new BenchmarkGroupBenchmarkMetrics();
        before.add( benchmarkGroup, benchmark, metrics, auxiliaryMetrics, neo4jConfig );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    void shouldSerializeBenchmarkMetrics()
    {
        // given
        Map<String,String> params = new HashMap<>();
        params.put( "key", "value" );
        Metrics metrics = getMetrics();
        AuxiliaryMetrics auxiliaryMetrics = getAuxiliaryMetrics();
        BenchmarkMetrics before = new BenchmarkMetrics(
                "name",
                "this is simple",
                LATENCY,
                params,
                metrics.toMap(),
                auxiliaryMetrics.toMap() );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    void shouldSerializeBenchmarks()
    {
        // given
        Map<String,String> params = new HashMap<>();
        params.put( "key", "value" );
        Benchmark benchmark1 = benchmarkFor( "desc", "name", LATENCY, params );
        Benchmarks before = new Benchmarks( benchmark1 );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    void shouldSerializeBenchmarkTool()
    {
        // given
        BenchmarkTool before = new BenchmarkTool( Repository.LDBC_BENCH, "commit", Repository.LDBC_BENCH.defaultOwner(), "3.2" );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    void shouldSerializeEnvironment()
    {
        // given
        Environment before = new Environment( "operating system", "server" );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    void shouldSerializeMetrics()
    {
        shouldSerializeAndDeserialize( getMetrics() );
        shouldSerializeAndDeserialize( getAuxiliaryMetrics() );
    }

    @Test
    void shouldSerializeNeo4j()
    {
        // given
        Project before = new Project( Repository.NEO4J, "commit", "3.3.3", COMMUNITY, "branch", "owner" );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    void shouldSerializeNeo4jConfig()
    {
        // given
        HashMap<String,String> params = new HashMap<>();
        params.put( "key", "value" );
        Neo4jConfig before = new Neo4jConfig( params );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    void shouldSerializePlanOperator()
    {
        // given
        Map<String,String> arguments = new HashMap<>();
        arguments.put( "key", "value" );
        PlanOperator before = new PlanOperator( 0, "operator type1", 1, 2, 3, arguments,
                                                emptyList(),
                                                Lists.newArrayList( new PlanOperator( 1, "operator type2", 4, 5, 6, arguments, emptyList(), emptyList() ) ) );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    void shouldSerializeJava()
    {
        // given
        Java before = new Java( "jvm", "version", "jvm args" );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    void shouldSerializeTestRun()
    {
        // given
        TestRun before = new TestRun( "id", 1, 2, 3, 1, "user" );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    void shouldSerializeTestRunWithProfile() throws IOException
    {
        // given
        TestRun before = new TestRun( "id", 1, 2, 3, 1, "user" );
        before.setArchive( "bucket/profile" );
        // then
        shouldSerializeAndDeserialize( before );
    }

    @Test
    void shouldSerializeProfiles()
    {
        // given
        ProfilerRecordings profilerRecordings = new ProfilerRecordings()
                .with( RecordingType.JFR, Parameters.NONE, "bucket/jfrName" )
                .with( RecordingType.ASYNC, Parameters.NONE, "bucket/asyncName" )
                .with( RecordingType.JFR_FLAMEGRAPH, Parameters.NONE, "bucket/jfrFlamegraph" )
                .with( RecordingType.ASYNC_FLAMEGRAPH, Parameters.NONE, "bucket/asyncFlamegraph" );

        // then
        shouldSerializeAndDeserialize( profilerRecordings );
    }

    @Test
    void shouldSerializeProfilesWithParameters()
    {
        Map<String,String> parametersMap = new HashMap<>();
        parametersMap.put( "k1", "v1" );
        parametersMap.put( "k2", "v2" );
        parametersMap.put( "k3", "v3" );
        Parameters parameters = Parameters.fromMap( parametersMap );
        // given
        ProfilerRecordings profilerRecordings = new ProfilerRecordings()
                .with( RecordingType.JFR, parameters, "bucket/jfrName" )
                .with( RecordingType.ASYNC, parameters, "bucket/asyncName" )
                .with( RecordingType.JFR_FLAMEGRAPH, parameters, "bucket/jfrFlamegraph" )
                .with( RecordingType.ASYNC_FLAMEGRAPH, parameters, "bucket/asyncFlamegraph" );

        // then
        shouldSerializeAndDeserialize( profilerRecordings );
    }

    @Test
    void shouldSerializeAnnotation()
    {
        // given
        Annotation before = new Annotation( "Comment", 0, "id", "Robert" );
        // then
        shouldSerializeAndDeserialize( before );
    }

    private Object shouldSerializeAndDeserialize( Object before )
    {
        File jsonFile = temporaryFolder.file( "file.json" ).toFile();
        JsonUtil.serializeJson( jsonFile.toPath(), before );
        Object after = JsonUtil.deserializeJson( jsonFile.toPath(), before.getClass() );
        assertThat( before, equalTo( after ) );
        return after;
    }

    private static Plan testPlan()
    {
        PlanOperator leftLeaf1 = new PlanOperator( 0, "left-leaf", 1, 2.0, 3 );
        leftLeaf1.addArgument( "a", "b" );
        PlanOperator leftLeaf2 = new PlanOperator( 1, "left-leaf", 1, 2.0, 3 );
        leftLeaf1.addArgument( "a", "b" );

        PlanOperator rightLeaf1 = new PlanOperator( 2, "right-leaf-1", 2, 3.0, 4 );
        rightLeaf1.addArgument( "a", "7" );
        rightLeaf1.addArgument( "b", "42" );
        PlanOperator rightLeaf2 = new PlanOperator( 3, "right-leaf-2", 3, 4.0, 5 );
        rightLeaf2.addArgument( "c", "pies" );

        PlanOperator left = new PlanOperator( 4, "left", 1, 1.0, 1 );
        left.addChild( leftLeaf1 );
        left.addChild( leftLeaf2 );

        PlanOperator right = new PlanOperator( 5, "right", 1, 1.0, 1 );
        right.addArgument( "cakes", "not as good as pies" );
        right.addChild( rightLeaf1 );
        right.addChild( rightLeaf2 );

        PlanOperator root = new PlanOperator( 6, "root", 0, 0.0, 0 );
        root.addArgument( "knock_knock", "who is there?" );
        root.addChild( left );
        root.addChild( right );

        return new Plan(
                "cost",
                "cost",
                "cost",
                "slotted",
                "slotted",
                "slotted",
                "3.2",
                new PlanTree( "plan description", root )
        );
    }

    private Metrics getMetrics()
    {
        return new Metrics( SECONDS, 1, 10, 5.0, 42, 2.5, 5.0, 7.5, 9.0, 9.5, 9.9, 9.99 );
    }

    private AuxiliaryMetrics getAuxiliaryMetrics()
    {
        return new AuxiliaryMetrics( "rows", 1, 10, 5.0, 42, 2.5, 5.0, 7.5, 9.0, 9.5, 9.9, 9.99 );
    }
}
