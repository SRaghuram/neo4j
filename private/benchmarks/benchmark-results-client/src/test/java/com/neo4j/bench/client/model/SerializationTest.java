/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.model;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.profiling.RecordingType;
import com.neo4j.bench.client.util.JsonUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
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
    public void shouldSerializeProfiles() throws IOException
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
    public void shouldSerializeProfilesWithParameters() throws IOException
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
