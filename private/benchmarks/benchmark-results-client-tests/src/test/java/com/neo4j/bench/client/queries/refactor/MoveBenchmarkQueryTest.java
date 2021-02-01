/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries.refactor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.neo4j.bench.client.StoreClient;
import com.neo4j.bench.client.queries.schema.CreateSchema;
import com.neo4j.bench.client.queries.schema.VerifyStoreSchema;
import com.neo4j.bench.client.queries.submit.SubmitTestRun;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkConfig;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.model.model.BenchmarkTool;
import com.neo4j.bench.model.model.Environment;
import com.neo4j.bench.model.model.Java;
import com.neo4j.bench.model.model.Metrics;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.model.Project;
import com.neo4j.bench.model.model.Repository;
import com.neo4j.bench.model.model.TestRun;
import com.neo4j.bench.model.model.TestRunReport;
import com.neo4j.bench.model.options.Edition;
import com.neo4j.harness.junit.extension.EnterpriseNeo4jExtension;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.junit.extension.Neo4jExtension;

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class MoveBenchmarkQueryTest
{
    private static final Query FIND_BENCHMARKS = new Query( Resources.fileToString( "/queries/refactor/find_benchmarks.cypher" ) );
    private static final Query FIND_BENCHMARK_GROUPS = new Query( Resources.fileToString( "/queries/refactor/find_benchmark_groups.cypher" ) );

    private static final BenchmarkTool BENCHMARK_TOOL = new BenchmarkTool( Repository.MICRO_BENCH, "commit", "owner", "branch" );
    private static final String TOOL_NAME = BENCHMARK_TOOL.toolName();
    private static final String OLD_GROUP_NAME = "bolt";
    private static final BenchmarkGroup OLD_GROUP = new BenchmarkGroup( OLD_GROUP_NAME );
    private static final String NEW_GROUP_NAME = "kernel";
    private static final BenchmarkGroup NEW_GROUP = new BenchmarkGroup( NEW_GROUP_NAME );
    private static final String BENCHMARK_NAME = "ConcurrentReadWriteLabels.createDeleteLabel";

    @RegisterExtension
    static final Neo4jExtension neo4jExtension = EnterpriseNeo4jExtension.builder()
                                                                         .withConfig( GraphDatabaseSettings.auth_enabled, false )
                                                                         .withConfig( BoltConnector.enabled, true )
                                                                         .withConfig( BoltConnector.encryption_level, BoltConnector.EncryptionLevel.OPTIONAL )
                                                                         .build();

    private StoreClient storeClient;

    @BeforeEach
    public void setUp( Neo4j neo4j )
    {
        storeClient = StoreClient.connect( neo4j.boltURI(), "neo4j", "neo4j" );
        storeClient.execute( new CreateSchema() );
    }

    @AfterEach
    public void verifySchema( GraphDatabaseService databaseService )
    {
        storeClient.execute( new VerifyStoreSchema() );
        storeClient.close();
        try ( org.neo4j.graphdb.Transaction transaction = databaseService.beginTx() )
        {
            transaction.execute( "MATCH (n) DETACH DELETE n" ).close();
            transaction.commit();
        }
    }

    @Test
    public void notChangeOtherBenchmarks()
    {
        BenchmarkGroupBenchmarkMetrics metrics = new BenchmarkGroupBenchmarkMetrics();
        add( metrics, OLD_GROUP, "otherName", "X" );
        add( metrics, OLD_GROUP, "matchingNameButWithSuffix", "X" );
        add( metrics, OLD_GROUP, "matchingName", "someParameter" );
        add( metrics, NEW_GROUP, "matchingName", "alreadyInNewGroupButWithDifferentParameter" );
        submitReport( metrics );

        MoveBenchmarkQuery query = new MoveBenchmarkQuery( TOOL_NAME, OLD_GROUP, NEW_GROUP, "matchingName" );
        storeClient.execute( query );

        List<Triple<String,String,Integer>> actual = readBenchmarks();
        List<Triple<String,String,Integer>> expected = ImmutableList.of(
                Triple.of( OLD_GROUP_NAME, "otherName_(some-param,X)_(mode,LATENCY)", 1 ),
                Triple.of( OLD_GROUP_NAME, "matchingNameButWithSuffix_(some-param,X)_(mode,LATENCY)", 1 ),
                Triple.of( NEW_GROUP_NAME, "matchingName_(some-param,someParameter)_(mode,LATENCY)", 1 ),
                Triple.of( NEW_GROUP_NAME, "matchingName_(some-param,alreadyInNewGroupButWithDifferentParameter)_(mode,LATENCY)", 1 ) );
        assertThat( actual, containsInAnyOrder( expected.toArray() ) );
    }

    @Test
    public void moveToNonexistentGroup()
    {
        BenchmarkGroupBenchmarkMetrics metrics = new BenchmarkGroupBenchmarkMetrics();
        add( metrics, OLD_GROUP, BENCHMARK_NAME, "A" );
        add( metrics, OLD_GROUP, BENCHMARK_NAME, "B" );
        submitReport( metrics );

        MoveBenchmarkQuery query = new MoveBenchmarkQuery( TOOL_NAME, OLD_GROUP, NEW_GROUP, BENCHMARK_NAME );
        storeClient.execute( query );

        List<Triple<String,String,Integer>> benchmarks = readBenchmarks();
        assertThat( benchmarks, containsInAnyOrder( Triple.of( NEW_GROUP_NAME, BENCHMARK_NAME + "_(some-param,A)_(mode,LATENCY)", 1 ),
                                                    Triple.of( NEW_GROUP_NAME, BENCHMARK_NAME + "_(some-param,B)_(mode,LATENCY)", 1 ) ) );
        List<Pair<String,String>> groups = readBenchmarkGroups();
        assertThat( groups, containsInAnyOrder( Pair.of( TOOL_NAME, OLD_GROUP_NAME ), Pair.of( TOOL_NAME, NEW_GROUP_NAME ) ) );
    }

    @Test
    public void moveToExistingGroupNonexistentBenchmark()
    {
        BenchmarkGroupBenchmarkMetrics metrics = new BenchmarkGroupBenchmarkMetrics();
        add( metrics, OLD_GROUP, BENCHMARK_NAME, "A" );
        add( metrics, NEW_GROUP, BENCHMARK_NAME, "B" );
        submitReport( metrics );

        MoveBenchmarkQuery query = new MoveBenchmarkQuery( TOOL_NAME, OLD_GROUP, NEW_GROUP, BENCHMARK_NAME );
        storeClient.execute( query );

        List<Triple<String,String,Integer>> benchmarks = readBenchmarks();
        assertThat( benchmarks, containsInAnyOrder( Triple.of( NEW_GROUP_NAME, BENCHMARK_NAME + "_(some-param,A)_(mode,LATENCY)", 1 ),
                                                    Triple.of( NEW_GROUP_NAME, BENCHMARK_NAME + "_(some-param,B)_(mode,LATENCY)", 1 ) ) );
        List<Pair<String,String>> groups = readBenchmarkGroups();
        assertThat( groups, containsInAnyOrder( Pair.of( TOOL_NAME, OLD_GROUP_NAME ), Pair.of( TOOL_NAME, NEW_GROUP_NAME ) ) );
    }

    @Test
    public void moveToExistingGroupExistingBenchmark()
    {
        BenchmarkGroupBenchmarkMetrics metrics = new BenchmarkGroupBenchmarkMetrics();
        add( metrics, OLD_GROUP, BENCHMARK_NAME, "A" );
        add( metrics, NEW_GROUP, BENCHMARK_NAME, "A" );
        submitReport( metrics );

        MoveBenchmarkQuery query = new MoveBenchmarkQuery( TOOL_NAME, OLD_GROUP, NEW_GROUP, BENCHMARK_NAME );
        storeClient.execute( query );

        List<Triple<String,String,Integer>> benchmarks = readBenchmarks();
        assertThat( benchmarks, containsInAnyOrder( Triple.of( NEW_GROUP_NAME, BENCHMARK_NAME + "_(some-param,A)_(mode,LATENCY)", 2 ) ) );
        List<Pair<String,String>> groups = readBenchmarkGroups();
        assertThat( groups, containsInAnyOrder( Pair.of( TOOL_NAME, OLD_GROUP_NAME ), Pair.of( TOOL_NAME, NEW_GROUP_NAME ) ) );
    }

    private void add( BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics, BenchmarkGroup group, String simpleName, String value )
    {
        Metrics metrics = new Metrics( Metrics.MetricsUnit.latency( TimeUnit.MILLISECONDS ), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 );
        benchmarkGroupBenchmarkMetrics.add(
                group,
                Benchmark.benchmarkFor( "description", simpleName, Benchmark.Mode.LATENCY, ImmutableMap.of( "some-param", value ) ),
                metrics,
                null,
                Neo4jConfig.empty() );
    }

    private void submitReport( BenchmarkGroupBenchmarkMetrics metrics )
    {
        Project project = new Project( Repository.NEO4J, "commit", "3.5.16", Edition.ENTERPRISE, "branch", "owner" );
        TestRunReport testRunReport = new TestRunReport( new TestRun( UUID.randomUUID().toString(), 0, 0, 0, 0, "triggeredBy" ),
                                                         new BenchmarkConfig( emptyMap() ),
                                                         ImmutableSet.of( project ),
                                                         Neo4jConfig.empty(),
                                                         Environment.local(),
                                                         metrics,
                                                         BENCHMARK_TOOL,
                                                         Java.current( "" ),
                                                         Collections.emptyList(),
                                                         Collections.emptyList() );
        SubmitTestRun submitTestRun = new SubmitTestRun( testRunReport );
        storeClient.execute( submitTestRun );
    }

    private List<Triple<String,String,Integer>> readBenchmarks()
    {
        return storeClient.session()
                          .readTransaction( this::readBenchmarks );
    }

    private List<Triple<String,String,Integer>> readBenchmarks( Transaction tx )
    {
        Result result = tx.run( FIND_BENCHMARKS );
        return result.list( r -> Triple.of( getName( r, "benchmark_group" ), getName( r, "benchmark" ), r.get( "metrics_count" ).asInt() ) );
    }

    private List<Pair<String,String>> readBenchmarkGroups()
    {
        return storeClient.session()
                          .readTransaction( this::readBenchmarkGroups );
    }

    private List<Pair<String,String>> readBenchmarkGroups( Transaction tx )
    {
        Result result = tx.run( FIND_BENCHMARK_GROUPS );
        return result.list( r -> Pair.of( getName( r, "benchmark_tool" ), getName( r, "benchmark_group" ) ) );
    }

    private String getName( Record record, String entityName )
    {
        Value value = record.get( entityName );
        return value.isNull() ? null : value.get( "name" ).asString();
    }
}
