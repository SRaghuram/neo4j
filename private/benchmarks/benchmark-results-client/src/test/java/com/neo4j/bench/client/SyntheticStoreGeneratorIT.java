/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.SyntheticStoreGenerator.Group;
import com.neo4j.bench.client.queries.CreateSchema;
import com.neo4j.bench.client.queries.DropSchema;
import com.neo4j.bench.client.queries.VerifyStoreSchema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.junit.EnterpriseNeo4jRule;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.kernel.configuration.Settings;

import static com.neo4j.bench.client.ReIndexStoreCommand.CMD_RESULTS_STORE_PASSWORD;
import static com.neo4j.bench.client.ReIndexStoreCommand.CMD_RESULTS_STORE_URI;
import static com.neo4j.bench.client.ReIndexStoreCommand.CMD_RESULTS_STORE_USER;
import static com.neo4j.bench.common.model.Repository.CAPS;
import static com.neo4j.bench.common.model.Repository.NEO4J;
import static com.neo4j.bench.common.options.Edition.COMMUNITY;
import static com.neo4j.bench.common.options.Edition.ENTERPRISE;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.junit.Assert.assertEquals;

public class SyntheticStoreGeneratorIT
{
    private final TemporaryFolder testFolder = new TemporaryFolder();

    private final Neo4jRule neo4j = new EnterpriseNeo4jRule()
            .withConfig( GraphDatabaseSettings.auth_enabled, Settings.FALSE );

    private static final int CLIENT_RETRY_COUNT = 0;
    private static final QueryRetrier QUERY_RETRIER = new QueryRetrier( false );

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( testFolder ).around( neo4j );

    private static final String USERNAME = "neo4j";
    private static final String PASSWORD = "neo4j";

    @Test
    public void shouldCreateStoreWithPersonalRuns() throws Exception
    {
        SyntheticStoreGenerator generator = new SyntheticStoreGenerator.SyntheticStoreGeneratorBuilder()
                .withDays( 5 )
                .withResultsPerDay( 10 )
                .withBenchmarkGroups( Group.from( "group1", 50 ),
                                      Group.from( "group2", 50 ) )
                .withNeo4jVersions( "3.0.2", "3.0.1", "3.0.0" )
                .withNeo4jEditions( COMMUNITY )
                .withSettingsInConfig( 10 )
                .withOperatingSystems( "Ubuntu" )
                .withServers( "Skalleper" )
                .withJvmArgs( "-server" )
                .withJvms( "Oracle" )
                .withJvmVersions( "1.80_66" )
                .withNeo4jBranchOwners( "Foo", "Bar" )
                .withToolBranchOwners( "Hammer", "PizzaCutter" )
                .build();

        SyntheticStoreGenerator.GenerationResult generationResult = generateStoreUsing( generator );

        verifySchema( generationResult, generator );
    }

    @Test
    public void shouldCreateStoreWithExpectedSchema() throws Exception
    {
        SyntheticStoreGenerator generator = new SyntheticStoreGenerator.SyntheticStoreGeneratorBuilder()
                .withDays( 10 )
                .withResultsPerDay( 10 )
                .withBenchmarkGroups( Group.from( "group1", 50 ),
                                      Group.from( "group2", 50 ),
                                      Group.from( "group3", 50 ),
                                      Group.from( "group4", 50 ) )
                .withNeo4jVersions( "3.0.2", "3.0.1", "3.0.0", "2.3.4", "2.3.3", "2.3.2" )
                .withNeo4jEditions( COMMUNITY, ENTERPRISE )
                .withSettingsInConfig( 50 )
                .withOperatingSystems( "Windows", "OSX", "Ubuntu" )
                .withServers( "Skalleper", "local", "AWS", "Mattis", "Borka" )
                .withJvmArgs( "-XX:+UseG1GC -Xmx4g", "-server", "-Xmx12g" )
                .withJvms( "Oracle", "OpenJDK" )
                .withJvmVersions( "1.80_66", "1.80_12", "1.7.0_42" )
                .withPrintout( true )
                .build();

        SyntheticStoreGenerator.GenerationResult generationResult = generateStoreUsing( generator );

        verifySchema( generationResult, generator );
    }

    private SyntheticStoreGenerator.GenerationResult generateStoreUsing( SyntheticStoreGenerator generator ) throws Exception
    {
        Main.main( new String[]{"index",
                                CMD_RESULTS_STORE_USER, USERNAME,
                                CMD_RESULTS_STORE_PASSWORD, PASSWORD,
                                CMD_RESULTS_STORE_URI, neo4j.boltURI().toString()} );

        try ( StoreClient client = StoreClient.connect( neo4j.boltURI(), USERNAME, PASSWORD, 1 ) )
        {
            QUERY_RETRIER.execute( client, new DropSchema(), CLIENT_RETRY_COUNT );
            QUERY_RETRIER.execute( client, new CreateSchema(), CLIENT_RETRY_COUNT );
            QUERY_RETRIER.execute( client, new VerifyStoreSchema(), 1 );
            return generator.generate( client );
        }
    }

    private void verifySchema( SyntheticStoreGenerator.GenerationResult generationResult, SyntheticStoreGenerator generator )
    {
        try ( StoreClient client = StoreClient.connect( neo4j.boltURI(), USERNAME, PASSWORD, 1 ) )
        {
            QUERY_RETRIER.execute( client, new VerifyStoreSchema(), 1 );

            try ( Session session = client.session() )
            {
                // -------------------------------------------------------------
                // ------------------------ Node Checks ------------------------
                // -------------------------------------------------------------

                int testRunCount = session.run( "MATCH (:TestRun) RETURN count(*) AS c" ).next().get( "c" ).asInt();

                assertThat( "has correct number of unique TestRun nodes",
                            testRunCount,
                            equalTo( generationResult.testRuns() ) );

                assertThat( "has correct number of unique Environment nodes",
                            session.run( "MATCH (:Environment) RETURN count(*) AS c" ).next().get( "c" ).asInt(),
                            equalTo( generationResult.environments() ) );

                assertThat( "has correct number of unique Java nodes",
                            session.run( "MATCH (:Java) RETURN count(*) AS c" ).next().get( "c" ).asInt(),
                            equalTo( generationResult.javas() ) );

                assertThat( "has correct number of unique Project nodes",
                            session.run( "MATCH (:Project) RETURN count(*) AS c" ).next().get( "c" ).asInt(),
                            equalTo( generationResult.projects() ) );

                assertThat( "has correct number of unique base Neo4jConfig nodes",
                            session.run( "RETURN size((:TestRun)-[:HAS_CONFIG]->(:Neo4jConfig)) AS c" )
                                   .next().get( "c" ).asInt(),
                            equalTo( generationResult.baseNeo4jConfigs() ) );

                int toolVersionCount = session.run( "MATCH (:BenchmarkToolVersion) RETURN count(*) AS c" ).next().get( "c" ).asInt();
                assertThat( "has correct number of unique BenchmarkToolVersion nodes",
                            toolVersionCount,
                            equalTo( generationResult.toolVersions() ) );

                int toolCount = session.run( "MATCH (:BenchmarkTool) RETURN count(*) AS c" ).next().get( "c" ).asInt();
                assertThat( "has correct number of unique BenchmarkTool nodes",
                            toolCount,
                            equalTo( generationResult.tools() ) );

                int metricsCount = session.run( "MATCH (:Metrics) RETURN count(*) AS c" ).next().get( "c" ).asInt();
                assertThat( "has correct number of unique Metrics nodes",
                            metricsCount,
                            equalTo( generationResult.metrics() ) );

                int benchmarkGroupCount = session.run( "MATCH (:BenchmarkGroup) RETURN count(*) AS c" ).next().get( "c" ).asInt();
                assertThat( "has correct number of unique BenchmarkGroup nodes",
                            benchmarkGroupCount,
                            equalTo( generationResult.benchmarkGroups() ) );

                int benchmarkCount = session.run( "MATCH (:Benchmark) RETURN count(*) AS c" ).next().get( "c" ).asInt();
                assertThat( "has correct number of unique Benchmark nodes",
                            benchmarkCount,
                            equalTo( generationResult.benchmarks() ) );

                assertThat( "has correct number of unique Neo4jConfig nodes",
                            session.run( "MATCH (:Neo4jConfig) RETURN count(*) AS c" ).next().get( "c" ).asInt(),
                            equalTo( generationResult.neo4jConfigs() ) );

                verifyPersonalRuns( session, generator );

                int testRunAnnotations = session.run( "RETURN size((:TestRun)-[:WITH_ANNOTATION]->(:Annotation)) AS c" ).next().get( "c" ).asInt();
                assertThat( "has correct number of TestRun Annotation nodes",
                            testRunAnnotations,
                            equalTo( generationResult.testRunAnnotations() ) );

                int metricsAnnotations = session.run( "RETURN size((:Metrics)-[:WITH_ANNOTATION]->(:Annotation)) AS c" ).next().get( "c" ).asInt();
                assertThat( "has correct number of Metrics Annotation nodes",
                            metricsAnnotations,
                            equalTo( generationResult.metricsAnnotations() ) );

                assertThat( "has correct number of Annotations nodes",
                            session.run( "MATCH (:Annotation) RETURN count(*) AS c" ).next().get( "c" ).asInt(),
                            equalTo( testRunAnnotations + metricsAnnotations ) );

                // -------------------------------------------------------------
                // -------------------- Relationship Checks --------------------
                // -------------------------------------------------------------

                int totalInEnvironmentCount =
                        session.run( "RETURN size(()<-[:IN_ENVIRONMENT]-()) AS c" )
                               .next().get( "c" ).asInt();
                int constrainedInEnvironmentCount =
                        session.run( "RETURN size((:Environment)<-[:IN_ENVIRONMENT]-(:TestRun)) AS c" )
                               .next().get( "c" ).asInt();
                assertThat( "has correct number of IN_ENVIRONMENT relationships",
                            totalInEnvironmentCount,
                            allOf( equalTo( testRunCount ), equalTo( constrainedInEnvironmentCount ) ) );

                int totalWithJavaCount =
                        session.run( "RETURN size(()<-[:WITH_JAVA]-()) AS c" )
                               .next().get( "c" ).asInt();
                int constrainedWithJavaCount =
                        session.run( "RETURN size((:Java)<-[:WITH_JAVA]-(:TestRun)) AS c" )
                               .next().get( "c" ).asInt();
                assertThat( "has correct number of WITH_JAVA relationships",
                            totalWithJavaCount,
                            allOf( equalTo( testRunCount ), equalTo( constrainedWithJavaCount ) ) );

                int totalWithProjectCount =
                        session.run( "RETURN size(()<-[:WITH_PROJECT]-()) AS c" )
                               .next().get( "c" ).asInt();
                assertThat( "has correct number of Project",
                            totalWithProjectCount,
                            allOf( equalTo( totalWithProjectCount ), equalTo( testRunCount ) ) );

                int projectCount =
                        session.run( "MATCH (:Project) RETURN COUNT(*) AS c" )
                               .next().get( "c" ).asInt();
                assertThat( "has correct number of Project",
                            totalWithProjectCount,
                            allOf( equalTo( testRunCount ), equalTo( projectCount ) ) );

                int constrainedWithNeo4jCount =
                        session.run( "RETURN size((:Project)<-[:WITH_PROJECT]-(:TestRun)) AS c" )
                               .next().get( "c" ).asInt();
                assertThat( "has correct number of WITH_PROJECT relationships",
                            totalWithProjectCount,
                            allOf( equalTo( testRunCount ), equalTo( constrainedWithNeo4jCount ) ) );

                int totalHasConfigCount =
                        session.run( "RETURN size(()<-[:HAS_CONFIG]-()) AS c" )
                               .next().get( "c" ).asInt();
                assertThat( "has correct number of HAS_CONFIG relationships",
                            totalHasConfigCount,
                            equalTo( testRunCount + metricsCount ) );

                int testRunHasConfigCount =
                        session.run( "RETURN size((:Neo4jConfig)<-[:HAS_CONFIG]-(:TestRun)) AS c" )
                               .next().get( "c" ).asInt();
                assertThat( "has correct number of HAS_CONFIG relationships",
                            testRunHasConfigCount,
                            equalTo( testRunCount ) );

                int metricsHasConfigCount =
                        session.run( "RETURN size((:Neo4jConfig)<-[:HAS_CONFIG]-(:Metrics)) AS c" )
                               .next().get( "c" ).asInt();
                assertThat( "has correct number of HAS_CONFIG relationships",
                            metricsHasConfigCount,
                            equalTo( metricsCount ) );

                int totalWithToolCount =
                        session.run( "RETURN size(()<-[:WITH_TOOL]-()) AS c" )
                               .next().get( "c" ).asInt();
                int constrainedWithToolCount =
                        session.run( "RETURN size((:BenchmarkToolVersion)<-[:WITH_TOOL]-(:TestRun)) AS c" )
                               .next().get( "c" ).asInt();
                assertThat( "has correct number of WITH_TOOL relationships",
                            totalWithToolCount,
                            allOf( equalTo( toolVersionCount ), equalTo( constrainedWithToolCount ) ) );

                int totalHasMetricsCount =
                        session.run( "RETURN size(()<-[:HAS_METRICS]-()) AS c" )
                               .next().get( "c" ).asInt();
                int constrainedHasMetricsCount =
                        session.run( "RETURN size((:Metrics)<-[:HAS_METRICS]-(:TestRun)) AS c" )
                               .next().get( "c" ).asInt();
                assertThat( "has correct number of HAS_METRICS relationships",
                            totalHasMetricsCount,
                            allOf( equalTo( metricsCount ), equalTo( constrainedHasMetricsCount ) ) );

                int totalMetricsForCount =
                        session.run( "RETURN size(()<-[:METRICS_FOR]-()) AS c" )
                               .next().get( "c" ).asInt();
                int constrainedMetricsForCount =
                        session.run( "RETURN size((:Benchmark)<-[:METRICS_FOR]-(:Metrics)) AS c" )
                               .next().get( "c" ).asInt();
                assertThat( "has correct number of METRICS_FOR relationships",
                            totalMetricsForCount,
                            allOf( equalTo( metricsCount ), equalTo( constrainedMetricsForCount ) ) );

                int totalHasBenchmarkCount =
                        session.run( "RETURN size(()<-[:HAS_BENCHMARK]-()) AS c" )
                               .next().get( "c" ).asInt();
                int constrainedHasBenchmarkCount =
                        session.run( "RETURN size((:Benchmark)<-[:HAS_BENCHMARK]-(:BenchmarkGroup)) AS c" )
                               .next().get( "c" ).asInt();
                assertThat( "has correct number of HAS_BENCHMARK relationships",
                            totalHasBenchmarkCount,
                            allOf( equalTo( benchmarkCount ), equalTo( constrainedHasBenchmarkCount ) ) );
            }
        }
    }

    private void verifyPersonalRuns( Session session, SyntheticStoreGenerator generator )
    {
        List<String> branchOwners = Lists.newArrayList( generator.neo4jBranchOwners() );

        int personalNeo4jCount = executeCountQuery( session,
                                                    "MATCH (n:Project) " +
                                                    "WHERE NOT n.owner IN $defaultOwner " +
                                                    "RETURN count(n) AS count",
                                                    singletonMap( "defaultOwner", Arrays.asList( NEO4J.defaultOwner(), CAPS.defaultOwner() ) ) );

        List<String> nonDefaultBranchOwners = branchOwners.stream()
                                                          .filter( owner -> !owner.equals( NEO4J.defaultOwner() ) )
                                                          .filter( owner -> !owner.equals( CAPS.defaultOwner() ) )
                                                          .collect( toList() );
        if ( nonDefaultBranchOwners.isEmpty() )
        {
            assertEquals( 0, personalNeo4jCount );
        }
        else
        {
            assertEquals( generator.days() * generator.resultsPerDay(), personalNeo4jCount );

            for ( String owner : generator.neo4jBranchOwners() )
            {
                int countForOwner = executeCountQuery( session,
                                                       "MATCH (n:Project {owner: $owner}) RETURN count(n) AS count",
                                                       singletonMap( "owner", owner ) );

                assertThat( countForOwner,
                            anyOf( greaterThanOrEqualTo( 0 ), lessThanOrEqualTo( personalNeo4jCount ) ) );
            }
        }
    }

    private static int executeCountQuery( Session session, String query, Map<String,Object> params )
    {
        StatementResult result = session.run( query, params );
        Record record = result.next();
        return record.get( "count" ).asInt();
    }
}
