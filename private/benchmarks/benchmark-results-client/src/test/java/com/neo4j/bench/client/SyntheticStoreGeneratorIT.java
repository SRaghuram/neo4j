/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.queries.CreateSchema;
import com.neo4j.bench.client.queries.DropSchema;
import com.neo4j.bench.client.queries.VerifyStoreSchema;
import com.neo4j.bench.client.util.SyntheticStoreGenerator;
import com.neo4j.harness.junit.rule.CommercialNeo4jRule;
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
import org.neo4j.harness.junit.rule.Neo4jRule;
import org.neo4j.kernel.configuration.Settings;

import static com.neo4j.bench.client.ReIndexStoreCommand.CMD_RESULTS_STORE_PASSWORD;
import static com.neo4j.bench.client.ReIndexStoreCommand.CMD_RESULTS_STORE_URI;
import static com.neo4j.bench.client.ReIndexStoreCommand.CMD_RESULTS_STORE_USER;
import static com.neo4j.bench.client.model.Edition.COMMUNITY;
import static com.neo4j.bench.client.model.Edition.ENTERPRISE;
import static com.neo4j.bench.client.model.Repository.CAPS;
import static com.neo4j.bench.client.model.Repository.NEO4J;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.junit.Assert.assertEquals;

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

public class SyntheticStoreGeneratorIT
{
    private final TemporaryFolder testFolder = new TemporaryFolder();

    private final Neo4jRule neo4j = new CommercialNeo4jRule()
            .withConfig( GraphDatabaseSettings.auth_enabled, Settings.FALSE );

    private static final int CLIENT_RETRY_COUNT = 0;
    private static final QueryRetrier QUERY_RETRIER = new QueryRetrier();

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
                .withBenchmarkGroupCount( 2 )
                .withBenchmarkPerGroupCount( 50 )
                .withNeo4jVersions( "3.0.2", "3.0.1", "3.0.0" )
                .withNeo4jEditions( COMMUNITY )
                .withSettingsInConfig( 10 )
                .withOperatingSystems( "Ubuntu" )
                .withServers( "Skalleper" )
                .withJvmArgs( "-server" )
                .withJvms( "Oracle" )
                .withJvmVersions( "1.80_66" )
                .withNeo4jBranchOwners( "Foo", "Bar" )
                .withCapsBranchOwners( "Hat", "Helmet" )
                .withToolBranchOwners( "Hammer", "PizzaCutter" )
                .withAssertions( true )
                .build();

        generateStoreUsing( generator );

        verifySchema( generator );
    }

    @Test
    public void shouldCreateStoreWithExpectedSchema() throws Exception
    {
        SyntheticStoreGenerator generator = new SyntheticStoreGenerator.SyntheticStoreGeneratorBuilder()
                .withDays( 10 )
                .withResultsPerDay( 10 )
                .withBenchmarkGroupCount( 4 )
                .withBenchmarkPerGroupCount( 50 )
                .withNeo4jVersions( "3.0.2", "3.0.1", "3.0.0", "2.3.4", "2.3.3", "2.3.2" )
                .withNeo4jEditions( COMMUNITY, ENTERPRISE )
                .withSettingsInConfig( 50 )
                .withOperatingSystems( "Windows", "OSX", "Ubuntu" )
                .withServers( "Skalleper", "local", "AWS", "Mattis", "Borka" )
                .withJvmArgs( "-XX:+UseG1GC -Xmx4g", "-server", "-Xmx12g" )
                .withJvms( "Oracle", "OpenJDK" )
                .withJvmVersions( "1.80_66", "1.80_12", "1.7.0_42" )
                .withPrintout( true )
                .withAssertions( true )
                .build();

        generateStoreUsing( generator );

        verifySchema( generator );
    }

    private void generateStoreUsing( SyntheticStoreGenerator generator ) throws Exception
    {
        Main.main( new String[]{"index",
                                CMD_RESULTS_STORE_USER, USERNAME,
                                CMD_RESULTS_STORE_PASSWORD, PASSWORD,
                                CMD_RESULTS_STORE_URI, neo4j.boltURI().toString()} );

        try ( StoreClient client = StoreClient.connect( neo4j.boltURI(), USERNAME, PASSWORD, 1 ) )
        {
            QUERY_RETRIER.execute( client, new DropSchema(), CLIENT_RETRY_COUNT );
            QUERY_RETRIER.execute( client, new CreateSchema(), CLIENT_RETRY_COUNT );
            new QueryRetrier().execute( client, new VerifyStoreSchema(), 1 );
            generator.generate( client );
        }
    }

    private void verifySchema( SyntheticStoreGenerator generator ) throws Exception
    {
        try ( StoreClient client = StoreClient.connect( neo4j.boltURI(), USERNAME, PASSWORD, 1 ) )
        {
            new QueryRetrier().execute( client, new VerifyStoreSchema(), 1 );

            try ( Session session = client.session() )
            {
                // -------------------------------------------------------------
                // ------------------------ Node Checks ------------------------
                // -------------------------------------------------------------

                int testRunCount = session.run( "MATCH (:TestRun) RETURN count(*) AS c" ).next().get( "c" ).asInt();
                assertThat( "has correct number of unique TestRun nodes",
                            testRunCount,
                            equalTo( generator.resultCount() ) );

                // NOTE: random selection -> for small stores some server/os combinations may not get created
                int minEnvironments = Math.min( generator.servers().length, generator.operatingSystems().length );
                int maxEnvironments = generator.servers().length * generator.operatingSystems().length;
                assertThat( "has correct number of unique Environment nodes",
                            session.run( "MATCH (:Environment) RETURN count(*) AS c" ).next().get( "c" ).asInt(),
                            allOf( greaterThanOrEqualTo( minEnvironments ), lessThanOrEqualTo( maxEnvironments ) ) );

                // NOTE: random selection -> for small stores some jvm/version/args combinations may not get created
                int minJavas = 1;
                int maxJavas = generator.jvmArgs().length * generator.jvms().length * generator.jvmVersions().length;
                assertThat( "has correct number of unique Java nodes",
                            session.run( "MATCH (:Java) RETURN count(*) AS c" ).next().get( "c" ).asInt(),
                            allOf( greaterThanOrEqualTo( minJavas ), lessThanOrEqualTo( maxJavas ) ) );

                // NOTE: generator creates new Project commit for every result
                assertThat( "has correct number of unique Project nodes",
                            session.run( "MATCH (:Project) RETURN count(*) AS c" ).next().get( "c" ).asInt(),
                            equalTo( generator.resultCount() ) );

                assertThat( "has correct number of unique base Neo4jConfig nodes",
                            session.run( "RETURN size((:TestRun)-[:HAS_CONFIG]->(:Neo4jConfig)) AS c" )
                                   .next().get( "c" ).asInt(),
                            equalTo( generator.resultCount() ) );

                // NOTE: generator creates new tool commit for every result
                int toolVersionCount = session.run( "MATCH (:BenchmarkToolVersion) RETURN count(*) AS c" ).next().get( "c" ).asInt();
                assertThat( "has correct number of unique BenchmarkToolVersion nodes",
                            toolVersionCount,
                            equalTo( generator.resultCount() ) );

                // NOTE: we should not have more than the number of repositories
                int toolCount = session.run( "MATCH (:BenchmarkTool) RETURN count(*) AS c" ).next().get( "c" ).asInt();
                assertThat( "has correct number of unique BenchmarkTool nodes",
                            toolCount,
                            lessThanOrEqualTo( generator.maxNumberOfBenchmarkTools() ) );

                int metricsCount = session.run( "MATCH (:Metrics) RETURN count(*) AS c" ).next().get( "c" ).asInt();
                assertThat( "has correct number of unique Metrics nodes",
                            metricsCount,
                            equalTo( generator.resultCount() * generator.benchmarkPerGroupCount() ) );

                // NOTE: random selection -> for small stores some groups may not get created
                int benchmarkGroupCount =
                        session.run( "MATCH (:BenchmarkGroup) RETURN count(*) AS c" ).next().get( "c" ).asInt();
                int minGroups = 1;
                int maxGroups = generator.benchmarkGroupCount() * toolCount;
                assertThat( "has correct number of unique BenchmarkGroup nodes",
                            benchmarkGroupCount,
                            allOf( greaterThanOrEqualTo( minGroups ), lessThanOrEqualTo( maxGroups ) ) );

                int benchmarkCount =
                        session.run( "MATCH (:Benchmark) RETURN count(*) AS c" ).next().get( "c" ).asInt();
                assertThat( "has correct number of unique Benchmark nodes",
                            benchmarkCount,
                            equalTo( benchmarkGroupCount * generator.benchmarkPerGroupCount() ) );

                assertThat( "has correct number of unique Neo4jConfig nodes",
                            session.run( "MATCH (:Neo4jConfig) RETURN count(*) AS c" ).next().get( "c" ).asInt(),
                            equalTo( generator.resultCount() + metricsCount ) );

                verifyPersonalRuns( session, generator );

                // NOTE: random selection -> for small stores variance may be beyond asserted range
                long minTestRunAnnotations = Math.round( generator.resultCount() * 0.40 );
                long maxTestRunAnnotations = Math.round( generator.resultCount() * 0.60 );
                long testRunAnnotations =
                        session.run( "RETURN size((:TestRun)-[:WITH_ANNOTATION]->(:Annotation)) AS c" )
                               .next().get( "c" ).asLong();
                assertThat( "has correct number of TestRun Annotation nodes",
                            testRunAnnotations,
                            allOf( greaterThanOrEqualTo( minTestRunAnnotations ),
                                   lessThanOrEqualTo( maxTestRunAnnotations ) ) );

                // NOTE: random selection -> for small stores variance may be beyond asserted range
                long minMetricsAnnotations = Math.round( metricsCount * 0.40 );
                long maxMetricsAnnotations = Math.round( metricsCount * 0.60 );
                long metricsAnnotations =
                        session.run( "RETURN size((:Metrics)-[:WITH_ANNOTATION]->(:Annotation)) AS c" )
                               .next().get( "c" ).asLong();
                assertThat( "has correct number of Metrics Annotation nodes",
                            metricsAnnotations,
                            allOf( greaterThanOrEqualTo( minMetricsAnnotations ),
                                   lessThanOrEqualTo( maxMetricsAnnotations ) ) );

                assertThat( "has correct number of Annotations nodes",
                            session.run( "MATCH (:Annotation) RETURN count(*) AS c" ).next().get( "c" ).asLong(),
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

    private void verifyPersonalRuns( Session session, SyntheticStoreGenerator generator ) throws Exception
    {
        List<String> branchOwners = Lists.newArrayList( generator.neo4jBranchOwners() );
        branchOwners.addAll( Lists.newArrayList( generator.capsBranchOwners() ) );

        int personalNeo4jCount = executeCountQuery( session,
                                                    "MATCH (n:Project) " +
                                                    "WHERE NOT n.owner IN {defaultOwner} " +
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
                                                       "MATCH (n:Project {owner: {owner}}) RETURN count(n) AS count",
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
