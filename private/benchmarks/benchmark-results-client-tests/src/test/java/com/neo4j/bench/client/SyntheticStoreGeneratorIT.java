/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.neo4j.bench.client.SyntheticStoreGenerator.SyntheticStoreGeneratorBuilder;
import com.neo4j.bench.client.SyntheticStoreGenerator.ToolBenchGroup;
import com.neo4j.bench.client.queries.schema.CreateSchema;
import com.neo4j.bench.client.queries.schema.VerifyStoreSchema;
import com.neo4j.bench.model.model.Repository;
import com.neo4j.harness.junit.extension.EnterpriseNeo4jExtension;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.core.AnyOf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.extension.Neo4jExtension;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static com.neo4j.bench.client.ReIndexStoreCommand.CMD_RESULTS_STORE_PASSWORD;
import static com.neo4j.bench.client.ReIndexStoreCommand.CMD_RESULTS_STORE_URI;
import static com.neo4j.bench.client.ReIndexStoreCommand.CMD_RESULTS_STORE_USER;
import static com.neo4j.bench.model.model.Repository.CAPS;
import static com.neo4j.bench.model.model.Repository.IMPORT_BENCH;
import static com.neo4j.bench.model.model.Repository.LDBC_BENCH;
import static com.neo4j.bench.model.model.Repository.MACRO_BENCH;
import static com.neo4j.bench.model.model.Repository.MICRO_BENCH;
import static com.neo4j.bench.model.model.Repository.NEO4J;
import static com.neo4j.bench.model.options.Edition.COMMUNITY;
import static com.neo4j.bench.model.options.Edition.ENTERPRISE;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SyntheticStoreGeneratorIT
{

    @RegisterExtension
    static Neo4jExtension neo4jExtension = EnterpriseNeo4jExtension.builder()
                                                                   .withConfig( BoltConnector.enabled, true )
                                                                   .withConfig( BoltConnector.encryption_level, BoltConnector.EncryptionLevel.OPTIONAL )
                                                                   .withConfig( GraphDatabaseSettings.auth_enabled, false )
                                                                   .build();

    private static final int CLIENT_RETRY_COUNT = 0;
    private static final QueryRetrier QUERY_RETRIER = new QueryRetrier( false );

    private static final String USERNAME = "neo4j";
    private static final String PASSWORD = "neo4j";

    private URI boltUri;

    @BeforeEach
    public void setUp( GraphDatabaseService databaseService )
    {
        HostnamePort address = ((GraphDatabaseAPI) databaseService).getDependencyResolver()
                                                                   .resolveDependency( ConnectorPortRegister.class ).getLocalAddress( "bolt" );
        boltUri = URI.create( "bolt://" + address.toString() );
    }

    @AfterEach
    public void cleanUpDb( GraphDatabaseService databaseService )
    {
        try ( Transaction transaction = databaseService.beginTx() )
        {
            // this is hacky HACK, needs to be fixed in Neo4jExtension
            transaction.execute( "MATCH (n) DETACH DELETE n" ).close();
            transaction.commit();
        }
    }

    @Test
    public void shouldCreateStoreWithPersonalRuns() throws Exception
    {
        Repository[] tools = {MICRO_BENCH, MACRO_BENCH, LDBC_BENCH, IMPORT_BENCH};
        ToolBenchGroup[] toolBenchGroups = Arrays.stream( tools )
                                                 .map( tool -> ToolBenchGroup.from( tool, "group-" + tool.name(), 10 ) )
                                                 .toArray( ToolBenchGroup[]::new );

        SyntheticStoreGeneratorBuilder generatorBuilder = new SyntheticStoreGeneratorBuilder()
                .withDays( 5 )
                .withResultsPerDay( 5 )
                .withBenchmarkGroups( toolBenchGroups )
                .withNeo4jVersions( "3.0.2", "3.0.1", "3.0.0" )
                .withNeo4jEditions( COMMUNITY )
                .withSettingsInConfig( 10 )
                .withOperatingSystems( "Ubuntu" )
                .withServers( "Skalleper" )
                .withJvmArgs( "-server" )
                .withJvms( "Oracle" )
                .withJvmVersions( "1.80_66" )
                .withNeo4jBranchOwners( "Foo", "Bar" )
                .withToolBranchOwners( "Hammer", "PizzaCutter" );

        SyntheticStoreGenerator.GenerationResult generationResult = generateStoreUsing( generatorBuilder.build() );

        verifySchema( generationResult );
    }

    @Test
    public void shouldCreateStoreWithExpectedSchema() throws Exception
    {
        Repository[] tools = {MICRO_BENCH, MACRO_BENCH, LDBC_BENCH, IMPORT_BENCH};
        ToolBenchGroup[] toolBenchGroups = Arrays.stream( tools )
                                                 .map( tool -> ToolBenchGroup.from( tool, "group-" + tool.name(), 10 ) )
                                                 .toArray( ToolBenchGroup[]::new );

        SyntheticStoreGeneratorBuilder generatorBuilder = new SyntheticStoreGeneratorBuilder()
                .withDays( 10 )
                .withResultsPerDay( 10 )
                .withBenchmarkGroups( toolBenchGroups )
                .withNeo4jVersions( "3.0.2", "3.0.1", "3.0.0", "2.3.4", "2.3.3", "2.3.2" )
                .withNeo4jEditions( COMMUNITY, ENTERPRISE )
                .withSettingsInConfig( 50 )
                .withOperatingSystems( "Windows", "OSX", "Ubuntu" )
                .withServers( "Skalleper", "local", "AWS", "Mattis", "Borka" )
                .withJvmArgs( "-XX:+UseG1GC -Xmx4g", "-server", "-Xmx12g" )
                .withJvms( "Oracle", "OpenJDK" )
                .withJvmVersions( "1.80_66", "1.80_12", "1.7.0_42" )
                .withPrintout( true );

        SyntheticStoreGenerator.GenerationResult generationResult = generateStoreUsing( generatorBuilder.build() );

        verifySchema( generationResult );
    }

    private SyntheticStoreGenerator.GenerationResult generateStoreUsing( SyntheticStoreGenerator generator ) throws Exception
    {
        Main.main( new String[]{"index",
                                CMD_RESULTS_STORE_USER, USERNAME,
                                CMD_RESULTS_STORE_PASSWORD, PASSWORD,
                                CMD_RESULTS_STORE_URI, boltUri.toString()} );

        try ( StoreClient client = StoreClient.connect( boltUri, USERNAME, PASSWORD, 1 ) )
        {
            QUERY_RETRIER.execute( client, new CreateSchema(), CLIENT_RETRY_COUNT );
            QUERY_RETRIER.execute( client, new VerifyStoreSchema(), 1 );
            return generator.generate( client );
        }
    }

    private void verifySchema( SyntheticStoreGenerator.GenerationResult generationResult )
    {
        try ( StoreClient client = StoreClient.connect( boltUri, USERNAME, PASSWORD, 1 ) )
        {
            QUERY_RETRIER.execute( client, new VerifyStoreSchema(), 1 );

            try ( Session session = client.session() )
            {
                // -------------------------------------------------------------
                // ------------------------ Node Checks ------------------------
                // -------------------------------------------------------------

                int testRunCount = session.run( "MATCH (:TestRun) RETURN count(*) AS c" ).next().get( "c" ).asInt();

                MatcherAssert.assertThat( "store has same number of TestRun nodes as generator claims it created",
                                          testRunCount,
                                          CoreMatchers.equalTo( generationResult.testRuns() ) );

                MatcherAssert.assertThat( "store has same number of TestRun nodes as generator was configured to create",
                                          testRunCount,
                                          CoreMatchers.equalTo( generationResult.expectedTotalTestRuns() ) );

                MatcherAssert.assertThat( "has correct number of unique Environment nodes",
                                          session.run( "MATCH (:Environment) RETURN count(*) AS c" ).next().get( "c" ).asInt(),
                                          CoreMatchers.equalTo( generationResult.environments() ) );

                MatcherAssert.assertThat( "has correct number of unique Java nodes",
                                          session.run( "MATCH (:Java) RETURN count(*) AS c" ).next().get( "c" ).asInt(),
                                          CoreMatchers.equalTo( generationResult.javas() ) );

                MatcherAssert.assertThat( "has correct number of unique Project nodes",
                                          session.run( "MATCH (:Project) RETURN count(*) AS c" ).next().get( "c" ).asInt(),
                                          CoreMatchers.equalTo( generationResult.projects() ) );

                MatcherAssert.assertThat( "has correct number of unique base Neo4jConfig nodes",
                                          session.run( "RETURN size((:TestRun)-[:HAS_CONFIG]->(:Neo4jConfig)) AS c" )
                                                 .next().get( "c" ).asInt(),
                                          CoreMatchers.equalTo( generationResult.baseNeo4jConfigs() ) );

                int toolVersionCount = session.run( "MATCH (:BenchmarkToolVersion) RETURN count(*) AS c" ).next().get( "c" ).asInt();
                MatcherAssert.assertThat( "has correct number of unique BenchmarkToolVersion nodes",
                                          toolVersionCount,
                                          CoreMatchers.equalTo( generationResult.toolVersions() ) );

                int toolCount = session.run( "MATCH (:BenchmarkTool) RETURN count(*) AS c" ).next().get( "c" ).asInt();
                MatcherAssert.assertThat( "has correct number of unique BenchmarkTool nodes",
                                          toolCount,
                                          CoreMatchers.equalTo( generationResult.tools() ) );

                int metricsCount = session.run( "MATCH (:Metrics) RETURN count(*) AS c" ).next().get( "c" ).asInt();
                MatcherAssert.assertThat( "has correct number of unique Metrics nodes",
                                          metricsCount,
                                          CoreMatchers.equalTo( generationResult.metrics() ) );

                int auxiliaryMetricsCount = session.run( "MATCH (:AuxiliaryMetrics) RETURN count(*) AS c" ).next().get( "c" ).asInt();
                MatcherAssert.assertThat( "has correct number of unique Auxiliary Metrics nodes",
                                          auxiliaryMetricsCount,
                                          CoreMatchers.equalTo( generationResult.auxiliaryMetrics() ) );

                int allMetricsCount = session.run( "MATCH (n) WHERE n:Metrics OR n:AuxiliaryMetrics RETURN count(*) AS c" )
                                             .next()
                                             .get( "c" )
                                             .asInt();
                MatcherAssert.assertThat( "has correct number of unique Auxiliary Metrics & Metrics nodes",
                                          allMetricsCount,
                                          CoreMatchers.equalTo( metricsCount + auxiliaryMetricsCount ) );

                int benchmarkGroupCount = session.run( "MATCH (:BenchmarkGroup) RETURN count(*) AS c" ).next().get( "c" ).asInt();
                MatcherAssert.assertThat( "has correct number of unique BenchmarkGroup nodes",
                                          benchmarkGroupCount,
                                          CoreMatchers.equalTo( generationResult.benchmarkGroups() ) );

                int benchmarkCount = session.run( "MATCH (:Benchmark) RETURN count(*) AS c" ).next().get( "c" ).asInt();
                MatcherAssert.assertThat( "has correct number of unique Benchmark nodes",
                                          benchmarkCount,
                                          CoreMatchers.equalTo( generationResult.benchmarks() ) );

                MatcherAssert.assertThat( "has correct number of unique Neo4jConfig nodes",
                                          session.run( "MATCH (:Neo4jConfig) RETURN count(*) AS c" ).next().get( "c" ).asInt(),
                                          CoreMatchers.equalTo( generationResult.neo4jConfigs() ) );

                verifyPersonalRuns( session, generationResult );

                int testRunAnnotations = session.run( "RETURN size((:TestRun)-[:WITH_ANNOTATION]->(:Annotation)) AS c" ).next().get( "c" ).asInt();
                MatcherAssert.assertThat( "has correct number of TestRun Annotation nodes",
                                          testRunAnnotations,
                                          CoreMatchers.equalTo( generationResult.testRunAnnotations() ) );

                int metricsAnnotations = session.run( "RETURN size((:Metrics)-[:WITH_ANNOTATION]->(:Annotation)) AS c" ).next().get( "c" ).asInt();
                MatcherAssert.assertThat( "has correct number of Metrics Annotation nodes",
                                          metricsAnnotations,
                                          CoreMatchers.equalTo( generationResult.metricsAnnotations() ) );

                MatcherAssert.assertThat( "has correct number of Annotations nodes",
                                          session.run( "MATCH (:Annotation) RETURN count(*) AS c" ).next().get( "c" ).asInt(),
                                          CoreMatchers.equalTo( testRunAnnotations + metricsAnnotations ) );

                // -------------------------------------------------------------
                // -------------------- Relationship Checks --------------------
                // -------------------------------------------------------------

                int totalInEnvironmentCount =
                        session.run( "RETURN size(()<-[:IN_ENVIRONMENT]-()) AS c" )
                               .next().get( "c" ).asInt();
                int constrainedInEnvironmentCount =
                        session.run( "RETURN size((:Environment)<-[:IN_ENVIRONMENT]-(:TestRun)) AS c" )
                               .next().get( "c" ).asInt();
                MatcherAssert.assertThat( "has correct number of IN_ENVIRONMENT relationships",
                                          totalInEnvironmentCount,
                                          CoreMatchers.allOf( CoreMatchers.equalTo( testRunCount ), CoreMatchers.equalTo( constrainedInEnvironmentCount ) ) );

                int totalWithJavaCount =
                        session.run( "RETURN size(()<-[:WITH_JAVA]-()) AS c" )
                               .next().get( "c" ).asInt();
                int constrainedWithJavaCount =
                        session.run( "RETURN size((:Java)<-[:WITH_JAVA]-(:TestRun)) AS c" )
                               .next().get( "c" ).asInt();
                MatcherAssert.assertThat( "has correct number of WITH_JAVA relationships",
                                          totalWithJavaCount,
                                          CoreMatchers.allOf( CoreMatchers.equalTo( testRunCount ), CoreMatchers.equalTo( constrainedWithJavaCount ) ) );

                int totalWithProjectCount =
                        session.run( "RETURN size(()<-[:WITH_PROJECT]-()) AS c" )
                               .next().get( "c" ).asInt();
                MatcherAssert.assertThat( "has correct number of Project",
                                          totalWithProjectCount,
                                          CoreMatchers.allOf( CoreMatchers.equalTo( totalWithProjectCount ), CoreMatchers.equalTo( testRunCount ) ) );

                int projectCount =
                        session.run( "MATCH (:Project) RETURN COUNT(*) AS c" )
                               .next().get( "c" ).asInt();
                MatcherAssert.assertThat( "has correct number of Project",
                                          totalWithProjectCount,
                                          CoreMatchers.allOf( CoreMatchers.equalTo( testRunCount ), CoreMatchers.equalTo( projectCount ) ) );

                int constrainedWithNeo4jCount =
                        session.run( "RETURN size((:Project)<-[:WITH_PROJECT]-(:TestRun)) AS c" )
                               .next().get( "c" ).asInt();
                MatcherAssert.assertThat( "has correct number of WITH_PROJECT relationships",
                                          totalWithProjectCount,
                                          CoreMatchers.allOf( CoreMatchers.equalTo( testRunCount ), CoreMatchers.equalTo( constrainedWithNeo4jCount ) ) );

                int totalHasConfigCount =
                        session.run( "RETURN size(()<-[:HAS_CONFIG]-()) AS c" )
                               .next().get( "c" ).asInt();
                MatcherAssert.assertThat( "has correct number of HAS_CONFIG relationships",
                                          totalHasConfigCount,
                                          CoreMatchers.equalTo( testRunCount + metricsCount ) );

                int testRunHasConfigCount =
                        session.run( "RETURN size((:Neo4jConfig)<-[:HAS_CONFIG]-(:TestRun)) AS c" )
                               .next().get( "c" ).asInt();
                MatcherAssert.assertThat( "has correct number of HAS_CONFIG relationships",
                                          testRunHasConfigCount,
                                          CoreMatchers.equalTo( testRunCount ) );

                int metricsHasConfigCount =
                        session.run( "RETURN size((:Neo4jConfig)<-[:HAS_CONFIG]-(:Metrics)) AS c" )
                               .next().get( "c" ).asInt();
                MatcherAssert.assertThat( "has correct number of HAS_CONFIG relationships",
                                          metricsHasConfigCount,
                                          CoreMatchers.equalTo( metricsCount ) );

                int totalWithToolCount =
                        session.run( "RETURN size(()<-[:WITH_TOOL]-()) AS c" )
                               .next().get( "c" ).asInt();
                int constrainedWithToolCount =
                        session.run( "RETURN size((:BenchmarkToolVersion)<-[:WITH_TOOL]-(:TestRun)) AS c" )
                               .next().get( "c" ).asInt();
                MatcherAssert.assertThat( "has correct number of WITH_TOOL relationships",
                                          totalWithToolCount,
                                          CoreMatchers.allOf( CoreMatchers.equalTo( toolVersionCount ), CoreMatchers.equalTo( constrainedWithToolCount ) ) );

                int totalHasMetricsCount =
                        session.run( "RETURN size(()<-[:HAS_METRICS]-()) AS c" )
                               .next().get( "c" ).asInt();
                int constrainedHasMetricsCount =
                        session.run( "RETURN size((:Metrics)<-[:HAS_METRICS]-(:TestRun)) AS c" )
                               .next().get( "c" ).asInt();
                MatcherAssert.assertThat( "has correct number of HAS_METRICS relationships",
                                          totalHasMetricsCount,
                                          CoreMatchers.allOf( CoreMatchers.equalTo( metricsCount ), CoreMatchers.equalTo( constrainedHasMetricsCount ) ) );

                int totalMetricsForCount =
                        session.run( "RETURN size(()<-[:METRICS_FOR]-()) AS c" )
                               .next().get( "c" ).asInt();
                int constrainedMetricsForCount =
                        session.run( "RETURN size((:Benchmark)<-[:METRICS_FOR]-(:Metrics)) AS c" )
                               .next().get( "c" ).asInt();
                MatcherAssert.assertThat( "has correct number of METRICS_FOR relationships",
                                          totalMetricsForCount,
                                          CoreMatchers.allOf( CoreMatchers.equalTo( metricsCount ), CoreMatchers.equalTo( constrainedMetricsForCount ) ) );

                int totalHasBenchmarkCount =
                        session.run( "RETURN size(()<-[:HAS_BENCHMARK]-()) AS c" )
                               .next().get( "c" ).asInt();
                int constrainedHasBenchmarkCount =
                        session.run( "RETURN size((:Benchmark)<-[:HAS_BENCHMARK]-(:BenchmarkGroup)) AS c" )
                               .next().get( "c" ).asInt();
                MatcherAssert.assertThat( "has correct number of HAS_BENCHMARK relationships",
                                          totalHasBenchmarkCount,
                                          CoreMatchers.allOf( CoreMatchers.equalTo( benchmarkCount ), CoreMatchers.equalTo( constrainedHasBenchmarkCount ) ) );
            }
        }
    }

    private void verifyPersonalRuns( Session session, SyntheticStoreGenerator.GenerationResult generationResult )
    {
        int personalNeo4jCount = executeCountQuery( session,
                                                    "MATCH (n:Project) " +
                                                    "WHERE NOT n.owner IN $defaultOwner " +
                                                    "RETURN count(n) AS count",
                                                    singletonMap( "defaultOwner", Arrays.asList( NEO4J.defaultOwner(), CAPS.defaultOwner() ) ) );

        List<String> nonDefaultBranchOwners = generationResult.projectBranchOwners().stream()
                                                              .filter( owner -> !owner.equals( NEO4J.defaultOwner() ) )
                                                              .filter( owner -> !owner.equals( CAPS.defaultOwner() ) )
                                                              .collect( toList() );
        if ( nonDefaultBranchOwners.isEmpty() )
        {
            assertEquals( 0, personalNeo4jCount );
        }
        else
        {
            assertEquals( generationResult.testRuns(), personalNeo4jCount );

            for ( String owner : generationResult.projectBranchOwners() )
            {
                int countForOwner = executeCountQuery( session,
                                                       "MATCH (n:Project {owner: $owner}) RETURN count(n) AS count",
                                                       singletonMap( "owner", owner ) );

                MatcherAssert.assertThat( countForOwner,
                                          AnyOf.anyOf( Matchers.greaterThanOrEqualTo( 0 ), Matchers.lessThanOrEqualTo( personalNeo4jCount ) ) );
            }
        }
    }

    private static int executeCountQuery( Session session, String query, Map<String,Object> params )
    {
        Result result = session.run( query, params );
        Record record = result.next();
        return record.get( "count" ).asInt();
    }
}
