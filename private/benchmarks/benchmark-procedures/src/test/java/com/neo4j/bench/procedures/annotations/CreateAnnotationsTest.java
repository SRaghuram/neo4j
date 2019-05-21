/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.procedures.annotations;

import com.neo4j.bench.client.QueryRetrier;
import com.neo4j.bench.client.StoreClient;
import com.neo4j.bench.client.queries.CreateSchema;
import com.neo4j.bench.client.queries.DropSchema;
import com.neo4j.bench.client.queries.VerifyStoreSchema;
import com.neo4j.bench.client.util.SyntheticStoreGenerator;
import com.neo4j.harness.junit.extension.CommercialNeo4jExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.time.Instant;
import java.util.List;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Settings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.harness.junit.extension.Neo4jExtension;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static com.neo4j.bench.client.model.Annotation.AUTHOR;
import static com.neo4j.bench.client.model.Annotation.COMMENT;
import static com.neo4j.bench.client.model.Annotation.DATE;
import static com.neo4j.bench.client.model.Annotation.EVENT_ID;
import static com.neo4j.bench.client.model.Edition.COMMUNITY;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;


public class CreateAnnotationsTest
{

    @RegisterExtension
    static Neo4jExtension neo4jExtension = CommercialNeo4jExtension.builder()
        .withConfig( GraphDatabaseSettings.auth_enabled, Settings.FALSE )
        .withProcedure( CreateAnnotation.class )
        .build();

    private static final QueryRetrier QUERY_RETRIER = new QueryRetrier();

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
        // this is hacky HACK, needs to be fixed in Neo4jExtension
        databaseService.execute( "MATCH (n) DETACH DELETE n" ).close();
    }

    @Test
    public void shouldCreateTestRunAnnotations() throws Exception
    {
        SyntheticStoreGenerator generator = new SyntheticStoreGenerator.SyntheticStoreGeneratorBuilder()
                .withDays( 10 )
                .withResultsPerDay( 10 )
                .withBenchmarkGroupCount( 1 )
                .withBenchmarkPerGroupCount( 1 )
                .withNeo4jVersions( "3.3.0" )
                .withNeo4jEditions( COMMUNITY )
                .withSettingsInConfig( 10 )
                .withOperatingSystems( "Ubuntu" )
                .withServers( "Skalleper1" )
                .withJvmArgs( "-server" )
                .withJvms( "Oracle" )
                .withJvmVersions( "1.80_66" )
                .withNeo4jBranchOwners( "foo", "bar" )
                .withAssertions( true )
                .build();
        generateStoreUsing( generator );

        try ( StoreClient client = StoreClient.connect( boltUri, USERNAME, PASSWORD ) )
        {
            try ( Session session = client.session() )
            {
                long expectedAnnotationCount = generator.days() * generator.resultsPerDay();
                long testRunCount = session.run( "MATCH (tr:TestRun) RETURN count(tr) AS c" ).next().get( "c" ).asLong();

                long now = Instant.now().toEpochMilli();

                String comment = "this is a comment with words";
                String author = "Alex";
                StatementResult result = session.run(
                        "MATCH (tr:TestRun)\n" + "CALL bench.createTestRunAnnotation(tr.id,'" + comment + "','" + author + "')\n" + "YIELD annotation\n" +
                                "RETURN annotation" );

                List<Record> records = result.list();
                assertThat( (long) records.size(), equalTo( testRunCount ) );
                assertThat( (long) records.size(), equalTo( expectedAnnotationCount ) );
                assertAnnotations( records, now, comment, author );
            }
            QUERY_RETRIER.execute( client, new VerifyStoreSchema() );
        }
    }

    @Test
    public void shouldCreateMetricsAnnotations() throws Exception
    {
        SyntheticStoreGenerator generator = new SyntheticStoreGenerator.SyntheticStoreGeneratorBuilder()
                .withDays( 10 )
                .withResultsPerDay( 10 )
                .withBenchmarkGroupCount( 1 )
                .withBenchmarkPerGroupCount( 1 )
                .withNeo4jVersions( "3.3.0" )
                .withNeo4jEditions( COMMUNITY )
                .withSettingsInConfig( 10 )
                .withOperatingSystems( "Ubuntu" )
                .withServers( "Skalleper1" )
                .withJvmArgs( "-server" )
                .withJvms( "Oracle" )
                .withJvmVersions( "1.80_66" )
                .withNeo4jBranchOwners( "Foo", "bar" )
                .withAssertions( true )
                .build();
        generateStoreUsing( generator );
        try ( StoreClient client = StoreClient.connect( boltUri, USERNAME, PASSWORD ) )
        {
            try ( Session session = client.session() )
            {
                long expectedAnnotationCount =
                        generator.days() * generator.resultsPerDay() * generator.benchmarkGroupCount() * generator.benchmarkPerGroupCount();
                long metricsCount = session.run( "MATCH (m:Metrics) RETURN count(m) AS c" ).next().get( "c" ).asLong();

                long now = Instant.now().toEpochMilli();

                String comment = "this is a comment with words";
                String author = "Alex";
                StatementResult result =
                        session.run( "MATCH (tr:TestRun)-[:HAS_METRICS]->(m:Metrics),\n" + "      (m)-[:METRICS_FOR]->(b:Benchmark),\n" +
                        "      (b)<-[:HAS_BENCHMARK]-(bg:BenchmarkGroup)\n" + "CALL bench.createMetricsAnnotation(tr.id,b.name,bg.name,'" + comment + "','" +
                        author + "')\n" + "YIELD annotation\n" + "RETURN annotation" );

                List<Record> records = result.list();
                assertThat( (long) records.size(), equalTo( metricsCount ) );
                assertThat( (long) records.size(), equalTo( expectedAnnotationCount ) );
                assertAnnotations( records, now, comment, author );
            }
            QUERY_RETRIER.execute( client, new VerifyStoreSchema() );
        }
    }

    private void assertAnnotations( List<Record> records,
            long now,
            String expectedComment,
            String expectedAuthor )
    {
        records.forEach( record ->
        {
            assertThat( record.get( "annotation" ).asNode().get( COMMENT ).asString(), equalTo( expectedComment ) );
            assertThat( record.get( "annotation" ).asNode().get( AUTHOR ).asString(), equalTo( expectedAuthor ) );
            // should be more or less now
            assertThat( record.get( "annotation" ).asNode().get( DATE ).asLong(),
                    allOf( greaterThanOrEqualTo( now ), lessThanOrEqualTo( now + MINUTES.toMillis( 1 ) ) ) );
            // should be auto-generated UUID
            assertThat( record.get( "annotation" ).asNode().get( EVENT_ID ).asString(), notNullValue() );
        } );
    }

    private void generateStoreUsing( SyntheticStoreGenerator generator ) throws Exception
    {
        try ( StoreClient client = StoreClient.connect( boltUri, USERNAME, PASSWORD ) )
        {
            QUERY_RETRIER.execute( client, new DropSchema() );
            QUERY_RETRIER.execute( client, new CreateSchema() );
            QUERY_RETRIER.execute( client, new VerifyStoreSchema() );
            generator.generate( client );
            QUERY_RETRIER.execute( client, new VerifyStoreSchema() );
        }
    }
}
