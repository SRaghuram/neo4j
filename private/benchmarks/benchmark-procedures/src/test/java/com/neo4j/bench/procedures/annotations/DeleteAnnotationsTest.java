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
import com.neo4j.common.util.SyntheticStoreGenerator;
import com.neo4j.harness.junit.extension.CommercialNeo4jExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.harness.junit.extension.Neo4jExtension;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static com.neo4j.bench.client.model.Edition.COMMUNITY;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.configuration.SettingValueParsers.FALSE;

public class DeleteAnnotationsTest
{

    @RegisterExtension
    static Neo4jExtension neo4jExtension = CommercialNeo4jExtension.builder()
        .withConfig( GraphDatabaseSettings.auth_enabled, FALSE )
        .withProcedure( CreateAnnotation.class )
        .withProcedure( DeleteAnnotation.class )
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
                .withNeo4jBranchOwners( "Foo", "Bar" )
                .withAssertions( true )
                .build();
        generateStoreUsing( generator );
        try ( StoreClient client = StoreClient.connect( boltUri, USERNAME, PASSWORD ) )
        {

            QUERY_RETRIER.execute( client, new VerifyStoreSchema() );

            try ( Session session = client.session() )
            {
                long annotationCount1 = session.run( "MATCH (a:Annotation) RETURN count(a) AS c" ).next().get( "c" ).asLong();

                System.out.println( annotationCount1 );

                StatementResult result1 = session.run(
                        "MATCH (a:Annotation) \n" + "WITH a LIMIT 1 \n" + "CALL bench.deleteAnnotation(a.date,a.comment,a.author,a.event_id)\n" + "RETURN 0" );

                // TODO counters seem to be wrong. seems like product bug. uncomment when fixed
    //            SummaryCounters summaryCounters1 = result1.consume().counters();
    //            assertThat( summaryCounters1.nodesDeleted(), equalTo( 1 ) );
    //            assertThat( summaryCounters1.relationshipsDeleted(), equalTo( 1 ) );

                long annotationCount2 = session.run( "MATCH (a:Annotation) RETURN count(a) AS c" ).next().get( "c" ).asLong();

                assertThat( annotationCount1 - annotationCount2, equalTo( 1L ) );

                StatementResult result2 =
                        session.run( "MATCH (a:Annotation) \n" + "CALL bench.deleteAnnotation(a.date,a.comment,a.author,a.event_id)\n" + "RETURN 0" );

                // TODO counters seem to be wrong. seems like product bug. uncomment when fixed
    //            SummaryCounters summaryCounters2 = result2.consume().counters();
    //            assertThat( summaryCounters2.nodesDeleted(), equalTo( annotationCount2 ) );
    //            assertThat( summaryCounters2.relationshipsDeleted(), equalTo( annotationCount2 ) );

                long annotationCount3 = session.run( "MATCH (a:Annotation) RETURN count(a) AS c" ).next().get( "c" ).asLong();

                assertThat( annotationCount3, equalTo( 0L ) );
            }
        }
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
