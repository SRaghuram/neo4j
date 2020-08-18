/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness.junit.extension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.junit.extension.Neo4jExtension;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogTimeZone;
import org.neo4j.test.extension.SkipThreadLeakageGuard;
import org.neo4j.test.server.HTTP;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

@SkipThreadLeakageGuard
class EnterpriseNeo4JExtensionRegisterIT
{
    private static final String REGISTERED_TEMP_PREFIX = "registeredTemp";
    @RegisterExtension
    static Neo4jExtension neo4jExtension;

    static
    {
        neo4jExtension = EnterpriseNeo4jExtension.builder()
                                                 .withFolder( createTempDirectory() )
                                                 .withFixture( "CREATE (u:User)" )
                                                 .withConfig( GraphDatabaseSettings.db_timezone, LogTimeZone.SYSTEM )
                                                 .withFixture( graphDatabaseService ->
                {
                    try ( Transaction tx = graphDatabaseService.beginTx() )
                    {
                        tx.createNode( Label.label( "User" ) );
                        tx.commit();
                    }
                    return null;
                } )
                                                 .build();
    }

    @Test
    void neo4jAvailable( Neo4j neo4j )
    {
        assertNotNull( neo4j );
        assertThat( HTTP.GET( neo4j.httpURI().toString() ).status() ).isEqualTo( 200 );
    }

    @Test
    void graphDatabaseServiceIsAvailable( GraphDatabaseService databaseService )
    {
        assertNotNull( databaseService );
        assertDoesNotThrow( () ->
        {
            try ( Transaction transaction = databaseService.beginTx() )
            {
                transaction.createNode();
                transaction.commit();
            }
        } );
    }

    @Test
    void databaseManagementServiceIsAvailable( DatabaseManagementService managementService, GraphDatabaseService databaseService )
    {
        assertNotNull( managementService );
        assertNotNull( databaseService );
        assertSame( managementService.database( DEFAULT_DATABASE_NAME ), databaseService );
    }

    @Test
    void shouldUseSystemTimeZoneForLogging( GraphDatabaseService databaseService ) throws Exception
    {
        String currentOffset = currentTimeZoneOffsetString();

        assertThat( contentOf( "neo4j.log", databaseService ) ).contains( currentOffset );
        assertThat( contentOf( "debug.log", databaseService ) ).contains( currentOffset );
    }

    @Test
    void customExtensionWorkingDirectory( Neo4j neo4j )
    {
        assertThat( neo4j.config().get( GraphDatabaseSettings.neo4j_home ).toFile().getParentFile().getName() ).startsWith( REGISTERED_TEMP_PREFIX );
    }

    @Test
    void fixturesRegistered( Neo4j neo4j ) throws Exception
    {
        // Then
        HTTP.Response response = HTTP.POST( neo4j.httpURI().toString() + "db/neo4j/tx/commit",
                quotedJson( "{'statements':[{'statement':'MATCH (n:User) RETURN n'}]}" ) );

        assertThat( response.get( "results" ).get( 0 ).get( "data" ).size() ).isEqualTo( 2 );
    }

    private static String currentTimeZoneOffsetString()
    {
        ZoneOffset offset = OffsetDateTime.now().getOffset();
        return offset.equals( UTC ) ? "+0000" : offset.toString().replace( ":", "" );
    }

    private String contentOf( String file, GraphDatabaseService databaseService ) throws IOException
    {
        GraphDatabaseAPI api = (GraphDatabaseAPI) databaseService;
        Config config = api.getDependencyResolver().resolveDependency( Config.class );
        Path homeDir = config.get( GraphDatabaseSettings.neo4j_home );
        return Files.readString( homeDir.resolve( file ) );
    }

    private static Path createTempDirectory()
    {
        try
        {
            return Files.createTempDirectory( REGISTERED_TEMP_PREFIX );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
