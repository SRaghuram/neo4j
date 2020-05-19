/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.utils.DriverUtils;
import com.neo4j.utils.ProxyFunctions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.exceptions.KernelException;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;
import org.neo4j.kernel.api.procedure.GlobalProcedures;

import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

@ExtendWith( FabricEverywhereExtension.class )
class CatalogManagementTest
{
    private Driver clientDriver;
    private TestServer testServer;
    private Neo4j shard0;
    private Neo4j shard1;
    private DriverUtils driverUtils;

    @BeforeEach
    void beforeEach() throws KernelException
    {

        shard0 = Neo4jBuilders.newInProcessBuilder().build();
        shard1 = Neo4jBuilders.newInProcessBuilder().build();

        var configProperties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", shard0.boltURI().toString(),
                "fabric.graph.0.name", "remote1",
                "fabric.graph.1.uri", shard1.boltURI().toString(),
                "fabric.graph.1.name", "remote2",
                "fabric.driver.connection.encrypted", "false",
                "dbms.connector.bolt.listen_address", "0.0.0.0:0",
                "dbms.connector.bolt.enabled", "true"
        );

        var config = Config.newBuilder()
                           .setRaw( configProperties )
                           .build();
        testServer = new TestServer( config );

        testServer.start();

        var globalProceduresRegistry = testServer.getDependencies().resolveDependency( GlobalProcedures.class );
        globalProceduresRegistry
                .registerFunction( ProxyFunctions.class );
        globalProceduresRegistry
                .registerProcedure( ProxyFunctions.class );

        clientDriver = GraphDatabase.driver(
                testServer.getBoltRoutingUri(),
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                                       .withoutEncryption()
                                       .withMaxConnectionPoolSize( 3 )
                                       .build() );

        driverUtils = new DriverUtils( "mega" );
    }

    @AfterEach
    void afterEach()
    {
        List.<Runnable>of(
                () -> testServer.stop(),
                () -> clientDriver.close(),
                () -> shard0.close(),
                () -> shard1.close()
        ).parallelStream().forEach( Runnable::run );
    }

    @Test
    void testLocalDatabaseChanges()
    {
        runSystemCommand( "CREATE DATABASE dbA" );
        awaitStatus( "dbA", "online" );

        verifyEntryPresent( "dbA" );
        verifyEntryPresent( "mega.remote1" );
        verifyEntryPresent( "mega.remote1" );

        runSystemCommand( "CREATE DATABASE dbB" );
        awaitStatus( "dbB", "online"  );

        verifyEntryPresent( "dbA" );
        verifyEntryPresent( "dbB" );
        verifyEntryPresent( "mega.remote1" );
        verifyEntryPresent( "mega.remote2" );

        runSystemCommand( "STOP DATABASE dbB" );
        awaitStatus( "dbB", "offline"  );

        verifyEntryPresent( "dbA" );
        verifyEntryNotPresent( "dbB", "The database is not currently available to serve your request" );
        verifyEntryPresent( "mega.remote1" );
        verifyEntryPresent( "mega.remote2" );

        runSystemCommand( "START DATABASE dbB" );
        awaitStatus( "dbB", "online"  );

        verifyEntryPresent( "dbA" );
        verifyEntryPresent( "dbB" );
        verifyEntryPresent( "mega.remote1" );
        verifyEntryPresent( "mega.remote2" );

        runSystemCommand( "DROP DATABASE dbB" );

        verifyEntryPresent( "dbA" );
        verifyEntryNotPresent( "dbB", "Catalog entry not found: dbB" );
        verifyEntryPresent( "mega.remote1" );
        verifyEntryPresent( "mega.remote2" );
    }

    private void verifyEntryPresent( String graphName )
    {
        driverUtils.doInSession( clientDriver, session -> session.run( "USE " + graphName + " RETURN 1" ).list() );
    }

    private void verifyEntryNotPresent( String graphName, String expectedMessage )
    {
        assertThat( catchThrowable(
                () -> driverUtils.doInSession( clientDriver, session -> session.run( "USE " + graphName + " RETURN 1" ).list() )
        ) ).hasMessageContaining( expectedMessage );
    }

    private void runSystemCommand( String command )
    {
        driverUtils.doInSession( clientDriver, session -> session.run( command ).list() );
    }

    private void awaitStatus( String databaseName, String status )
    {
        var start = Instant.now();

        while ( true )
        {
            var currentStatus = driverUtils.inSession( clientDriver, session ->
                    session.run( "SHOW DATABASES" ).stream()
                           .filter( record -> databaseName.toLowerCase().equals( record.get( "name" ).asString() ) )
                           .map( record -> record.get( "currentStatus" ).asString() )
                           .findAny()
                           .get()
            );

            if ( status.equals( currentStatus ) )
            {
                return;
            }

            if ( Duration.between( start, Instant.now() ).toMinutes() > 1 )
            {
                throw new IllegalStateException( "Failed to wait for desired status " + status + "; Current status: " + currentStatus );
            }

            try
            {
                Thread.sleep( 1 );
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
}
