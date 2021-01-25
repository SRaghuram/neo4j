/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.utils.DriverUtils;
import com.neo4j.utils.TestFabric;
import com.neo4j.utils.TestFabricFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import org.neo4j.driver.Driver;

import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class CatalogManagementTest
{
    private Driver clientDriver;
    private TestFabric testFabric;
    private DriverUtils driverUtils;

    @BeforeEach
    void beforeEach()
    {
        testFabric = new TestFabricFactory()
                .withFabricDatabase( "mega" )
                .withShards( "remote1", "remote2" )
                .build();

        clientDriver = testFabric.routingClientDriver();
        driverUtils = new DriverUtils( "mega" );
    }

    @AfterEach
    void afterEach()
    {
        testFabric.close();
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
                    session.run( "SHOW DATABASES YIELD name, currentStatus" ).stream()
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
