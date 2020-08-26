/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import com.neo4j.test.driver.DriverExtension;
import com.neo4j.test.driver.DriverFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.driver.SessionConfig.forDatabase;

@TestDirectoryExtension
@DriverExtension
class MultiDatabaseBoltIT
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private DriverFactory driverFactory;
    private DatabaseManagementService managementService;

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @BeforeEach
    void setup()
    {
        managementService = createManagementService();
    }

    @Test
    void failToStartQueryExecutionOnFailedDatabase() throws IOException
    {
        String databaseName = "testDatabase";
        prepareEmptyDatabase( databaseName );

        TransientException transientException = assertThrows( TransientException.class, () ->
        {
            try ( var driver = driverFactory.graphDatabaseDriver( boltAddress() );
                  var session = driver.session( forDatabase( databaseName ) ) )
            {
                session.run( "CREATE (n)" ).consume();
            }
        } );
        assertThat( transientException.getMessage() ).isEqualTo( "Database 'testDatabase' is unavailable." );
    }

    @Test
    void shouldBeAbleToCreateMultipleDatabasesUsingCypher() throws IOException
    {
        assertDatabasesNotFound( "foo", "bar" );
        try ( var driver = driverFactory.graphDatabaseDriver( boltAddress() );
              var system = driver.session( forDatabase( SYSTEM_DATABASE_NAME ) ) )
        {
            system.run( "CREATE DATABASE foo" ).consume();
            assertDatabasesFound( "foo" );
            assertDatabasesNotFound( "bar" );
            system.run( "CREATE DATABASE bar" ).consume();
            assertDatabasesFound( "foo", "bar" );
        }
    }

    @Test
    void shouldFailToCreateExistingDatabaseWithCypher() throws IOException
    {
        assertDatabasesNotFound( "foo" );
        try ( var driver = driverFactory.graphDatabaseDriver( boltAddress() );
              var system = driver.session( forDatabase( SYSTEM_DATABASE_NAME ) ) )
        {
            system.run( "CREATE DATABASE foo" ).consume();
            assertDatabasesFound( "foo" );
            var exception = assertThrows( ClientException.class, () -> system.run( "CREATE DATABASE foo" ).consume() );
            assertThat( exception.getMessage() ).isEqualTo( "Failed to create the specified database 'foo': Database already exists." );
            assertDatabasesFound( "foo" );
        }
    }

    @Test
    void shouldBeAbleToCreateDatabaseIfNotExists() throws IOException
    {
        assertDatabasesNotFound( "foo", "bar" );
        try ( var driver = driverFactory.graphDatabaseDriver( boltAddress() );
              var system = driver.session( forDatabase( SYSTEM_DATABASE_NAME ) ) )
        {
            system.run( "CREATE DATABASE foo IF NOT EXISTS" ).consume();
            assertDatabasesFound( "foo" );
            assertDatabasesNotFound( "bar" );
            system.run( "CREATE DATABASE foo IF NOT EXISTS" ).consume();
            assertDatabasesFound( "foo" );
            assertDatabasesNotFound( "bar" );
            system.run( "CREATE DATABASE bar IF NOT EXISTS" ).consume();
            assertDatabasesFound( "foo", "bar" );
            system.run( "CREATE DATABASE bar IF NOT EXISTS" ).consume();
            assertDatabasesFound( "foo", "bar" );
        }
    }

    private void assertDatabasesFound( String... names )
    {
        for ( String name : names )
        {
            managementService.database( name );
        }
    }

    private void assertDatabasesNotFound( String... names )
    {
        for ( String name : names )
        {
            DatabaseNotFoundException exception = assertThrows( DatabaseNotFoundException.class, () ->
            {
                managementService.database( name );
            } );
            assertThat( exception.getMessage() ).isEqualTo( name );
        }
    }

    private void prepareEmptyDatabase( String databaseName ) throws IOException
    {
        managementService.createDatabase( databaseName );
        var databaseApi = (GraphDatabaseAPI) managementService.database( databaseName );
        DatabaseLayout testDatabaseLayout = databaseApi.databaseLayout();
        tearDown();
        fileSystem.deleteRecursively( testDatabaseLayout.getTransactionLogsDirectory() );
        setup();
    }

    private String boltAddress()
    {
        var db = (GraphDatabaseAPI) managementService.database( SYSTEM_DATABASE_NAME );
        ConnectorPortRegister portRegister = db.getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
        return "bolt://" + portRegister.getLocalAddress( "bolt" );
    }

    private DatabaseManagementService createManagementService()
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() )
                .setConfig( BoltConnector.enabled, true )
                .setConfig( BoltConnector.listen_address, new SocketAddress( "localhost", 0 ) )
                .build();
    }
}
