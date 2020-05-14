/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dmbs.database;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseLimitReachedException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings.maxNumberOfDatabases;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.logging.LogAssertions.assertThat;

@TestDirectoryExtension
class MultiDatabaseManagerIT
{
    private static final String CUSTOM_DATABASE_NAME = "customdatabasename";

    @Inject
    private TestDirectory testDirectory;
    private AssertableLogProvider logProvider;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        logProvider = new AssertableLogProvider( true );
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homeDir() )
                .setInternalLogProvider( logProvider )
                .setConfig( default_database, CUSTOM_DATABASE_NAME )
                .setConfig( maxNumberOfDatabases, 5L )
                .build();
    }

    @AfterEach
    void tearDown()
    {
        managementService.shutdown();
    }

    @Test
    void restrictDatabaseCreation()
    {
        for ( int i = 0; i < 3; i++ )
        {
            managementService.createDatabase( "database" + i );
        }
        DatabaseLimitReachedException exception = assertThrows( DatabaseLimitReachedException.class,
                () -> managementService.createDatabase( "any" ) );
        assertThat( getRootCause( exception ).getMessage() ).contains(
                "The total limit of databases is already reached. To create more you need to either drop databases or change the" +
                        " limit via the config setting 'dbms.max_databases'" );
    }

    @Test
    void disallowMaxNumberOfDatabaseLowerThanTwo()
    {
        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class, () ->
        {
            new TestEnterpriseDatabaseManagementServiceBuilder( new File( "testFileForMaxDbTest" ) )
                    .setInternalLogProvider( logProvider )
                    .setConfig( default_database, CUSTOM_DATABASE_NAME )
                    .setConfig( maxNumberOfDatabases, 1L )
                    .build();
        } );

        assertThat( exception.getMessage() ).contains( "Failed to validate '1' for 'dbms.max_databases': minimum allowed value is 2" );
    }

    @Test
    void allowCreationOfDatabaseAfterDrop()
    {
        for ( int i = 0; i < 3; i++ )
        {
            managementService.createDatabase( "database" + i );
        }
        assertThrows( DatabaseLimitReachedException.class, () -> managementService.createDatabase( "any" ) );
        managementService.dropDatabase( "database0" );

        assertDoesNotThrow( () -> managementService.createDatabase( "any" ) );
    }

    @Test
    void restrictDatabaseCreationWhenDatabasesAreStopped()
    {
        for ( int i = 0; i < 3; i++ )
        {
            managementService.createDatabase( "database" + i );
        }

        for ( int i = 0; i < 3; i++ )
        {
            managementService.shutdownDatabase( "database" + i );
        }
        assertThrows( DatabaseLimitReachedException.class, () -> managementService.createDatabase( "any" ) );
    }

    @Test
    void createDatabase() throws DatabaseExistsException
    {
        String testDatabaseName = "testdatabase";
        managementService.createDatabase( testDatabaseName );
        GraphDatabaseService createdDatabase = managementService.database( testDatabaseName );

        assertNotNull( createdDatabase );
        assertEquals( "testdatabase", createdDatabase.databaseName() );
    }

    @Test
    void failToCreateDatabasesWithSameName() throws DatabaseExistsException
    {
        String uniqueDatabaseName = "uniqueDatabaseName";
        managementService.createDatabase( uniqueDatabaseName );

        assertThrows( DatabaseExistsException.class, () -> managementService.createDatabase( uniqueDatabaseName ) );
        assertThrows( DatabaseExistsException.class, () -> managementService.createDatabase( uniqueDatabaseName ) );
        assertThrows( DatabaseExistsException.class, () -> managementService.createDatabase( uniqueDatabaseName ) );
        assertThrows( DatabaseExistsException.class, () -> managementService.createDatabase( uniqueDatabaseName ) );
    }

    @Test
    void failToStartUnknownDatabase()
    {
        String unknownDatabase = "unknownDatabase";
        assertThrows( DatabaseNotFoundException.class, () -> managementService.shutdownDatabase( unknownDatabase ) );
    }

    @Test
    void failToStartDroppedDatabase() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String databaseToDrop = "databaseToDrop";
        managementService.createDatabase( databaseToDrop );
        managementService.dropDatabase( databaseToDrop );

        assertThrows( DatabaseNotFoundException.class, () -> managementService.startDatabase( databaseToDrop ) );
    }

    @Test
    void startStartedDatabase() throws DatabaseExistsException
    {
        String multiStartDatabase = "multiStartDatabase";
        managementService.createDatabase( multiStartDatabase );

        assertDoesNotThrow( () -> managementService.startDatabase( multiStartDatabase ) );
        assertDoesNotThrow( () -> managementService.startDatabase( multiStartDatabase ) );
        assertDoesNotThrow( () -> managementService.startDatabase( multiStartDatabase ) );
        assertDoesNotThrow( () -> managementService.startDatabase( multiStartDatabase ) );
    }

    @Test
    void stopStartDatabase() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String startStopDatabaseName = "startStopDatabase";
        managementService.createDatabase( startStopDatabaseName );
        for ( int i = 0; i < 10; i++ )
        {
            managementService.shutdownDatabase( startStopDatabaseName );
            GraphDatabaseService database = managementService.database( startStopDatabaseName );
            assertFalse( database.isAvailable( 0 ) );
            managementService.startDatabase( startStopDatabaseName );
            assertTrue( database.isAvailable( 0 ) );
        }
    }

    @Test
    void failToCreateDatabaseWithStoppedDatabaseName() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String stoppedDatabaseName = "stoppedDatabase";
        managementService.createDatabase( stoppedDatabaseName );

        managementService.shutdownDatabase( stoppedDatabaseName );

        assertThrows( DatabaseExistsException.class, () -> managementService.createDatabase( stoppedDatabaseName ) );
    }

    @Test
    void stopStoppedDatabaseIsFine() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String stoppedDatabaseName = "stoppedDatabase";

        managementService.createDatabase( stoppedDatabaseName );
        managementService.shutdownDatabase( stoppedDatabaseName );

        assertDoesNotThrow( () -> managementService.shutdownDatabase( stoppedDatabaseName ) );
    }

    @Test
    void recreateDatabaseWithSameName() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String databaseToRecreate = "databaseToRecreate";

        managementService.createDatabase( databaseToRecreate );

        managementService.dropDatabase( databaseToRecreate );
        assertThrows( DatabaseNotFoundException.class, () -> managementService.database( databaseToRecreate ) );

        assertDoesNotThrow( () -> managementService.createDatabase( databaseToRecreate ) );
        assertNotNull( managementService.database( databaseToRecreate ) );
    }

    @Test
    void dropStartedDatabase() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String dropStartedDatabaseName = "dropStarted";

        managementService.createDatabase( dropStartedDatabaseName );
        managementService.dropDatabase( dropStartedDatabaseName );
        assertThrows( DatabaseNotFoundException.class, () -> managementService.database( dropStartedDatabaseName ) );
    }

    @Test
    void dropStoppedDatabase() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String stoppedDatabase = "stoppedDatabase";

        managementService.createDatabase( stoppedDatabase );
        managementService.shutdownDatabase( stoppedDatabase );

        managementService.dropDatabase( stoppedDatabase );
        assertThrows( DatabaseNotFoundException.class, () -> managementService.database( stoppedDatabase ) );
    }

    @Test
    void dropRemovesDatabaseFiles() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String databaseToDrop = "databaseToDrop";
        managementService.createDatabase( databaseToDrop );
        GraphDatabaseFacade database = (GraphDatabaseFacade) managementService.database( databaseToDrop );

        DatabaseLayout databaseLayout = database.databaseLayout();
        assertNotEquals( databaseLayout.databaseDirectory(), databaseLayout.getTransactionLogsDirectory() );
        assertTrue( databaseLayout.databaseDirectory().exists() );
        assertTrue( databaseLayout.getTransactionLogsDirectory().exists() );

        managementService.dropDatabase( databaseToDrop );
        assertFalse( databaseLayout.databaseDirectory().exists() );
        assertFalse( databaseLayout.getTransactionLogsDirectory().exists() );
    }

    @Test
    void stopDoesNotRemovesDatabaseFiles() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String databaseToStop = "databaseToStop";
        managementService.createDatabase( databaseToStop );
        GraphDatabaseFacade database = (GraphDatabaseFacade) managementService.database( databaseToStop );

        DatabaseLayout databaseLayout = database.databaseLayout();
        assertTrue( databaseLayout.getTransactionLogsDirectory().exists() );
        assertTrue( databaseLayout.databaseDirectory().exists() );

        managementService.shutdownDatabase( databaseToStop );
        assertTrue( databaseLayout.databaseDirectory().exists() );
        assertTrue( databaseLayout.getTransactionLogsDirectory().exists() );
    }

    @Test
    void failToDropUnknownDatabase()
    {
        String unknownDatabase = "unknownDatabase";
        assertThrows( DatabaseNotFoundException.class, () -> managementService.dropDatabase( unknownDatabase ) );
    }

    @Test
    void failToStopUnknownDatabase()
    {
        String unknownDatabase = "unknownDatabase";
        assertThrows( DatabaseNotFoundException.class, () -> managementService.shutdownDatabase( unknownDatabase ) );
    }

    @Test
    void failToStopDroppedDatabase() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String testDatabase = "testDatabase";
        managementService.createDatabase( testDatabase );
        managementService.dropDatabase( testDatabase );
        assertThrows( DatabaseNotFoundException.class, () -> managementService.shutdownDatabase( testDatabase ) );
    }

    @Test
    void lookupNotExistingDatabase()
    {
        String testDatabase = "testdatabase";
        assertThrows( DatabaseNotFoundException.class, () -> managementService.database( testDatabase ) );
    }

    @Test
    void lookupExistingDatabase()
    {
        assertNotNull( managementService.database( CUSTOM_DATABASE_NAME ) );
    }

    @Test
    void createAndshutdownDatabase() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String databaseToShutdown = "databaseToShutdown";
        managementService.createDatabase( databaseToShutdown );

        var databaseLookup = managementService.database( databaseToShutdown );
        assertNotNull( databaseLookup );

        managementService.shutdownDatabase( databaseToShutdown );
        assertNotNull( managementService.database( databaseToShutdown ) );
    }

    @Test
    void logAboutDatabaseCreationAndStop() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String logTestDb = "logTestDb";
        managementService.createDatabase( logTestDb );
        managementService.shutdownDatabase( logTestDb );
        assertThat( logProvider ).containsMessages( "Creating 'logtestdb' database.", "Stop 'logtestdb' database." );
    }

    @Test
    void listAvailableDatabases() throws DatabaseExistsException
    {
        var initialDatabases = managementService.listDatabases();
        assertEquals( 2, initialDatabases.size() );
        assertTrue( initialDatabases.contains( CUSTOM_DATABASE_NAME ) );
        String myAnotherDatabase = "myanotherdatabase";
        String aMyAnotherDatabase = "amyanotherdatabase";
        managementService.createDatabase( myAnotherDatabase );
        managementService.createDatabase( aMyAnotherDatabase );
        var postCreationDatabases = managementService.listDatabases();
        assertEquals( 4, postCreationDatabases.size() );

        assertThat( postCreationDatabases ).containsExactly( aMyAnotherDatabase, CUSTOM_DATABASE_NAME, myAnotherDatabase, SYSTEM_DATABASE_NAME );
    }
}
