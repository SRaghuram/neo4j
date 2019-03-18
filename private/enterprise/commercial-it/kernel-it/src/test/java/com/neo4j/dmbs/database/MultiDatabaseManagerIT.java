/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dmbs.database;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseExistsException;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.DatabaseManagerException;
import org.neo4j.dbms.database.DatabaseNotFoundException;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@ExtendWith( TestDirectoryExtension.class )
class MultiDatabaseManagerIT
{
    private static final String CUSTOM_DATABASE_NAME = "customDatabaseName";

    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;
    private AssertableLogProvider logProvider;
    private DatabaseManager<?> databaseManager;

    @BeforeEach
    void setUp()
    {
        logProvider = new AssertableLogProvider( true );
        database = new TestCommercialGraphDatabaseFactory().setInternalLogProvider( logProvider )
                .newEmbeddedDatabase( testDirectory.databaseLayout( CUSTOM_DATABASE_NAME ).databaseDirectory() );
        databaseManager = getDatabaseManager();
    }

    @AfterEach
    void tearDown()
    {
        database.shutdown();
    }

    @Test
    void createDatabase() throws DatabaseExistsException
    {
        String databaseName = "testDatabase";
        GraphDatabaseFacade database1 = databaseManager.createDatabase( databaseName ).databaseFacade();

        assertNotNull( database1 );
        assertEquals( databaseName, database1.databaseLayout().getDatabaseName() );
    }

    @Test
    void failToCreateDatabasesWithSameName() throws DatabaseExistsException
    {
        String uniqueDatabaseName = "uniqueDatabaseName";
        databaseManager.createDatabase( uniqueDatabaseName );

        assertThrows( DatabaseExistsException.class, () -> databaseManager.createDatabase( uniqueDatabaseName ) );
        assertThrows( DatabaseExistsException.class, () -> databaseManager.createDatabase( uniqueDatabaseName ) );
        assertThrows( DatabaseExistsException.class, () -> databaseManager.createDatabase( uniqueDatabaseName ) );
        assertThrows( DatabaseExistsException.class, () -> databaseManager.createDatabase( uniqueDatabaseName ) );
    }

    @Test
    void failToStartUnknownDatabase()
    {
        String unknownDatabase = "unknownDatabase";
        assertThrows( DatabaseNotFoundException.class, () -> databaseManager.stopDatabase( unknownDatabase ) );
    }

    @Test
    void failToStartDroppedDatabase() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String databaseToDrop = "databaseToDrop";
        databaseManager.createDatabase( databaseToDrop );
        databaseManager.dropDatabase( databaseToDrop );

        assertThrows( DatabaseNotFoundException.class, () -> databaseManager.startDatabase( databaseToDrop ) );
    }

    @Test
    void startStartedDatabase() throws DatabaseExistsException
    {
        String multiStartDatabase = "multiStartDatabase";
        databaseManager.createDatabase( multiStartDatabase );

        assertDoesNotThrow( () -> databaseManager.startDatabase( multiStartDatabase ) );
        assertDoesNotThrow( () -> databaseManager.startDatabase( multiStartDatabase ) );
        assertDoesNotThrow( () -> databaseManager.startDatabase( multiStartDatabase ) );
        assertDoesNotThrow( () -> databaseManager.startDatabase( multiStartDatabase ) );
    }

    @Test
    void stopStartDatabase() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String startStopDatabase = "startStopDatabase";
        databaseManager.createDatabase( startStopDatabase );
        for ( int i = 0; i < 10; i++ )
        {
            databaseManager.stopDatabase( startStopDatabase );
            assertTrue( databaseManager.getDatabaseContext( startStopDatabase ).isPresent() );
            databaseManager.startDatabase( startStopDatabase );
        }
    }

    @Test
    void failToCreateDatabaseWithStoppedDatabaseName() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String stoppedDatabase = "stoppedDatabase";
        databaseManager.createDatabase( stoppedDatabase );

        databaseManager.stopDatabase( stoppedDatabase );

        assertThrows( DatabaseExistsException.class, () -> databaseManager.createDatabase( stoppedDatabase ) );
    }

    @Test
    void stopStoppedDatabaseIsFine() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String stoppedDatabase = "stoppedDatabase";

        databaseManager.createDatabase( stoppedDatabase );
        databaseManager.stopDatabase( stoppedDatabase );

        assertDoesNotThrow( () -> databaseManager.stopDatabase( stoppedDatabase ) );
    }

    @Test
    void recreateDatabaseWithSameName() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String databaseToRecreate = "databaseToRecreate";

        databaseManager.createDatabase( databaseToRecreate );

        databaseManager.dropDatabase( databaseToRecreate );
        assertFalse( databaseManager.getDatabaseContext( databaseToRecreate ).isPresent() );

        assertDoesNotThrow( () -> databaseManager.createDatabase( databaseToRecreate ) );
        assertTrue( databaseManager.getDatabaseContext( databaseToRecreate ).isPresent() );
    }

    @Test
    void dropStartedDatabase() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String startedDatabase = "startedDatabase";

        databaseManager.createDatabase( startedDatabase );
        databaseManager.dropDatabase( startedDatabase );
        assertFalse( databaseManager.getDatabaseContext( startedDatabase ).isPresent() );
    }

    @Test
    void dropStoppedDatabase() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String stoppedDatabase = "stoppedDatabase";

        databaseManager.createDatabase( stoppedDatabase );
        databaseManager.stopDatabase( stoppedDatabase );

        databaseManager.dropDatabase( stoppedDatabase );
        assertFalse( databaseManager.getDatabaseContext( stoppedDatabase ).isPresent() );
    }

    @Test
    void dropRemovesDatabaseFiles() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String databaseToDrop = "databaseToDrop";
        DatabaseContext database = databaseManager.createDatabase( databaseToDrop );

        DatabaseLayout databaseLayout = database.database().getDatabaseLayout();
        assertNotEquals( databaseLayout.databaseDirectory(), databaseLayout.getTransactionLogsDirectory() );
        assertTrue( databaseLayout.databaseDirectory().exists() );
        assertTrue( databaseLayout.getTransactionLogsDirectory().exists() );

        databaseManager.dropDatabase( databaseToDrop );
        assertFalse( databaseLayout.databaseDirectory().exists() );
        assertFalse( databaseLayout.getTransactionLogsDirectory().exists() );
    }

    @Test
    void stopDoesNotRemovesDatabaseFiles() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String databaseToStop = "databaseToStop";
        DatabaseContext database = databaseManager.createDatabase( databaseToStop );

        DatabaseLayout databaseLayout = database.database().getDatabaseLayout();
        assertTrue( databaseLayout.getTransactionLogsDirectory().exists() );
        assertTrue( databaseLayout.databaseDirectory().exists() );

        databaseManager.stopDatabase( databaseToStop );
        assertTrue( databaseLayout.databaseDirectory().exists() );
        assertTrue( databaseLayout.getTransactionLogsDirectory().exists() );
    }

    @Test
    void failToDropUnknownDatabase()
    {
        String unknownDatabase = "unknownDatabase";
        assertThrows( DatabaseNotFoundException.class, () -> databaseManager.dropDatabase( unknownDatabase ) );
    }

    @Test
    void failToStopUnknownDatabase()
    {
        String unknownDatabase = "unknownDatabase";
        assertThrows( DatabaseNotFoundException.class, () -> databaseManager.stopDatabase( unknownDatabase ) );
    }

    @Test
    void failToStopDroppedDatabase() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String testDatabase = "testDatabase";
        databaseManager.createDatabase( testDatabase );
        databaseManager.dropDatabase( testDatabase );
        assertThrows( DatabaseNotFoundException.class, () -> databaseManager.stopDatabase( testDatabase ) );
    }

    @Test
    void lookupNotExistingDatabase()
    {
        var database = databaseManager.getDatabaseContext( "testDatabase" );
        assertFalse( database.isPresent() );
    }

    @Test
    void lookupExistingDatabase()
    {
        var database = databaseManager.getDatabaseContext( CUSTOM_DATABASE_NAME );
        assertTrue( database.isPresent() );
    }

    @Test
    void createAndStopDatabase() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String databaseName = "databaseToShutdown";
        DatabaseContext context = databaseManager.createDatabase( databaseName );

        var databaseLookup = databaseManager.getDatabaseContext( databaseName );
        assertTrue( databaseLookup.isPresent() );
        assertEquals( context, databaseLookup.get() );

        databaseManager.stopDatabase( databaseName );
        assertTrue( databaseManager.getDatabaseContext( databaseName ).isPresent() );
    }

    @Test
    void logAboutDatabaseCreationAndStop() throws DatabaseExistsException, DatabaseNotFoundException
    {
        String logTestDb = "logTestDb";
        databaseManager.createDatabase( logTestDb );
        databaseManager.stopDatabase( logTestDb );
        logProvider.assertLogStringContains( "Creating 'logTestDb' database." );
        logProvider.assertLogStringContains( "Stop 'logTestDb' database." );
    }

    @Test
    void listAvailableDatabases() throws DatabaseExistsException
    {
        var initialDatabases = databaseManager.registeredDatabases();
        assertEquals( 2, initialDatabases.size() );
        assertTrue( initialDatabases.containsKey( CUSTOM_DATABASE_NAME ) );
        String myAnotherDatabase = "myAnotherDatabase";
        String aMyAnotherDatabase = "aMyAnotherDatabase";
        databaseManager.createDatabase( myAnotherDatabase );
        databaseManager.createDatabase( aMyAnotherDatabase );
        var postCreationDatabases = databaseManager.registeredDatabases();
        assertEquals( 4, postCreationDatabases.size() );

        ArrayList<String> postCreationDatabasesNames = new ArrayList<>( postCreationDatabases.keySet() );
        assertEquals( aMyAnotherDatabase, postCreationDatabasesNames.get( 0 ) );
        assertEquals( CUSTOM_DATABASE_NAME, postCreationDatabasesNames.get( 1 ) );
        assertEquals( myAnotherDatabase, postCreationDatabasesNames.get( 2 ) );
        assertEquals( SYSTEM_DATABASE_NAME, postCreationDatabasesNames.get( 3 ) );
    }

    @Test
    void listAvailableDatabaseOnShutdownManager() throws Throwable
    {
        databaseManager.stop();
        databaseManager.shutdown();
        var databases = databaseManager.registeredDatabases();
        assertTrue( databases.isEmpty() );
    }

    private DatabaseManager<?> getDatabaseManager()
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }
}
