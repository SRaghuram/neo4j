/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dmbs.database;

import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings.maxNumberOfDatabases;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.internal.helpers.Exceptions.rootCause;

@ExtendWith( TestDirectoryExtension.class )
class MultiDatabaseManagerIT
{
    private static final DatabaseId CUSTOM_DATABASE_ID = new DatabaseId( "customDatabaseName" );

    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;
    private AssertableLogProvider logProvider;
    private DatabaseManager<?> databaseManager;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        logProvider = new AssertableLogProvider( true );
        managementService = new TestCommercialDatabaseManagementServiceBuilder( testDirectory.storeDir() )
                .setInternalLogProvider( logProvider )
                .setConfig( default_database, CUSTOM_DATABASE_ID.name() )
                .setConfig( maxNumberOfDatabases, "5" )
                .build();
        database = managementService.database( CUSTOM_DATABASE_ID.name() );
        databaseManager = getDatabaseManager();
    }

    @AfterEach
    void tearDown()
    {
        managementService.shutdown();
    }

    @Test
    void failToDropSystemDatabaseOnDatabaseManagerLevel()
    {
        DatabaseManager<?> databaseManager = getDatabaseManager();
        assertThrows( DatabaseManagementException.class, () -> databaseManager.dropDatabase( new DatabaseId( SYSTEM_DATABASE_NAME ) ) );
    }

    @Test
    void failToStopSystemDatabase()
    {
        DatabaseManager<?> databaseManager = getDatabaseManager();
        assertThrows( DatabaseManagementException.class, () -> databaseManager.stopDatabase( new DatabaseId( SYSTEM_DATABASE_NAME ) ) );
    }

    @Test
    void restrictDatabaseCreation()
    {
        for ( int i = 0; i < 3; i++ )
        {
            managementService.createDatabase( "database" + i );
        }
        TransactionFailureException exception = assertThrows( TransactionFailureException.class, () -> managementService.createDatabase( "any" ) );
        assertThat( rootCause( exception ).getMessage(),
                containsString( "Reached maximum number of active databases. Fail to create new database `any`." ) );
    }

    @Test
    void allowCreationOfDatabaseAfterDrop()
    {
        for ( int i = 0; i < 3; i++ )
        {
            managementService.createDatabase( "database" + i );
        }
        TransactionFailureException exception = assertThrows( TransactionFailureException.class, () -> managementService.createDatabase( "any" ) );
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
        assertThrows( TransactionFailureException.class, () -> managementService.createDatabase( "any" ) );
    }

    @Test
    void createDatabase() throws DatabaseExistsException
    {
        DatabaseId databaseId = new DatabaseId( "testDatabase" );
        GraphDatabaseFacade database1 = databaseManager.createDatabase( databaseId ).databaseFacade();

        assertNotNull( database1 );
        assertEquals( databaseId.name(), database1.databaseName() );
    }

    @Test
    void failToCreateDatabasesWithSameName() throws DatabaseExistsException
    {
        DatabaseId uniqueDatabaseName = new DatabaseId( "uniqueDatabaseName" );
        databaseManager.createDatabase( uniqueDatabaseName );

        assertThrows( DatabaseExistsException.class, () -> databaseManager.createDatabase( uniqueDatabaseName ) );
        assertThrows( DatabaseExistsException.class, () -> databaseManager.createDatabase( uniqueDatabaseName ) );
        assertThrows( DatabaseExistsException.class, () -> databaseManager.createDatabase( uniqueDatabaseName ) );
        assertThrows( DatabaseExistsException.class, () -> databaseManager.createDatabase( uniqueDatabaseName ) );
    }

    @Test
    void failToStartUnknownDatabase()
    {
        DatabaseId unknownDatabase = new DatabaseId( "unknownDatabase" );
        assertThrows( DatabaseNotFoundException.class, () -> databaseManager.stopDatabase( unknownDatabase ) );
    }

    @Test
    void failToStartDroppedDatabase() throws DatabaseExistsException, DatabaseNotFoundException
    {
        DatabaseId databaseToDrop = new DatabaseId( "databaseToDrop" );
        databaseManager.createDatabase( databaseToDrop );
        databaseManager.dropDatabase( databaseToDrop );

        assertThrows( DatabaseNotFoundException.class, () -> databaseManager.startDatabase( databaseToDrop ) );
    }

    @Test
    void startStartedDatabase() throws DatabaseExistsException
    {
        DatabaseId multiStartDatabase = new DatabaseId( "multiStartDatabase" );
        databaseManager.createDatabase( multiStartDatabase );

        assertDoesNotThrow( () -> databaseManager.startDatabase( multiStartDatabase ) );
        assertDoesNotThrow( () -> databaseManager.startDatabase( multiStartDatabase ) );
        assertDoesNotThrow( () -> databaseManager.startDatabase( multiStartDatabase ) );
        assertDoesNotThrow( () -> databaseManager.startDatabase( multiStartDatabase ) );
    }

    @Test
    void stopStartDatabase() throws DatabaseExistsException, DatabaseNotFoundException
    {
        DatabaseId startStopDatabase = new DatabaseId( "startStopDatabase" );
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
        DatabaseId stoppedDatabase = new DatabaseId( "stoppedDatabase" );
        databaseManager.createDatabase( stoppedDatabase );

        databaseManager.stopDatabase( stoppedDatabase );

        assertThrows( DatabaseExistsException.class, () -> databaseManager.createDatabase( stoppedDatabase ) );
    }

    @Test
    void stopStoppedDatabaseIsFine() throws DatabaseExistsException, DatabaseNotFoundException
    {
        DatabaseId stoppedDatabase = new DatabaseId( "stoppedDatabase" );

        databaseManager.createDatabase( stoppedDatabase );
        databaseManager.stopDatabase( stoppedDatabase );

        assertDoesNotThrow( () -> databaseManager.stopDatabase( stoppedDatabase ) );
    }

    @Test
    void recreateDatabaseWithSameName() throws DatabaseExistsException, DatabaseNotFoundException
    {
        DatabaseId databaseToRecreate = new DatabaseId( "databaseToRecreate" );

        databaseManager.createDatabase( databaseToRecreate );

        databaseManager.dropDatabase( databaseToRecreate );
        assertFalse( databaseManager.getDatabaseContext( databaseToRecreate ).isPresent() );

        assertDoesNotThrow( () -> databaseManager.createDatabase( databaseToRecreate ) );
        assertTrue( databaseManager.getDatabaseContext( databaseToRecreate ).isPresent() );
    }

    @Test
    void dropStartedDatabase() throws DatabaseExistsException, DatabaseNotFoundException
    {
        DatabaseId startedDatabase = new DatabaseId( "startedDatabase" );

        databaseManager.createDatabase( startedDatabase );
        databaseManager.dropDatabase( startedDatabase );
        assertFalse( databaseManager.getDatabaseContext( startedDatabase ).isPresent() );
    }

    @Test
    void dropStoppedDatabase() throws DatabaseExistsException, DatabaseNotFoundException
    {
        DatabaseId stoppedDatabase = new DatabaseId( "stoppedDatabase" );

        databaseManager.createDatabase( stoppedDatabase );
        databaseManager.stopDatabase( stoppedDatabase );

        databaseManager.dropDatabase( stoppedDatabase );
        assertFalse( databaseManager.getDatabaseContext( stoppedDatabase ).isPresent() );
    }

    @Test
    void dropRemovesDatabaseFiles() throws DatabaseExistsException, DatabaseNotFoundException
    {
        DatabaseId databaseToDrop = new DatabaseId( "databaseToDrop" );
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
        DatabaseId databaseToStop = new DatabaseId( "databaseToStop" );
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
        DatabaseId unknownDatabase = new DatabaseId( "unknownDatabase" );
        assertThrows( DatabaseNotFoundException.class, () -> databaseManager.dropDatabase( unknownDatabase ) );
    }

    @Test
    void failToStopUnknownDatabase()
    {
        DatabaseId unknownDatabase = new DatabaseId( "unknownDatabase" );
        assertThrows( DatabaseNotFoundException.class, () -> databaseManager.stopDatabase( unknownDatabase ) );
    }

    @Test
    void failToStopDroppedDatabase() throws DatabaseExistsException, DatabaseNotFoundException
    {
        DatabaseId testDatabase = new DatabaseId( "testDatabase" );
        databaseManager.createDatabase( testDatabase );
        databaseManager.dropDatabase( testDatabase );
        assertThrows( DatabaseNotFoundException.class, () -> databaseManager.stopDatabase( testDatabase ) );
    }

    @Test
    void lookupNotExistingDatabase()
    {
        var database = databaseManager.getDatabaseContext( new DatabaseId( "testDatabase" ) );
        assertFalse( database.isPresent() );
    }

    @Test
    void lookupExistingDatabase()
    {
        var database = databaseManager.getDatabaseContext( CUSTOM_DATABASE_ID );
        assertTrue( database.isPresent() );
    }

    @Test
    void createAndStopDatabase() throws DatabaseExistsException, DatabaseNotFoundException
    {
        DatabaseId databaseId = new DatabaseId( "databaseToShutdown" );
        DatabaseContext context = databaseManager.createDatabase( databaseId );

        var databaseLookup = databaseManager.getDatabaseContext( databaseId );
        assertTrue( databaseLookup.isPresent() );
        assertEquals( context, databaseLookup.get() );

        databaseManager.stopDatabase( databaseId );
        assertTrue( databaseManager.getDatabaseContext( databaseId ).isPresent() );
    }

    @Test
    void logAboutDatabaseCreationAndStop() throws DatabaseExistsException, DatabaseNotFoundException
    {
        DatabaseId logTestDb = new DatabaseId( "logTestDb" );
        databaseManager.createDatabase( logTestDb );
        databaseManager.stopDatabase( logTestDb );
        logProvider.assertLogStringContains( "Creating 'logtestdb' database." );
        logProvider.assertLogStringContains( "Stop 'logtestdb' database." );
    }

    @Test
    void listAvailableDatabases() throws DatabaseExistsException
    {
        var initialDatabases = databaseManager.registeredDatabases();
        assertEquals( 2, initialDatabases.size() );
        assertTrue( initialDatabases.containsKey( CUSTOM_DATABASE_ID ) );
        DatabaseId myAnotherDatabase = new DatabaseId( "myAnotherDatabase" );
        DatabaseId aMyAnotherDatabase = new DatabaseId( "aMyAnotherDatabase" );
        databaseManager.createDatabase( myAnotherDatabase );
        databaseManager.createDatabase( aMyAnotherDatabase );
        var postCreationDatabases = databaseManager.registeredDatabases();
        assertEquals( 4, postCreationDatabases.size() );

        assertThat( postCreationDatabases.keySet(),
                contains( new DatabaseId( SYSTEM_DATABASE_NAME ), aMyAnotherDatabase, CUSTOM_DATABASE_ID, myAnotherDatabase) );
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
