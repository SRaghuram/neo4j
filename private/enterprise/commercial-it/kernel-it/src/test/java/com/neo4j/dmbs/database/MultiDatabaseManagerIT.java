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

import java.util.Optional;

import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseLimitReachedException;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
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
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.internal.helpers.Exceptions.rootCause;

@ExtendWith( TestDirectoryExtension.class )
class MultiDatabaseManagerIT
{
    private static final DatabaseIdRepository DATABASE_ID_REPOSITORY = new TestDatabaseIdRepository();
    private static final DatabaseId CUSTOM_DATABASE_ID = DATABASE_ID_REPOSITORY.get( "customDatabaseName" );

    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;
    private AssertableLogProvider logProvider;
    private DatabaseManager<?> databaseManager;
    private DatabaseManagementService managementService;
    private DatabaseId systemDB = new TestDatabaseIdRepository().systemDatabase();

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
        assertThrows( DatabaseManagementException.class, () -> databaseManager.dropDatabase( systemDB ) );
    }

    @Test
    void failToStopSystemDatabase()
    {
        DatabaseManager<?> databaseManager = getDatabaseManager();
        assertThrows( DatabaseManagementException.class, () -> databaseManager.stopDatabase( systemDB ) );
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
        assertThat( rootCause( exception ).getMessage(),
                containsString( "The total limit of databases is already reached. To create more you need to either drop databases or change the" +
                        " limit via the config setting 'dbms.max_databases'" ) );
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
        DatabaseId databaseId = DATABASE_ID_REPOSITORY.get( "testDatabase" );
        GraphDatabaseFacade database1 = databaseManager.createDatabase( databaseId ).databaseFacade();

        assertNotNull( database1 );
        assertEquals( databaseId.name(), database1.databaseName() );
    }

    @Test
    void failToCreateDatabasesWithSameName() throws DatabaseExistsException
    {
        DatabaseId uniqueDatabaseName = DATABASE_ID_REPOSITORY.get( "uniqueDatabaseName" );
        databaseManager.createDatabase( uniqueDatabaseName );

        assertThrows( DatabaseExistsException.class, () -> databaseManager.createDatabase( uniqueDatabaseName ) );
        assertThrows( DatabaseExistsException.class, () -> databaseManager.createDatabase( uniqueDatabaseName ) );
        assertThrows( DatabaseExistsException.class, () -> databaseManager.createDatabase( uniqueDatabaseName ) );
        assertThrows( DatabaseExistsException.class, () -> databaseManager.createDatabase( uniqueDatabaseName ) );
    }

    @Test
    void failToStartUnknownDatabase()
    {
        DatabaseId unknownDatabase = DATABASE_ID_REPOSITORY.get( "unknownDatabase" );
        assertThrows( DatabaseNotFoundException.class, () -> databaseManager.stopDatabase( unknownDatabase ) );
    }

    @Test
    void failToStartDroppedDatabase() throws DatabaseExistsException, DatabaseNotFoundException
    {
        DatabaseId databaseToDrop = DATABASE_ID_REPOSITORY.get( "databaseToDrop" );
        databaseManager.createDatabase( databaseToDrop );
        databaseManager.dropDatabase( databaseToDrop );

        assertThrows( DatabaseNotFoundException.class, () -> databaseManager.startDatabase( databaseToDrop ) );
    }

    @Test
    void startStartedDatabase() throws DatabaseExistsException
    {
        DatabaseId multiStartDatabase = DATABASE_ID_REPOSITORY.get( "multiStartDatabase" );
        databaseManager.createDatabase( multiStartDatabase );

        assertDoesNotThrow( () -> databaseManager.startDatabase( multiStartDatabase ) );
        assertDoesNotThrow( () -> databaseManager.startDatabase( multiStartDatabase ) );
        assertDoesNotThrow( () -> databaseManager.startDatabase( multiStartDatabase ) );
        assertDoesNotThrow( () -> databaseManager.startDatabase( multiStartDatabase ) );
    }

    @Test
    void stopStartDatabase() throws DatabaseExistsException, DatabaseNotFoundException
    {
        DatabaseId startStopDatabase = DATABASE_ID_REPOSITORY.get( "startStopDatabase" );
        databaseManager.createDatabase( startStopDatabase );
        for ( int i = 0; i < 10; i++ )
        {
            databaseManager.stopDatabase( startStopDatabase );
            Optional<? extends DatabaseContext> databaseContext = databaseManager.getDatabaseContext( startStopDatabase );
            assertFalse( databaseContext.get().database().isStarted() );
            databaseManager.startDatabase( startStopDatabase );
            assertTrue( databaseContext.get().database().isStarted() );
        }
    }

    @Test
    void failToCreateDatabaseWithStoppedDatabaseName() throws DatabaseExistsException, DatabaseNotFoundException
    {
        DatabaseId stoppedDatabase = DATABASE_ID_REPOSITORY.get( "stoppedDatabase" );
        databaseManager.createDatabase( stoppedDatabase );

        databaseManager.stopDatabase( stoppedDatabase );

        assertThrows( DatabaseExistsException.class, () -> databaseManager.createDatabase( stoppedDatabase ) );
    }

    @Test
    void stopStoppedDatabaseIsFine() throws DatabaseExistsException, DatabaseNotFoundException
    {
        DatabaseId stoppedDatabase = DATABASE_ID_REPOSITORY.get( "stoppedDatabase" );

        databaseManager.createDatabase( stoppedDatabase );
        databaseManager.stopDatabase( stoppedDatabase );

        assertDoesNotThrow( () -> databaseManager.stopDatabase( stoppedDatabase ) );
    }

    @Test
    void recreateDatabaseWithSameName() throws DatabaseExistsException, DatabaseNotFoundException
    {
        DatabaseId databaseToRecreate = DATABASE_ID_REPOSITORY.get( "databaseToRecreate" );

        databaseManager.createDatabase( databaseToRecreate );

        databaseManager.dropDatabase( databaseToRecreate );
        assertFalse( databaseManager.getDatabaseContext( databaseToRecreate ).isPresent() );

        assertDoesNotThrow( () -> databaseManager.createDatabase( databaseToRecreate ) );
        assertTrue( databaseManager.getDatabaseContext( databaseToRecreate ).isPresent() );
    }

    @Test
    void dropStartedDatabase() throws DatabaseExistsException, DatabaseNotFoundException
    {
        DatabaseId startedDatabase = DATABASE_ID_REPOSITORY.get( "startedDatabase" );

        databaseManager.createDatabase( startedDatabase );
        databaseManager.dropDatabase( startedDatabase );
        assertFalse( databaseManager.getDatabaseContext( startedDatabase ).isPresent() );
    }

    @Test
    void dropStoppedDatabase() throws DatabaseExistsException, DatabaseNotFoundException
    {
        DatabaseId stoppedDatabase = DATABASE_ID_REPOSITORY.get( "stoppedDatabase" );

        databaseManager.createDatabase( stoppedDatabase );
        databaseManager.stopDatabase( stoppedDatabase );

        databaseManager.dropDatabase( stoppedDatabase );
        assertFalse( databaseManager.getDatabaseContext( stoppedDatabase ).isPresent() );
    }

    @Test
    void dropRemovesDatabaseFiles() throws DatabaseExistsException, DatabaseNotFoundException
    {
        DatabaseId databaseToDrop = DATABASE_ID_REPOSITORY.get( "databaseToDrop" );
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
        DatabaseId databaseToStop = DATABASE_ID_REPOSITORY.get( "databaseToStop" );
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
        DatabaseId unknownDatabase = DATABASE_ID_REPOSITORY.get( "unknownDatabase" );
        assertThrows( DatabaseNotFoundException.class, () -> databaseManager.dropDatabase( unknownDatabase ) );
    }

    @Test
    void failToStopUnknownDatabase()
    {
        DatabaseId unknownDatabase = DATABASE_ID_REPOSITORY.get( "unknownDatabase" );
        assertThrows( DatabaseNotFoundException.class, () -> databaseManager.stopDatabase( unknownDatabase ) );
    }

    @Test
    void failToStopDroppedDatabase() throws DatabaseExistsException, DatabaseNotFoundException
    {
        DatabaseId testDatabase = DATABASE_ID_REPOSITORY.get( "testDatabase" );
        databaseManager.createDatabase( testDatabase );
        databaseManager.dropDatabase( testDatabase );
        assertThrows( DatabaseNotFoundException.class, () -> databaseManager.stopDatabase( testDatabase ) );
    }

    @Test
    void lookupNotExistingDatabase()
    {
        var database = databaseManager.getDatabaseContext( DATABASE_ID_REPOSITORY.get( "testDatabase" ) );
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
        DatabaseId databaseId = DATABASE_ID_REPOSITORY.get( "databaseToShutdown" );
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
        DatabaseId logTestDb = DATABASE_ID_REPOSITORY.get( "logTestDb" );
        databaseManager.createDatabase( logTestDb );
        databaseManager.stopDatabase( logTestDb );
        logProvider.formattedMessageMatcher().assertContains( "Creating 'logtestdb' database." );
        logProvider.formattedMessageMatcher().assertContains( "Stop 'logtestdb' database." );
    }

    @Test
    void listAvailableDatabases() throws DatabaseExistsException
    {
        var initialDatabases = databaseManager.registeredDatabases();
        assertEquals( 2, initialDatabases.size() );
        assertTrue( initialDatabases.containsKey( CUSTOM_DATABASE_ID ) );
        DatabaseId myAnotherDatabase = DATABASE_ID_REPOSITORY.get( "myAnotherDatabase" );
        DatabaseId aMyAnotherDatabase = DATABASE_ID_REPOSITORY.get( "aMyAnotherDatabase" );
        databaseManager.createDatabase( myAnotherDatabase );
        databaseManager.createDatabase( aMyAnotherDatabase );
        var postCreationDatabases = databaseManager.registeredDatabases();
        assertEquals( 4, postCreationDatabases.size() );

        assertThat( postCreationDatabases.keySet(),
                contains( DATABASE_ID_REPOSITORY.systemDatabase(), aMyAnotherDatabase, CUSTOM_DATABASE_ID, myAnotherDatabase) );
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
