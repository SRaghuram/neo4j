/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dmbs.database;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.List;
import java.util.Optional;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( TestDirectoryExtension.class )
class MultiDatabaseManagerIT
{
    private static final String CUSTOM_DATABASE_NAME = "customDatabaseName";

    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;
    private AssertableLogProvider logProvider;
    private DatabaseManager databaseManager;

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
    void createDatabase()
    {
        String databaseName = "testDatabase";
        GraphDatabaseFacade database1 = databaseManager.createDatabase( databaseName ).getDatabaseFacade();

        assertNotNull( database1 );
        assertEquals( databaseName, database1.databaseLayout().getDatabaseName() );
    }

    @Test
    void failToCreateDatabasesWithSameName()
    {
        String uniqueDatabaseName = "uniqueDatabaseName";
        databaseManager.createDatabase( uniqueDatabaseName );

        assertThrows( IllegalStateException.class, () -> databaseManager.createDatabase( uniqueDatabaseName ) );
        assertThrows( IllegalStateException.class, () -> databaseManager.createDatabase( uniqueDatabaseName ) );
        assertThrows( IllegalStateException.class, () -> databaseManager.createDatabase( uniqueDatabaseName ) );
        assertThrows( IllegalStateException.class, () -> databaseManager.createDatabase( uniqueDatabaseName ) );
    }

    @Test
    void failToStartUnknownDatabase()
    {
        String unknownDatabase = "unknownDatabase";
        assertThrows( IllegalStateException.class, () -> databaseManager.stopDatabase( unknownDatabase ) );
    }

    @Test
    void failToStartDroppedDatabase()
    {
        String databaseToDrop = "databaseToDrop";
        databaseManager.createDatabase( databaseToDrop );
        databaseManager.dropDatabase( databaseToDrop );

        assertThrows( IllegalStateException.class, () -> databaseManager.startDatabase( databaseToDrop ) );
    }

    @Test
    void startStartedDatabase()
    {
        String multiStartDatabase = "multiStartDatabase";
        databaseManager.createDatabase( multiStartDatabase );

        assertDoesNotThrow( () -> databaseManager.startDatabase( multiStartDatabase ) );
        assertDoesNotThrow( () -> databaseManager.startDatabase( multiStartDatabase ) );
        assertDoesNotThrow( () -> databaseManager.startDatabase( multiStartDatabase ) );
        assertDoesNotThrow( () -> databaseManager.startDatabase( multiStartDatabase ) );
    }

    @Test
    void stopStartDatabase()
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
    void failToCreateDatabaseWithStoppedDatabaseName()
    {
        String stoppedDatabase = "stoppedDatabase";
        databaseManager.createDatabase( stoppedDatabase );

        databaseManager.stopDatabase( stoppedDatabase );

        assertThrows( IllegalStateException.class, () -> databaseManager.createDatabase( stoppedDatabase ) );
    }

    @Test
    void stopStoppedDatabaseIsFine()
    {
        String stoppedDatabase = "stoppedDatabase";

        databaseManager.createDatabase( stoppedDatabase );
        databaseManager.stopDatabase( stoppedDatabase );

        assertDoesNotThrow( () -> databaseManager.stopDatabase( stoppedDatabase ) );
    }

    @Test
    void recreateDatabaseWithSameName()
    {
        String databaseToRecreate = "databaseToRecreate";

        databaseManager.createDatabase( databaseToRecreate );

        databaseManager.dropDatabase( databaseToRecreate );
        assertFalse( databaseManager.getDatabaseContext( databaseToRecreate ).isPresent() );

        assertDoesNotThrow( () -> databaseManager.createDatabase( databaseToRecreate ) );
        assertTrue( databaseManager.getDatabaseContext( databaseToRecreate ).isPresent() );
    }

    @Test
    void dropStartedDatabase()
    {
        String startedDatabase = "startedDatabase";

        databaseManager.createDatabase( startedDatabase );
        databaseManager.dropDatabase( startedDatabase );
        assertFalse( databaseManager.getDatabaseContext( startedDatabase ).isPresent() );
    }

    @Test
    void dropStoppedDatabase()
    {
        String stoppedDatabase = "stoppedDatabase";

        databaseManager.createDatabase( stoppedDatabase );
        databaseManager.stopDatabase( stoppedDatabase );

        databaseManager.dropDatabase( stoppedDatabase );
        assertFalse( databaseManager.getDatabaseContext( stoppedDatabase ).isPresent() );
    }

    @Test
    void dropRemovesDatabaseFiles()
    {
        String databaseToDrop = "databaseToDrop";
        DatabaseContext database = databaseManager.createDatabase( databaseToDrop );

        File databaseDirectory = database.getDatabase().getDatabaseLayout().databaseDirectory();
        assertTrue( databaseDirectory.exists() );

        databaseManager.dropDatabase( databaseToDrop );
        assertFalse( databaseDirectory.exists() );
    }

    @Test
    void stopDoesNotRemovesDatabaseFiles()
    {
        String databaseToStop = "databaseToStop";
        DatabaseContext database = databaseManager.createDatabase( databaseToStop );

        File databaseDirectory = database.getDatabase().getDatabaseLayout().databaseDirectory();
        assertTrue( databaseDirectory.exists() );

        databaseManager.stopDatabase( databaseToStop );
        assertTrue( databaseDirectory.exists() );
    }

    @Test
    void failToDropUnknownDatabase()
    {
        String unknownDatabase = "unknownDatabase";
        assertThrows( IllegalStateException.class, () -> databaseManager.dropDatabase( unknownDatabase ) );
    }

    @Test
    void failToStopUnknownDatabase()
    {
        String unknownDatabase = "unknownDatabase";
        assertThrows( IllegalStateException.class, () -> databaseManager.stopDatabase( unknownDatabase ) );
    }

    @Test
    void failToStopDroppedDatabase()
    {
        String testDatabase = "testDatabase";
        databaseManager.createDatabase( testDatabase );
        databaseManager.dropDatabase( testDatabase );
        assertThrows( IllegalStateException.class, () -> databaseManager.stopDatabase( testDatabase ) );
    }

    @Test
    void lookupNotExistingDatabase()
    {
        Optional<DatabaseContext> database = databaseManager.getDatabaseContext( "testDatabase" );
        assertFalse( database.isPresent() );
    }

    @Test
    void lookupExistingDatabase()
    {
        Optional<DatabaseContext> database = databaseManager.getDatabaseContext( CUSTOM_DATABASE_NAME );
        assertTrue( database.isPresent() );
    }

    @Test
    void createAndStopDatabase()
    {
        String databaseName = "databaseToShutdown";
        DatabaseContext context = databaseManager.createDatabase( databaseName );

        Optional<DatabaseContext> databaseLookup = databaseManager.getDatabaseContext( databaseName );
        assertTrue( databaseLookup.isPresent() );
        assertEquals( context, databaseLookup.get() );

        databaseManager.stopDatabase( databaseName );
        assertTrue( databaseManager.getDatabaseContext( databaseName ).isPresent() );
    }

    @Test
    void logAboutDatabaseCreationAndStop()
    {
        String logTestDb = "logTestDb";
        databaseManager.createDatabase( logTestDb );
        databaseManager.stopDatabase( logTestDb );
        logProvider.assertLogStringContains( "Creating 'logTestDb' database." );
        logProvider.assertLogStringContains( "Stop 'logTestDb' database." );
    }

    @Test
    void listAvailableDatabases()
    {
        List<String> initialDatabases = databaseManager.listDatabases();
        assertThat( initialDatabases, hasSize( 1 ) );
        assertEquals( CUSTOM_DATABASE_NAME, initialDatabases.get( 0 ) );
        String myAnotherDatabase = "myAnotherDatabase";
        String aMyAnotherDatabase = "aMyAnotherDatabase";
        databaseManager.createDatabase( myAnotherDatabase );
        databaseManager.createDatabase( aMyAnotherDatabase );
        List<String> postCreationDatabasesNames = databaseManager.listDatabases();
        assertThat( postCreationDatabasesNames, hasSize( 3 ) );
        assertEquals( aMyAnotherDatabase, postCreationDatabasesNames.get( 0 ) );
        assertEquals( CUSTOM_DATABASE_NAME, postCreationDatabasesNames.get( 1 ) );
        assertEquals( myAnotherDatabase, postCreationDatabasesNames.get( 2 ) );
    }

    @Test
    void listAvailableDatabaseOnShutdownManager() throws Throwable
    {
        databaseManager.stop();
        databaseManager.shutdown();
        List<String> databases = databaseManager.listDatabases();
        assertThat( databases, empty() );
    }

    private DatabaseManager getDatabaseManager()
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }
}
