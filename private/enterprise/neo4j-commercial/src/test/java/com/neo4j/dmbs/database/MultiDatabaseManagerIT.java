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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( TestDirectoryExtension.class )
class MultiDatabaseManagerIT
{
    private static final String CUSTOM_DATABASE_NAME = "customDatabaseName";

    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;
    private AssertableLogProvider logProvider;

    @BeforeEach
    void setUp()
    {
        logProvider = new AssertableLogProvider( true );
        database = new TestCommercialGraphDatabaseFactory().setInternalLogProvider( logProvider )
                .newEmbeddedDatabase( testDirectory.databaseLayout( CUSTOM_DATABASE_NAME ).databaseDirectory() );
    }

    @AfterEach
    void tearDown()
    {
        database.shutdown();
    }

    @Test
    void createDatabase()
    {
        DatabaseManager databaseManager = getDatabaseManager();
        String databaseName = "testDatabase";
        GraphDatabaseFacade database1 = databaseManager.createDatabase( databaseName ).getDatabaseFacade();

        assertNotNull( database1 );
        assertEquals( databaseName, database1.databaseLayout().getDatabaseName() );
    }

    @Test
    void lookupNotExistingDatabase()
    {
        DatabaseManager databaseManager = getDatabaseManager();
        Optional<DatabaseContext> database = databaseManager.getDatabaseContext( "testDatabase" );
        assertFalse( database.isPresent() );
    }

    @Test
    void lookupExistingDatabase()
    {
        DatabaseManager databaseManager = getDatabaseManager();
        Optional<DatabaseContext> database = databaseManager.getDatabaseContext( CUSTOM_DATABASE_NAME );
        assertTrue( database.isPresent() );
    }

    @Test
    void createAndShutdownDatabase()
    {
        DatabaseManager databaseManager = getDatabaseManager();
        String databaseName = "databaseToShutdown";
        DatabaseContext context = databaseManager.createDatabase( databaseName );

        Optional<DatabaseContext> databaseLookup = databaseManager.getDatabaseContext( databaseName );
        assertTrue( databaseLookup.isPresent() );
        assertEquals( context, databaseLookup.get() );

        databaseManager.shutdownDatabase( databaseName );
        assertFalse( databaseManager.getDatabaseContext( databaseName ).isPresent() );
    }

    @Test
    void logAboutDatabaseCreationAndShutdown()
    {
        DatabaseManager databaseManager = getDatabaseManager();
        String logTestDb = "logTestDb";
        databaseManager.createDatabase( logTestDb );
        databaseManager.shutdownDatabase( logTestDb );
        logProvider.assertLogStringContains( "Creating 'logTestDb' database." );
        logProvider.assertLogStringContains( "Shutting down 'logTestDb' database." );
    }

    @Test
    void listAvailableDatabases()
    {
        DatabaseManager databaseManager = getDatabaseManager();
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
        DatabaseManager databaseManager = getDatabaseManager();
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
