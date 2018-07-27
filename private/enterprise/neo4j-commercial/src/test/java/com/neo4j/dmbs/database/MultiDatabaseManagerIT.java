/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dmbs.database;

import com.neo4j.commercial.edition.factory.CommercialGraphDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Optional;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( TestDirectoryExtension.class )
class MultiDatabaseManagerIT
{
    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;

    @BeforeEach
    void setUp()
    {
        database = new CommercialGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.storeDir() );
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
        GraphDatabaseFacade database1 = databaseManager.createDatabase( databaseName );

        assertNotNull( database1 );
        assertEquals( databaseName, database1.databaseDirectory().getName() );
    }

    @Test
    void lookupNotExistingDatabase()
    {
        DatabaseManager databaseManager = getDatabaseManager();
        Optional<GraphDatabaseFacade> database = databaseManager.getDatabaseFacade( "testDatabase" );
        assertFalse( database.isPresent() );
    }

    @Test
    void lookupExistingDatabase()
    {
        DatabaseManager databaseManager = getDatabaseManager();
        Optional<GraphDatabaseFacade> database = databaseManager.getDatabaseFacade( DatabaseManager.DEFAULT_DATABASE_NAME );
        assertTrue( database.isPresent() );
    }

    @Test
    void createAndShutdownDatabase()
    {
        DatabaseManager databaseManager = getDatabaseManager();
        String databaseName = "databaseToShutdown";
        GraphDatabaseFacade database = databaseManager.createDatabase( databaseName );

        Optional<GraphDatabaseFacade> databaseLookup = databaseManager.getDatabaseFacade( databaseName );
        assertTrue( databaseLookup.isPresent() );
        assertEquals( database, databaseLookup.get() );

        databaseManager.shutdownDatabase( databaseName );
        assertFalse( databaseManager.getDatabaseFacade( databaseName ).isPresent() );
    }

    private DatabaseManager getDatabaseManager()
    {
        return ((GraphDatabaseAPI)database).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }
}
