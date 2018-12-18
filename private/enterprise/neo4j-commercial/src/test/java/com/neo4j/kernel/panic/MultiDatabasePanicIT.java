/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.panic;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.DatabaseEventHandlerAdapter;
import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( TestDirectoryExtension.class )
class MultiDatabasePanicIT
{
    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;

    @BeforeEach
    void setUp()
    {
        database = new TestCommercialGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.storeDir() );
    }

    @AfterEach
    void tearDown()
    {
        database.shutdown();
    }

    @Test
    void databasesPanicSeparately()
    {
        DatabaseManager databaseManager = getDatabaseManager();
        GraphDatabaseFacade firstDatabase = databaseManager.createDatabase( "first" ).getDatabaseFacade();
        GraphDatabaseFacade secondDatabase = databaseManager.createDatabase( "second" ).getDatabaseFacade();
        PanicDatabaseEventListener firstPanicListener = new PanicDatabaseEventListener();
        PanicDatabaseEventListener secondPanicListener = new PanicDatabaseEventListener();
        firstDatabase.registerDatabaseEventHandler( firstPanicListener );
        secondDatabase.registerDatabaseEventHandler( secondPanicListener );

        assertFalse( firstPanicListener.isPanic() );
        assertFalse( secondPanicListener.isPanic() );

        getDatabaseHealth( firstDatabase ).panic( new IllegalStateException() );
        assertTrue( firstPanicListener.isPanic() );
        assertFalse( secondPanicListener.isPanic() );

        getDatabaseHealth( secondDatabase ).panic( new IllegalStateException() );
        assertTrue( firstPanicListener.isPanic() );
        assertTrue( secondPanicListener.isPanic() );
    }

    private DatabaseHealth getDatabaseHealth( GraphDatabaseFacade facade )
    {
        return facade.getDependencyResolver().resolveDependency( DatabaseHealth.class );
    }

    private DatabaseManager getDatabaseManager()
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }

    private static class PanicDatabaseEventListener extends DatabaseEventHandlerAdapter
    {
        private volatile boolean panic;

        @Override
        public void panic( ErrorState error )
        {
            panic = true;
        }

        boolean isPanic()
        {
            return panic;
        }
    }
}
