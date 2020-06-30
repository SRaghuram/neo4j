/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.panic;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Health;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
class MultiDatabasePanicIT
{
    @Inject
    private TestDirectory testDirectory;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() ).build();
    }

    @AfterEach
    void tearDown()
    {
        managementService.shutdown();
    }

    @Test
    void databasePanicNotification() throws DatabaseExistsException
    {
        String firstDatabaseName = "first";
        String secondDatabaseName = "second";
        managementService.createDatabase( firstDatabaseName );
        managementService.createDatabase( secondDatabaseName );
        PanicDatabaseEventListener firstPanicListener = new PanicDatabaseEventListener( firstDatabaseName );
        PanicDatabaseEventListener secondPanicListener = new PanicDatabaseEventListener( secondDatabaseName );
        managementService.registerDatabaseEventListener( firstPanicListener );
        managementService.registerDatabaseEventListener( secondPanicListener );

        assertFalse( firstPanicListener.isPanic() );
        assertFalse( secondPanicListener.isPanic() );

        getDatabaseHealth( managementService.database( firstDatabaseName ) ).panic( new IllegalStateException() );
        assertTrue( firstPanicListener.isPanic() );
        assertFalse( secondPanicListener.isPanic() );

        getDatabaseHealth( managementService.database( secondDatabaseName ) ).panic( new IllegalStateException() );
        assertTrue( firstPanicListener.isPanic() );
        assertTrue( secondPanicListener.isPanic() );
    }

    private static Health getDatabaseHealth( GraphDatabaseService service )
    {
        return ((GraphDatabaseAPI) service).getDependencyResolver().resolveDependency( DatabaseHealth.class );
    }

    private static class PanicDatabaseEventListener extends DatabaseEventListenerAdapter
    {
        private final String databaseName;
        private boolean panic;

        PanicDatabaseEventListener( String databaseName )
        {
            this.databaseName = databaseName;
        }

        @Override
        public void databasePanic( DatabaseEventContext eventContext )
        {
            if ( databaseName.equals( eventContext.getDatabaseName() ) )
            {
                panic = true;
            }
        }

        boolean isPanic()
        {
            return panic;
        }
    }
}
