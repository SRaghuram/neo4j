/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database.events;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@TestDirectoryExtension
class StartingDatabaseEventsIT
{
    @Inject
    private TestDirectory testDirectory;
    @Test
    void receiveStartingDatabaseEvents()
    {
        AllDatabasesEventListener eventListener = new AllDatabasesEventListener();
        DatabaseManagementService managementService = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homeDir() )
                                                        .addDatabaseListener( eventListener ).build();
        managementService.shutdown();

        List<String> startedDatabases = eventListener.getStartedDatabases();
        List<String> shutdownDatabases = eventListener.getShutdownDatabases();
        assertThat( startedDatabases ).containsExactly( SYSTEM_DATABASE_NAME, DEFAULT_DATABASE_NAME );
        assertThat( shutdownDatabases ).containsExactly( DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME );
    }

    private static class AllDatabasesEventListener extends DatabaseEventListenerAdapter
    {
        private final List<String> startedDatabases = new ArrayList<>();
        private final List<String> shutdownDatabases = new ArrayList<>();

        @Override
        public void databaseStart( DatabaseEventContext eventContext )
        {
            startedDatabases.add( eventContext.getDatabaseName() );
        }

        @Override
        public void databaseShutdown( DatabaseEventContext eventContext )
        {
            shutdownDatabases.add( eventContext.getDatabaseName() );
        }

        List<String> getStartedDatabases()
        {
            return startedDatabases;
        }

        List<String> getShutdownDatabases()
        {
            return shutdownDatabases;
        }
    }
}
