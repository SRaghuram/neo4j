/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphdb.facade;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManagementException;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.event.DatabaseEventHandlerAdapter;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@ExtendWith( TestDirectoryExtension.class )
class GraphDatabaseFacadeFactoryIT
{
    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        managementService = getDatabaseManagementService();
    }

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void haveTwoDatabasesByDefault()
    {
        assertThat( managementService.listDatabases(), hasSize( 2 ) );
    }

    @Test
    void failToCreateNonDefaultDatabase()
    {
        assertThrows( DatabaseManagementException.class, () -> managementService.createDatabase( "newDb" ) );
    }

    @Test
    void failToDropDatabase()
    {
        for ( String databaseName : managementService.listDatabases() )
        {
            assertThrows( DatabaseManagementException.class, () -> managementService.dropDatabase( databaseName ) );
        }
    }

    @Test
    void failToStartStopSystemDatabase()
    {
        assertThrows( DatabaseManagementException.class, () -> managementService.stopDatabase( SYSTEM_DATABASE_NAME ) );
        assertThrows( DatabaseManagementException.class, () -> managementService.startDatabase( SYSTEM_DATABASE_NAME ) );
    }

    @Test
    void shutdownShouldShutdownAllDatabases()
    {
        ShutdownListenerDatabaseEventHandler shutdownListenerDatabaseEventHandler = new ShutdownListenerDatabaseEventHandler();
        for ( String databaseName : managementService.listDatabases() )
        {
            managementService.database( databaseName ).registerDatabaseEventHandler( shutdownListenerDatabaseEventHandler );
        }
        managementService.shutdown();
        managementService = null;

        assertEquals( 2, shutdownListenerDatabaseEventHandler.getShutdownInvocations() );
    }

    private DatabaseManagementService getDatabaseManagementService()
    {
        GraphDatabaseFacadeFactory graphDatabaseFacadeFactory = new GraphDatabaseFacadeFactory( DatabaseInfo.COMMUNITY, CommunityEditionModule::new );
        return graphDatabaseFacadeFactory.initFacade( testDirectory.databaseDir(), Config.defaults(), GraphDatabaseDependencies.newDependencies(),
                new GraphDatabaseFacade() );
    }

    private static class ShutdownListenerDatabaseEventHandler extends DatabaseEventHandlerAdapter
    {
        private final AtomicLong shutdownInvocations = new AtomicLong();

        @Override
        public void beforeShutdown()
        {
            shutdownInvocations.incrementAndGet();
        }

        long getShutdownInvocations()
        {
            return shutdownInvocations.get();
        }
    }
}
