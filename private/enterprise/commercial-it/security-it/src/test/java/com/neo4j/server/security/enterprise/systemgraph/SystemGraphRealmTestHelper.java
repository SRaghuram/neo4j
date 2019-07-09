/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;

import java.util.Collections;
import java.util.Optional;
import java.util.SortedMap;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

class SystemGraphRealmTestHelper
{
    public static class TestDatabaseManager extends LifecycleAdapter implements DatabaseManager<StandaloneDatabaseContext>
    {
        GraphDatabaseFacade testSystemDb;
        private final DatabaseManagementService managementService;

        TestDatabaseManager( TestDirectory testDir )
        {
            managementService = new TestCommercialDatabaseManagementServiceBuilder( testDir.databaseDir() ).impermanent()
                    .setConfig( GraphDatabaseSettings.auth_enabled, "false" ).build();
            testSystemDb = (GraphDatabaseFacade) managementService.database( SYSTEM_DATABASE_NAME );
        }

        DatabaseManagementService getManagementService()
        {
            return managementService;
        }

        @Override
        public Optional<StandaloneDatabaseContext> getDatabaseContext( DatabaseId databaseId )
        {
            if ( DatabaseId.isSystemDatabase( databaseId ) )
            {
                DependencyResolver dependencyResolver = testSystemDb.getDependencyResolver();
                Database database = dependencyResolver.resolveDependency( Database.class );
                return Optional.of( new StandaloneDatabaseContext( database ) );
            }
            return Optional.empty();
        }

        @Override
        public StandaloneDatabaseContext createDatabase( DatabaseId databaseId )
        {
            throw new UnsupportedOperationException( "Call to createDatabase not expected" );
        }

        @Override
        public void dropDatabase( DatabaseId databaseId )
        {
        }

        @Override
        public void stopDatabase( DatabaseId databaseId )
        {
        }

        @Override
        public void startDatabase( DatabaseId databaseId )
        {
        }

        @Override
        public void initialiseDefaultDatabases()
        {
        }

        @Override
        public SortedMap<DatabaseId,StandaloneDatabaseContext> registeredDatabases()
        {
            return Collections.emptySortedMap();
        }
    }
}
