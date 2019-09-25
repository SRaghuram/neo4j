/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
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
        private final DatabaseIdRepository.Caching databaseIdRepository = new TestDatabaseIdRepository();

        TestDatabaseManager( TestDirectory testDir )
        {
            managementService = new TestEnterpriseDatabaseManagementServiceBuilder( testDir.homeDir() ).impermanent()
                    .setConfig( GraphDatabaseSettings.auth_enabled, false ).build();
            testSystemDb = (GraphDatabaseFacade) managementService.database( SYSTEM_DATABASE_NAME );
        }

        DatabaseManagementService getManagementService()
        {
            return managementService;
        }

        boolean userHasRole( String user, String role )
        {
            List<Object> usersWithRole = new LinkedList<>();
            try ( Transaction tx = testSystemDb.beginTx() )
            {
                Result result = tx.execute( "SHOW USERS" );
                result.stream().filter( u -> ((List) u.get( "roles" )).contains( role ) ).map( u -> u.get( "user" ) ).forEach( usersWithRole::add );
                tx.commit();
                result.close();
            }

            return usersWithRole.contains( user );
        }

        @Override
        public Optional<StandaloneDatabaseContext> getDatabaseContext( DatabaseId databaseId )
        {
            if ( databaseId.isSystemDatabase() )
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
        public DatabaseIdRepository.Caching databaseIdRepository()
        {
            return databaseIdRepository;
        }

        @Override
        public SortedMap<DatabaseId,StandaloneDatabaseContext> registeredDatabases()
        {
            return Collections.emptySortedMap();
        }
    }
}
