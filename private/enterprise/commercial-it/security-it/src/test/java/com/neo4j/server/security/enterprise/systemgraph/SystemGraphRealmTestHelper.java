/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.ExcessiveAttemptsException;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.server.security.auth.ShiroAuthToken;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.string.UTF8;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

class SystemGraphRealmTestHelper
{
    public static class TestDatabaseManager extends LifecycleAdapter implements DatabaseManager<StandaloneDatabaseContext>
    {
        GraphDatabaseFacade testSystemDb;

        TestDatabaseManager( TestDirectory testDir )
        {
            testSystemDb = (GraphDatabaseFacade) new TestCommercialGraphDatabaseFactory()
                    .newImpermanentDatabaseBuilder( testDir.databaseDir() )
                    .setConfig( GraphDatabaseSettings.auth_enabled, "false" )
                    .newGraphDatabase();
        }

        @Override
        public Optional<StandaloneDatabaseContext> getDatabaseContext( DatabaseId databaseId )
        {
            if ( SYSTEM_DATABASE_NAME.equals( databaseId.name() ) )
            {
                DependencyResolver dependencyResolver = testSystemDb.getDependencyResolver();
                Database database = dependencyResolver.resolveDependency( Database.class );
                return Optional.of( new StandaloneDatabaseContext( database, testSystemDb ) );
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
        public SortedMap<DatabaseId,StandaloneDatabaseContext> registeredDatabases()
        {
            return Collections.emptySortedMap();
        }
    }

    static ShiroAuthToken testAuthenticationToken( String username, String password )
    {
        Map<String,Object> authToken = new TreeMap<>();
        authToken.put( AuthToken.PRINCIPAL, username );
        authToken.put( AuthToken.CREDENTIALS, UTF8.encode( password ) );
        return new ShiroAuthToken( authToken );
    }

    static void assertAuthenticationSucceeds( BasicSystemGraphRealm realm, String username )
    {
        // NOTE: Password is the same as username
        // Try twice to rule out differences if authentication info has been cached or not
        assertNotNull( realm.getAuthenticationInfo( testAuthenticationToken( username, username ) ) );
        assertNotNull( realm.getAuthenticationInfo( testAuthenticationToken( username, username ) ) );

        // Also test the non-cached result explicitly
        assertNotNull( realm.doGetAuthenticationInfo( testAuthenticationToken( username, username ) ) );
    }

    static void assertAuthenticationFailsWithTooManyAttempts( BasicSystemGraphRealm realm, String username, int attempts )
    {
        // NOTE: Password is the same as username
        for ( int i = 0; i < attempts; i++ )
        {
            try
            {
                assertNull( realm.getAuthenticationInfo( testAuthenticationToken( username, "wrong_password" ) ) );
            }
            catch ( ExcessiveAttemptsException e )
            {
                // This is what we were really looking for
                return;
            }
            catch ( AuthenticationException e )
            {
                // This is expected
            }
        }
        fail( "Did not get an ExcessiveAttemptsException after " + attempts + " attempts." );
    }
}
