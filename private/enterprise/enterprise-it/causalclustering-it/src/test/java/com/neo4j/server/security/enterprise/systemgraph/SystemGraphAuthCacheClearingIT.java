/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.configuration.SecuritySettings;
import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.JUnitException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.FALSE;
import static org.neo4j.test.conditions.Conditions.TRUE;

@ClusterExtension
@TestDirectoryExtension
class SystemGraphAuthCacheClearingIT
{

    @Inject
    private ClusterFactory clusterFactory;

    @Inject
    private TestDirectory directory;

    private DatabaseManagementService dbms;
    private Cluster cluster;

    @AfterEach
    void teardown()
    {
        if ( dbms != null )
        {
            dbms.shutdown();
            dbms = null;
        }
        if ( cluster != null )
        {
            cluster.shutdown();
            cluster = null;
        }
    }

    @Test
    void systemDbUpdatesShouldClearAuthCacheInStandalone()
    {
        // Given a standalone db
        dbms = new TestEnterpriseDatabaseManagementServiceBuilder( directory.homePath() )
                .impermanent()
                .setConfigRaw( getConfig() )
                .build();

        var systemDb = (GraphDatabaseAPI) dbms.database( SYSTEM_DATABASE_NAME );
        var dbs = Collections.singletonList( systemDb );

        try ( Transaction tx = systemDb.beginTransaction( EXPLICIT, EnterpriseSecurityContext.AUTH_DISABLED ) )
        {
            tx.execute( "CREATE USER foo SET PASSWORD 'f00'" );
            tx.commit();
        }

        // When initial login attempt is made, user is stored in cache
        assertEventually( () -> allCanAuth( dbs, "foo", "f00" ), TRUE, 30, SECONDS );

        // When changing the system database
        try ( Transaction tx = systemDb.beginTransaction( EXPLICIT, EnterpriseSecurityContext.AUTH_DISABLED ) )
        {
            tx.execute( "ALTER USER foo SET PASSWORD 'b4r'" );
            tx.commit();
        }

        // Then the auth cache should be cleared and login with new password is required
        assertEventually( () -> allCanAuth( dbs, "foo", "f00" ), FALSE, 30, SECONDS );
        assertTrue( () -> allCanAuth( dbs, "foo", "b4r" ) );
    }

    @Test
    void systemDbUpdatesShouldClearPrivilegeCacheInStandalone()
    {
        // Given a standalone db
        dbms = new TestEnterpriseDatabaseManagementServiceBuilder( directory.homePath() )
                .impermanent()
                .setConfigRaw( getConfig() )
                .build();

        var systemDb = (GraphDatabaseAPI) dbms.database( SYSTEM_DATABASE_NAME );
        var userDB = (GraphDatabaseAPI) dbms.database( DEFAULT_DATABASE_NAME );
        var userDbs = Collections.singletonList( userDB );

        try ( Transaction tx = systemDb.beginTransaction( EXPLICIT, EnterpriseSecurityContext.AUTH_DISABLED ) )
        {
            tx.execute( "REVOKE ACCESS ON DEFAULT DATABASE FROM PUBLIC" );
            tx.execute( "CREATE USER foo SET PASSWORD 'f00' CHANGE NOT REQUIRED" );
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT ROLE role TO foo" );
            tx.commit();
        }

        // When initial login attempt is made, user is stored in cache
        assertEventually( () -> allCanAuth( userDbs, "foo", "f00" ), TRUE, 30, SECONDS );
        assertFalse( () -> allCanExecuteQuery( userDbs, "foo", "f00", "MATCH (n) RETURN count(n)" ) );

        // When changing the system database
        try ( Transaction tx = systemDb.beginTransaction( EXPLICIT, EnterpriseSecurityContext.AUTH_DISABLED ) )
        {
            tx.execute( "GRANT ACCESS ON DATABASE * TO role" );
            tx.commit();
        }

        // Then the privilege cache should be cleared giving read access
        assertEventually( () -> allCanExecuteQuery( userDbs, "foo", "f00", "MATCH (n) RETURN count(n)" ), TRUE, 30, SECONDS );

        // When changing the system database
        try ( Transaction tx = systemDb.beginTransaction( EXPLICIT, EnterpriseSecurityContext.AUTH_DISABLED ) )
        {
            tx.execute( "REVOKE ACCESS ON DATABASE * FROM role" );
            tx.commit();
        }

        // Then the privilege cache should be cleared giving no read access
        assertEventually( () -> allCanExecuteQuery( userDbs, "foo", "f00", "MATCH (n) RETURN count(n)" ), FALSE, 30, SECONDS );
    }

    @Test
    void systemDbUpdatesShouldClearAuthCacheInCluster() throws Exception
    {
        // Given a cluster with 1 user
        var clusterConfig = ClusterConfig.clusterConfig()
            .withSharedCoreParams( getConfig() )
            .withNumberOfCoreMembers( 3 );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
        var clusterSystemDbs = clusterSystemDbs( cluster );

        cluster.systemTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE USER foo SET PASSWORD 'f00'" );
            tx.commit();
        } );

        // When initial login attempt is made, user is stored in cache
        assertEventually( () -> allCanAuth( clusterSystemDbs, "foo", "f00" ), TRUE, 30, SECONDS );

        // When changing the system database
        cluster.systemTx( ( sys, tx ) ->
        {
            tx.execute( "ALTER USER foo SET PASSWORD 'b4r'" );
            tx.commit();
        } );

        // Then the auth cache should be cleared and login with new password is required
        assertEventually( () -> allCanAuth( clusterSystemDbs, "foo", "f00" ), FALSE, 30, SECONDS );
        assertEventually( () -> allCanAuth( clusterSystemDbs, "foo", "b4r" ), TRUE, 30, SECONDS );
    }

    @Test
    void systemDbUpdatesShouldClearPrivilegeCacheInCluster() throws Exception
    {
        // Given a cluster and 1 user with no privileges
        var clusterConfig = ClusterConfig.clusterConfig()
                .withSharedCoreParams( getConfig() )
                .withNumberOfCoreMembers( 3 );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
        var userDbs = clusteruserDbs( cluster );

        cluster.systemTx( ( sys, tx ) ->
        {
            tx.execute( "REVOKE ACCESS ON DEFAULT DATABASE FROM PUBLIC" );
            tx.execute( "CREATE USER foo SET PASSWORD 'f00' CHANGE NOT REQUIRED" );
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT ROLE role TO foo" );
            tx.commit();
        } );

        // When initial login attempt is made, role to privilege is cached
        assertEventually( () -> allCanAuth( userDbs, "foo", "f00" ), TRUE, 30, SECONDS );
        assertFalse( () -> allCanExecuteQuery( userDbs, "foo", "f00", "MATCH (n) RETURN count(n)" ) );

        // When granting privilege
        cluster.systemTx( ( sys, tx ) ->
        {
            tx.execute( "GRANT ACCESS ON DATABASE * TO role" );
            tx.commit();
        } );

        // Then the privilege cache should be cleared giving read access
        assertEventually( () -> allCanExecuteQuery( userDbs, "foo", "f00", "MATCH (n) RETURN count(n)" ), TRUE, 30, SECONDS );

        // When revoking privilege
        cluster.systemTx( ( sys, tx ) ->
        {
            tx.execute( "REVOKE ACCESS ON DATABASE * FROM role" );
            tx.commit();
        } );

        // Then the privilege cache should be cleared giving no read access
        assertEventually( () -> allCanExecuteQuery( userDbs, "foo", "f00", "MATCH (n) RETURN count(n)" ), FALSE, 30, SECONDS );
    }

    private List<GraphDatabaseAPI> clusterSystemDbs( Cluster cluster )
    {
        return cluster.allMembers().stream()
                .map( ClusterMember::systemDatabase )
                .collect( Collectors.toList() );
    }

    private List<GraphDatabaseAPI> clusteruserDbs( Cluster cluster )
    {
        return cluster.allMembers().stream()
                .map( ClusterMember::defaultDatabase )
                .collect( Collectors.toList() );
    }

    private boolean allCanAuth( List<GraphDatabaseAPI> dbs, String user, String password )
    {
        return dbs.stream()
                .map( db -> canAuthenticateAgainstDbms( db, authToken( user, password ) ) )
                .reduce( true, ( l, r ) -> l && r );
    }

    private boolean canAuthenticateAgainstDbms( GraphDatabaseAPI database, Map<String,Object> authToken )
    {
        var result = login( database, authToken ).subject().getAuthenticationResult();
        return result == AuthenticationResult.SUCCESS || result == AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
    }

    private boolean allCanExecuteQuery( List<GraphDatabaseAPI> dbs, String user, String password, String query )
    {
        return dbs.stream()
                .map( db -> canExecuteQuery( db, authToken( user, password ), query ) )
                .reduce( true, ( l, r ) -> l && r );
    }

    private boolean canExecuteQuery( GraphDatabaseAPI database, Map<String,Object> authToken, String query )
    {
        var context = login( database, authToken );
        try ( var tx = database.beginTransaction( EXPLICIT, context ) )
        {
            Result result = tx.execute( query );
            result.accept( row -> true );
            tx.commit();
        }
        catch ( AuthorizationViolationException e )
        {
            return false;
        }
        return true;
    }

    private LoginContext login( GraphDatabaseAPI database, Map<String,Object> credentials )
    {
        var authManager = database.getDependencyResolver().resolveDependency( EnterpriseAuthManager.class );
        try
        {
            return authManager.login( credentials );
        }
        catch ( InvalidAuthTokenException e )
        {
            throw new JUnitException( "Failure in test setup, invalid auth token", e );
        }
    }

    private static Map<String,String> getConfig()
    {
        var config = new HashMap<String,String>();
        config.put( GraphDatabaseSettings.auth_enabled.name(), "true" );
        config.put( SecuritySettings.auth_cache_use_ttl.name(), "false" ); // disable cache timeout
        config.put( GraphDatabaseSettings.auth_max_failed_attempts.name(), "0" ); // disable rate limit
        return config;
    }
}
