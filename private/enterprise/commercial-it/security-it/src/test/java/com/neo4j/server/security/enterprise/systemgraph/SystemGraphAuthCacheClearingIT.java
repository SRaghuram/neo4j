/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.kernel.enterprise.api.security.CommercialAuthManager;
import com.neo4j.kernel.enterprise.api.security.CommercialSecurityContext;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.security.auth.SecurityTestUtils;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ClusterExtension
@ExtendWith( { TestDirectoryExtension.class } )
class SystemGraphAuthCacheClearingIT
{

    @Inject
    private ClusterFactory clusterFactory;

    @Inject
    private TestDirectory directory;

    private File tempLogsDir;

    @BeforeEach
    void setup()
    {
        tempLogsDir = this.directory.directory( "tempLogs" );
    }

    @Test
    void systemDbUpdatesShouldClearAuthCacheInStandalone() throws Exception
    {
        // Given a standalone db
        File tempLog = queryLog( 0 );
        tempLog.createNewFile();
        DatabaseManagementService dbms = new TestCommercialDatabaseManagementServiceBuilder( directory.storeDir() )
                .setConfigRaw( getConfig() )
                .setConfig( GraphDatabaseSettings.log_queries_filename, tempLog.toPath() )
                .build();

        GraphDatabaseAPI systemDb = (GraphDatabaseAPI) dbms.database( SYSTEM_DATABASE_NAME );

        try ( Transaction tx = systemDb.beginTransaction( KernelTransaction.Type.explicit, CommercialSecurityContext.AUTH_DISABLED ) )
        {
            systemDb.execute( "CREATE USER foo SET PASSWORD 'f00'" );
            systemDb.execute( "CREATE USER bar SET PASSWORD 'b4r'" );
            tx.success();
        }

        var dbs = Collections.singletonList( systemDb );
        var queryLogs = Collections.singletonList( tempLog );

        // When initial login attempt is made
        allCanAuth( dbs, "foo", "f00" );

        // Then a login query against the system database should be logged
        assertEventually( () -> nLoginsAttempted( queryLogs, 1, "foo" ), is( true ), 5, TimeUnit.SECONDS );

        // When subsequent attempts are made
        allCanAuth( dbs, "foo", "f00" );
        allCanAuth( dbs, "foo", "f00" );
        allCanAuth( dbs, "bar", "b4r" );

        // Then only those for new users should cause a login query against the system database
        assertEventually( () -> nLoginsAttempted( queryLogs, 1, "bar" ), is( true ), 5, TimeUnit.SECONDS );
        assertThat( nLoginsAttempted( queryLogs, 1, "foo" ), is( true ) );

        // When changing the system database
        try ( Transaction tx = systemDb.beginTransaction( KernelTransaction.Type.explicit, CommercialSecurityContext.AUTH_DISABLED ) )
        {
            systemDb.execute( "DROP USER bar" );
            tx.success();
        }

        // Then the auth cache should be cleared and logins for previous users require a login query against the system database
        allCanAuth( dbs, "foo", "f00" );
        assertEventually( () -> nLoginsAttempted( queryLogs, 2, "foo" ), is( false ), 5, TimeUnit.SECONDS );
    }

    @Test
    void systemDbUpdatesShouldClearAuthCacheInCluster() throws Exception
    {
        // Given a cluster and 2 users
        var clusterConfig = ClusterConfig.clusterConfig()
            .withSharedCoreParams( getConfig() )
            .withNumberOfCoreMembers( 3 );

        clusterConfig.withInstanceCoreParam( GraphDatabaseSettings.log_queries_filename, i -> queryLog( i ).getAbsolutePath() );
        var cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        cluster.systemTx( ( sys, tx ) ->
        {
            sys.execute( "CREATE USER foo SET PASSWORD 'f00'" );
            sys.execute( "CREATE USER bar SET PASSWORD 'b4r'" );
            tx.success();
        } );

        var queryLogs = clusterQueryLogs( cluster );
        var clusterSystemDbs = clusterSystemDbs( cluster );

        // When initial login attempt is made
        allCanAuth( clusterSystemDbs, "foo", "f00" );

        // Then a login query against the system database should be logged
        assertEventually( () -> nLoginsAttempted( queryLogs, 1, "foo" ), is( true ), 5, TimeUnit.SECONDS );

        // When subsequent attempts are made
        allCanAuth( clusterSystemDbs, "foo", "f00" );
        allCanAuth( clusterSystemDbs, "foo", "f00" );
        allCanAuth( clusterSystemDbs, "bar", "b4r" );

        // Then only those for new users should cause a login query against the system database
        assertEventually( () -> nLoginsAttempted( queryLogs, 1, "bar" ),
                is( true ), 5, TimeUnit.SECONDS );
        assertThat( nLoginsAttempted( queryLogs, 1, "foo" ), is( true ) );

        // When changing the system database
        cluster.systemTx( ( sys, tx ) ->
        {
            sys.execute( "DROP USER bar" );
            tx.success();
        } );

        // Then the auth cache should be cleared and logins for previous users require a login query against the system database
        allCanAuth( clusterSystemDbs, "foo", "f00" );
        assertEventually( () -> nLoginsAttempted( queryLogs, 2, "foo" ),
                is( false ), 5, TimeUnit.SECONDS );
    }

    private List<GraphDatabaseAPI> clusterSystemDbs( Cluster cluster )
    {
        return cluster.allMembers()
                .map( ClusterMember::systemDatabase )
                .collect( Collectors.toList() );
    }

    private File queryLog( int mId )
    {
        return new File( tempLogsDir, format( "query_log_%d", mId ) );
    }

    private List<File> clusterQueryLogs( Cluster cluster )
    {
        return cluster.allMembers()
                .map( ClusterMember::serverId )
                .map( this::queryLog )
                .collect( Collectors.toList() );
    }

    private boolean allCanAuth( List<GraphDatabaseAPI> dbs, String user, String password )
    {
        return dbs.stream()
                .map( db -> canAuthenticateAgainstDbms( db, SecurityTestUtils.authToken( user, password ) ) )
                .reduce( true, ( l, r ) -> l && r );
    }

    private boolean nLoginsAttempted( List<File> queryLogs, long n, String username )
    {
        return allLoginAttempts( queryLogs, username ).stream().allMatch( l -> l.equals( n ) );
    }

    private List<Long> allLoginAttempts( List<File> queryLogNames, String username )
    {
        return queryLogNames.stream()
                .map( logFile -> countLoginAttempts( logFile, username ) )
                .collect( Collectors.toList() );
    }

    private File tempLog( String queryLogName, File tempParent )
    {
        return new File( tempParent, queryLogName );
    }

    // TODO: Improve proxy for detecting whether the cache is working or not
    private long countLoginAttempts( File queryLog, String username )
    {
        try
        {
            List<String> strings = Files.readAllLines( queryLog.toPath() );
            return Files.readAllLines( queryLog.toPath() ).stream()
                    .filter( l -> l.contains( loginQuery( username ) ) )
                    .count();
        }
        catch ( IOException e )
        {
            return -1;
        }
    }

    private static String loginQuery( String username )
    {
        return format( "MATCH (u:User {name: $name}) RETURN u.credentials, u.passwordChangeRequired, u.suspended - {name -> %s}", username );
    }

    private boolean canAuthenticateAgainstDbms( GraphDatabaseAPI database, Map<String,Object> credentials )
    {
        var authManager = database.getDependencyResolver().resolveDependency( CommercialAuthManager.class );
        try
        {
            authManager.login( credentials );
            return true;
        }
        catch ( InvalidAuthTokenException e )
        {
            return false;
        }
    }

    private static Map<String,String> getConfig()
    {
        var config = new HashMap<String,String>();
        config.put( GraphDatabaseSettings.auth_enabled.name(), "true" );
        config.put( GraphDatabaseSettings.log_queries.name(), "true" );
        config.put( GraphDatabaseSettings.log_queries_parameter_logging_enabled.name(), "true" );
        config.put( SecuritySettings.authentication_providers.name(), SecuritySettings.NATIVE_REALM_NAME );
        return config;
    }
}
