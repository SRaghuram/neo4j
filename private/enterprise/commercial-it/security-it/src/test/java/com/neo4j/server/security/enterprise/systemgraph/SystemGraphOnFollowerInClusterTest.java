/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.causalclustering.common.CausalClusteringTestHelpers;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.DatabaseStatus;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@ClusterExtension
@TestDirectoryExtension
class SystemGraphOnFollowerInClusterTest
{

    @Inject
    private ClusterFactory clusterFactory;

    @Inject
    private TestDirectory directory;

    private Cluster cluster;
    private String followerError = "Administration commands must be executed on the LEADER server.";

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException
    {
        var clusterConfig = ClusterConfig.clusterConfig().withSharedCoreParams( getConfig() ).withNumberOfCoreMembers( 3 );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @AfterEach
    void teardown()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
            cluster = null;
        }
    }

    // User commands tests

    @Test
    void showUsers() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            var result = sys.execute( "SHOW USERS" ).columnAs( "user" );
            assertEquals( "neo4j", result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // Also works on leader
        leaderTx( ( sys, tx ) ->
        {
            var result = sys.execute( "SHOW USERS" ).columnAs( "user" );
            assertEquals( "neo4j", result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void createUser() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            try
            {
                sys.execute( "CREATE USER foo SET PASSWORD 'f00'" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to create the specified user 'foo': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = sys.execute( "SHOW USERS" ).columnAs( "user" );
            assertEquals( "neo4j", result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            sys.execute( "CREATE USER foo SET PASSWORD 'f00'" );

            var result = sys.execute( "SHOW USERS" ).columnAs( "user" );
            assertEquals( Set.of( "neo4j", "foo" ), Set.of( result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void deleteUser() throws Exception
    {
        leaderTx( ( sys, tx ) ->
        {
            sys.execute( "CREATE USER foo SET PASSWORD '123'" );
            tx.commit();
        } );

        followerTx( ( sys, tx ) ->
        {
            try
            {
                sys.execute( "DROP USER foo" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to delete the specified user 'foo': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = sys.execute( "SHOW USERS" ).columnAs( "user" );
            assertEquals( Set.of( "neo4j", "foo" ), Set.of( result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            sys.execute( "DROP USER foo" );

            var result = sys.execute( "SHOW USERS" ).columnAs( "user" );
            assertEquals( Set.of( "neo4j" ), Set.of( result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void alterUser() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            try
            {
                sys.execute( "ALTER USER neo4j SET PASSWORD CHANGE NOT REQUIRED" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to alter the specified user 'neo4j': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = sys.execute( "SHOW USERS" ).columnAs( "passwordChangeRequired" );
            assertEquals( true, result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            sys.execute( "ALTER USER neo4j SET PASSWORD CHANGE NOT REQUIRED" );

            var result = sys.execute( "SHOW USERS" ).columnAs( "passwordChangeRequired" );
            assertEquals( false, result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void alterPassword() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            try
            {
                sys.execute( "ALTER CURRENT USER SET PASSWORD FROM 'old' TO 'new'" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "User '' failed to alter their own password: " + followerError, e.getMessage() );
            }
        } );

        // But it works on leader
        // Current user is '' so nothing will happen but we don't throw any exceptions
        leaderTx( ( sys, tx ) -> sys.execute( "ALTER CURRENT USER SET PASSWORD FROM 'old' TO 'new'" ) );
    }

    // Role commands tests

    @Test
    void showRoles() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            var result = sys.execute( "SHOW POPULATED ROLES" ).columnAs( "role" );
            assertEquals( PredefinedRoles.ADMIN, result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // Also works on leader
        leaderTx( ( sys, tx ) ->
        {
            var result = sys.execute( "SHOW POPULATED ROLES" ).columnAs( "role" );
            assertEquals( PredefinedRoles.ADMIN, result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void createRole() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            try
            {
                sys.execute( "CREATE ROLE foo" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to create the specified role 'foo': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = sys.execute( "SHOW ROLES" ).columnAs( "role" );
            assertEquals( Set.of( PredefinedRoles.ADMIN, PredefinedRoles.ARCHITECT, PredefinedRoles.PUBLISHER, PredefinedRoles.EDITOR, PredefinedRoles.READER ),
                    Set.of( result.next().toString(), result.next().toString(), result.next().toString(), result.next().toString(),
                            result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            sys.execute( "CREATE ROLE foo" );

            var result = sys.execute( "SHOW ROLES" ).columnAs( "role" );
            assertEquals(
                    Set.of( PredefinedRoles.ADMIN, PredefinedRoles.ARCHITECT, PredefinedRoles.PUBLISHER,
                            PredefinedRoles.EDITOR, PredefinedRoles.READER, "foo" ),
                    Set.of( result.next().toString(), result.next().toString(), result.next().toString(),
                            result.next().toString(), result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void deleteRole() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            try
            {
                sys.execute( "DROP ROLE foo" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to delete the specified role 'foo': " + followerError, e.getMessage() );
            }
        } );

        // But it works on leader (gives correct error for the command and setup)
        leaderTx( ( sys, tx ) ->
        {
            try
            {
                sys.execute( "DROP ROLE foo" );
                fail( "Should have failed to drop non-existing role, but succeeded." );
            }
            catch ( QueryExecutionException e )
            {
                assertEquals( "Failed to delete the specified role 'foo': Role does not exist.", e.getMessage() );
            }
        } );
    }

    @Test
    void grantRoleToUser() throws Exception
    {
        leaderTx( ( sys, tx ) ->
        {
            sys.execute( "CREATE USER foo SET PASSWORD '123'" );
            sys.execute( "CREATE ROLE bar" );
            tx.commit();
        } );

        followerTx( ( sys, tx ) ->
        {
            try
            {
                sys.execute( "GRANT ROLE bar TO foo" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to grant role 'bar' to user 'foo': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = sys.execute( "SHOW POPULATED ROLES" ).columnAs( "role" );
            assertEquals( PredefinedRoles.ADMIN, result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            sys.execute( "GRANT ROLE bar TO foo" );

            var result = sys.execute( "SHOW POPULATED ROLES" ).columnAs( "role" );
            assertEquals( Set.of( PredefinedRoles.ADMIN, "bar" ), Set.of( result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void revokeRoleFromUser() throws Exception
    {
        leaderTx( ( sys, tx ) ->
        {
            sys.execute( "CREATE USER foo SET PASSWORD '123'" );
            sys.execute( "CREATE ROLE bar" );
            sys.execute( "GRANT ROLE bar TO foo" );
            tx.commit();
        } );

        followerTx( ( sys, tx ) ->
        {
            try
            {
                sys.execute( "REVOKE ROLE bar FROM foo" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to revoke role 'bar' from user 'foo': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = sys.execute( "SHOW POPULATED ROLES" ).columnAs( "role" );
            assertEquals( Set.of( PredefinedRoles.ADMIN, "bar" ), Set.of( result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            sys.execute( "REVOKE ROLE bar FROM foo" );

            var result = sys.execute( "SHOW POPULATED ROLES" ).columnAs( "role" );
            assertEquals( PredefinedRoles.ADMIN, result.next().toString() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    // Privilege commands tests

    @Test
    void showPrivileges() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            var result = sys.execute( "SHOW ROLE " + PredefinedRoles.READER + " PRIVILEGES" ).columnAs( "role" );
            assertEquals( PredefinedRoles.READER, result.next() );
            result.close();
            tx.commit();
        } );

        // Also works on leader
        leaderTx( ( sys, tx ) ->
        {
            var result = sys.execute( "SHOW ROLE " + PredefinedRoles.READER + " PRIVILEGES" ).columnAs( "role" );
            assertEquals( PredefinedRoles.READER, result.next() );
            result.close();
            tx.commit();
        } );
    }

    @Test
    void grantTraverse() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            try
            {
                sys.execute( "GRANT TRAVERSE ON GRAPH * TO foo" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to grant traversal privilege to role 'foo': " + followerError, e.getMessage() );
            }
        } );

        // But it works on leader (gives correct error for the command and setup)
        leaderTx( ( sys, tx ) ->
        {
            try
            {
                sys.execute( "GRANT TRAVERSE ON GRAPH * TO foo" );
                fail( "Should have failed to grant traverse to non-existing role, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to grant traversal privilege to role 'foo': Role does not exist.", e.getMessage() );
            }
        } );
    }

    @Test
    void denyRead() throws Exception
    {
        leaderTx( ( sys, tx ) ->
        {
            sys.execute( "CREATE ROLE foo" );
            tx.commit();
        } );

        followerTx( ( sys, tx ) ->
        {
            try
            {
                sys.execute( "DENY READ {prop} ON GRAPH * TO foo" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to deny read privilege to role 'foo': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = sys.execute( "SHOW ROLE foo PRIVILEGES" ).columnAs( "role" );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            sys.execute( "DENY READ {prop} ON GRAPH * NODES * TO foo" );

            var result = sys.execute( "SHOW ROLE foo PRIVILEGES" ).columnAs( "role" );
            assertEquals( "foo", result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void revokeWrite() throws Exception
    {
        leaderTx( ( sys, tx ) ->
        {
            sys.execute( "CREATE ROLE foo" );
            sys.execute( "GRANT WRITE {*} ON GRAPH * TO foo" );
            tx.commit();
        } );

        followerTx( ( sys, tx ) ->
        {
            try
            {
                sys.execute( "REVOKE WRITE {*} ON GRAPH * FROM foo" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to revoke write privilege from role 'foo': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = sys.execute( "SHOW ROLE foo PRIVILEGES" ).columnAs( "grant" );
            assertTrue( result.hasNext() );
            result.close();
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            sys.execute( "REVOKE WRITE {*} ON GRAPH * FROM foo" );

            var result = sys.execute( "SHOW ROLE foo PRIVILEGES" ).columnAs( "grant" );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    // Database commands tests

    @Test
    void showDatabases() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            var result = sys.execute( "SHOW DATABASES" ).columnAs( "name" );
            assertEquals( Set.of( DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME ), Set.of( result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // Also works on leader
        leaderTx( ( sys, tx ) ->
        {
            var result = sys.execute( "SHOW DATABASES" ).columnAs( "name" );
            assertEquals( Set.of( DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME ), Set.of( result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void showDefaultDatabase() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            var result = sys.execute( "SHOW DEFAULT DATABASE" ).columnAs( "name" );
            assertEquals( DEFAULT_DATABASE_NAME, result.next().toString() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // Also works on leader
        leaderTx( ( sys, tx ) ->
        {
            var result = sys.execute( "SHOW DEFAULT DATABASE" ).columnAs( "name" );
            assertEquals( DEFAULT_DATABASE_NAME, result.next().toString() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void showDatabase() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            var result = sys.execute( "SHOW DATABASE " + DEFAULT_DATABASE_NAME ).columnAs( "name" );
            assertEquals( DEFAULT_DATABASE_NAME, result.next().toString() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // Also works on leader
        leaderTx( ( sys, tx ) ->
        {
            var result = sys.execute( "SHOW DATABASE " + DEFAULT_DATABASE_NAME ).columnAs( "name" );
            assertEquals( DEFAULT_DATABASE_NAME, result.next().toString() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void createDatabase() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            try
            {
                sys.execute( "CREATE DATABASE foo" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to create the specified database 'foo': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = sys.execute( "SHOW DATABASE foo" ).columnAs( "name" );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            sys.execute( "CREATE DATABASE foo" );

            var result = sys.execute( "SHOW DATABASE foo" ).columnAs( "name" );
            assertTrue( result.hasNext() );
            result.close();
            tx.commit();
        } );
        CausalClusteringTestHelpers.assertDatabaseEventuallyStarted( "foo", cluster );
    }

    @Test
    void deleteDatabase() throws Exception
    {
        leaderTx( ( sys, tx ) ->
        {
            sys.execute( "CREATE DATABASE foo" );
            tx.commit();
        } );
        CausalClusteringTestHelpers.assertDatabaseEventuallyStarted( "foo", cluster );

        followerTx( ( sys, tx ) ->
        {
            try
            {
                sys.execute( "DROP DATABASE foo" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to delete the specified database 'foo': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = sys.execute( "SHOW DATABASE foo" ).columnAs( "name" );
            assertTrue( result.hasNext() );
            result.close();
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            sys.execute( "DROP DATABASE foo" );

            var result = sys.execute( "SHOW DATABASE foo" ).columnAs( "name" );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
        CausalClusteringTestHelpers.assertDatabaseDoesNotExist( "foo", cluster );
    }

    @Test
    void startDatabase() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            try
            {
                sys.execute( "START DATABASE foo" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to start the specified database 'foo': " + followerError, e.getMessage() );
            }
        } );

        // But it works on leader (gives correct error for the command and setup)
        leaderTx( ( sys, tx ) ->
        {
            try
            {
                sys.execute( "START DATABASE foo" );
                fail( "Should have failed to start non-existing database, but succeeded." );
            }
            catch ( QueryExecutionException e )
            {
                assertEquals( "Failed to start the specified database 'foo': Database does not exist.", e.getMessage() );
            }
        } );
    }

    @Test
    void stopDatabase() throws Exception
    {
        leaderTx( ( sys, tx ) ->
        {
            sys.execute( "CREATE DATABASE foo" );
            tx.commit();
        } );
        CausalClusteringTestHelpers.assertDatabaseEventuallyStarted( "foo", cluster );

        followerTx( ( sys, tx ) ->
        {
            try
            {
                sys.execute( "STOP DATABASE foo" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to stop the specified database 'foo': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = sys.execute( "SHOW DATABASE foo" ).columnAs( "status" );
            assertEquals( DatabaseStatus.Online().stringValue(), result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            sys.execute( "STOP DATABASE foo" );

            var result = sys.execute( "SHOW DATABASE foo" ).columnAs( "status" );
            assertEquals( DatabaseStatus.Offline().stringValue(), result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
        CausalClusteringTestHelpers.assertDatabaseEventuallyStopped( "foo", cluster );
    }

    // Help methods

    /**
     * Perform a transaction on system database against the leader of the core cluster, retrying as necessary.
     */
    private void leaderTx( BiConsumer<GraphDatabaseFacade,Transaction> op ) throws Exception
    {
        cluster.coreTx( SYSTEM_DATABASE_NAME, op, (int) MINUTES.toMillis( 3 ), MILLISECONDS );
    }

    /**
     * Perform a transaction on system database against a follower of the core cluster, retrying as necessary.
     */
    private void followerTx( BiConsumer<GraphDatabaseFacade,Transaction> op ) throws Exception
    {
        cluster.coreTx( SYSTEM_DATABASE_NAME, Role.FOLLOWER, op, (int) MINUTES.toMillis( 3 ), MILLISECONDS );
    }

    private static Map<String,String> getConfig()
    {
        var config = new HashMap<String,String>();
        config.put( GraphDatabaseSettings.auth_enabled.name(), "true" );
        return config;
    }
}
