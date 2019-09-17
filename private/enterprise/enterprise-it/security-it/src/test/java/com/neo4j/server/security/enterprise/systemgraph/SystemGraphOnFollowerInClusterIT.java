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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.DatabaseStatus;
import org.neo4j.dbms.api.DatabaseNotFoundException;
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
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@TestInstance( PER_CLASS )
@ClusterExtension
@TestDirectoryExtension
class SystemGraphOnFollowerInClusterIT
{

    @Inject
    private static ClusterFactory clusterFactory;

    @Inject
    private TestDirectory directory;

    private static Cluster cluster;
    private static String followerError = "Administration commands must be executed on the LEADER server.";
    private String userName = "foo";
    private String roleName = "bar";
    private String roleName2 = "foo";
    private String dbName = "foo";

    @BeforeAll
    static void setUp() throws ExecutionException, InterruptedException
    {
        var clusterConfig = ClusterConfig.clusterConfig().withSharedCoreParams( getConfig() ).withNumberOfReadReplicas( 0 );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @AfterEach
    void teardown() throws Exception
    {
        if ( cluster != null )
        {
            leaderTx( ( sys, tx ) ->
            {
                tx.execute( "ALTER USER neo4j SET PASSWORD CHANGE REQUIRED" );
                tx.commit();
            } );
            List<String> toBeDropped = Arrays.asList( "DATABASE " + dbName, "ROLE " + roleName, "ROLE " + roleName2, "USER " + userName );
            for ( String dropMe : toBeDropped )
            {
                leaderTx( ( sys, tx ) ->
                {
                    tx.execute( "DROP " + dropMe + " IF EXISTS" );
                    tx.commit();
                } );
            }
            CausalClusteringTestHelpers.assertDatabaseEventuallyDoesNotExist( dbName, cluster );
            CausalClusteringTestHelpers.assertRoleDoesNotExist( roleName, cluster );
            CausalClusteringTestHelpers.assertRoleDoesNotExist( roleName2, cluster );
            CausalClusteringTestHelpers.assertUserDoesNotExist( userName, cluster );
        }
    }

    // User commands tests

    @Test
    void showUsers() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW USERS" ).columnAs( "user" );
            assertEquals( "neo4j", result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // Also works on leader
        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW USERS" ).columnAs( "user" );
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
                tx.execute( "CREATE USER " + userName + " SET PASSWORD 'f00'" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to create the specified user '" + userName + "': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW USERS" ).columnAs( "user" );
            assertEquals( "neo4j", result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE USER " + userName + " SET PASSWORD 'f00'" );

            var result = tx.execute( "SHOW USERS" ).columnAs( "user" );
            assertEquals( Set.of( "neo4j", userName ), Set.of( result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void createUserIfNotExistsNonExisting() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "CREATE USER " + userName + " IF NOT EXISTS SET PASSWORD 'f00'" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to create the specified user '" + userName + "': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW USERS" ).columnAs( "user" );
            assertEquals( "neo4j", result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE USER " + userName + " IF NOT EXISTS SET PASSWORD 'f00'" );

            var result = tx.execute( "SHOW USERS" ).columnAs( "user" );
            assertEquals( Set.of( "neo4j", userName ), Set.of( result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void createUserIfNotExistsWhenExisting() throws Exception
    {
        // set-up user
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE USER " + userName + " SET PASSWORD 'f00' CHANGE REQUIRED" );

            var result = tx.execute( "SHOW USERS" ).columnAs( "passwordChangeRequired" );
            assertEquals( List.of( true, true ), List.of( result.next(), result.next() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        followerTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "CREATE USER " + userName + " IF NOT EXISTS SET PASSWORD 'f00' CHANGE NOT REQUIRED" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to create the specified user '" + userName + "': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW USERS" ).columnAs( "passwordChangeRequired" );
            assertEquals( List.of( true, true ), List.of( result.next(), result.next() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But works on leader
        leaderTx( ( sys, tx ) ->
        {
            // gives no error, does nothing
            tx.execute( "CREATE USER " + userName + " IF NOT EXISTS SET PASSWORD 'f00' CHANGE NOT REQUIRED" );

            var result = tx.execute( "SHOW USERS" ).columnAs( "passwordChangeRequired" );
            assertEquals( List.of( true, true ), List.of( result.next(), result.next() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void createOrReplaceUserNonExisting() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "CREATE OR REPLACE USER " + userName + " SET PASSWORD 'f00'" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                // First fail on trying to delete the old user
                assertEquals( "Failed to delete the specified user '" + userName + "': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW USERS" ).columnAs( "user" );
            assertEquals( "neo4j", result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE OR REPLACE USER " + userName + " SET PASSWORD 'f00'" );

            var result = tx.execute( "SHOW USERS" ).columnAs( "user" );
            assertEquals( Set.of( "neo4j", userName ), Set.of( result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void createOrReplaceUserWhenExisting() throws Exception
    {
        // set-up user
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE USER " + userName + " SET PASSWORD 'f00' CHANGE REQUIRED" );

            var result = tx.execute( "SHOW USERS" ).columnAs( "passwordChangeRequired" );
            assertEquals( List.of( true, true ), List.of( result.next(), result.next() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        followerTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "CREATE OR REPLACE USER " + userName + " SET PASSWORD 'f00' CHANGE NOT REQUIRED" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                // First fail on deleting the old user
                assertEquals( "Failed to delete the specified user '" + userName + "': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW USERS" ).columnAs( "passwordChangeRequired" );
            assertEquals( List.of( true, true ), List.of( result.next(), result.next() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But works on leader
        leaderTx( ( sys, tx ) ->
        {
            // replaces user
            tx.execute( "CREATE OR REPLACE USER " + userName + " SET PASSWORD 'f00' CHANGE NOT REQUIRED" );

            var result = tx.execute( "SHOW USERS" ).columnAs( "passwordChangeRequired" );
            assertEquals( Set.of( true, false ), Set.of( result.next(), result.next() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void deleteUser() throws Exception
    {
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE USER " + userName + " SET PASSWORD '123'" );
            tx.commit();
        } );

        followerTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "DROP USER " + userName );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to delete the specified user '" + userName + "': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW USERS" ).columnAs( "user" );
            assertEquals( Set.of( "neo4j", userName ), Set.of( result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "DROP USER " + userName );

            var result = tx.execute( "SHOW USERS" ).columnAs( "user" );
            assertEquals( Set.of( "neo4j" ), Set.of( result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void deleteUserIfExistsNonExisting() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "DROP USER " + userName + " IF EXISTS" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to delete the specified user '" + userName + "': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW USERS" ).columnAs( "user" );
            assertEquals( Set.of( "neo4j" ), Set.of( result.next() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            // gives no error, does nothing
            tx.execute( "DROP USER " + userName + " IF EXISTS" );

            var result = tx.execute( "SHOW USERS" ).columnAs( "user" );
            assertEquals( Set.of( "neo4j" ), Set.of( result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void deleteUserIfExistsWhenExisting() throws Exception
    {
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE USER " + userName + " SET PASSWORD '123'" );
            tx.commit();
        } );

        followerTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "DROP USER " + userName + " IF EXISTS" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to delete the specified user '" + userName + "': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW USERS" ).columnAs( "user" );
            assertEquals( Set.of( "neo4j", userName ), Set.of( result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "DROP USER " + userName + " IF EXISTS" );

            var result = tx.execute( "SHOW USERS" ).columnAs( "user" );
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
                tx.execute( "ALTER USER neo4j SET PASSWORD CHANGE NOT REQUIRED" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to alter the specified user 'neo4j': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW USERS" ).columnAs( "passwordChangeRequired" );
            assertEquals( true, result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "ALTER USER neo4j SET PASSWORD CHANGE NOT REQUIRED" );

            var result = tx.execute( "SHOW USERS" ).columnAs( "passwordChangeRequired" );
            assertEquals( false, result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void alterUserNonExisting() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "ALTER USER " + userName + " SET PASSWORD CHANGE NOT REQUIRED" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to alter the specified user '" + userName + "': " + followerError, e.getMessage() );
            }
        } );

        // But it works on leader (gives correct error for the command and setup)
        leaderTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "ALTER USER " + userName + " SET PASSWORD CHANGE NOT REQUIRED" );
                fail( "Should have failed to alter non-existing user, but succeeded." );
            }
            catch ( QueryExecutionException e )
            {
                assertEquals( "Failed to alter the specified user '" + userName + "': User does not exist.", e.getMessage() );
            }
        } );
    }

    @Test
    void alterPassword() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "ALTER CURRENT USER SET PASSWORD FROM 'old' TO 'new'" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "User '' failed to alter their own password: " + followerError, e.getMessage() );
            }
        } );

        // But it works on leader
        // Current user is '' so nothing will happen but we don't throw any exceptions
        leaderTx( ( sys, tx ) -> tx.execute( "ALTER CURRENT USER SET PASSWORD FROM 'old' TO 'new'" ) );
    }

    // Role commands tests

    @Test
    void showRoles() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW POPULATED ROLES" ).columnAs( "role" );
            assertEquals( PredefinedRoles.ADMIN, result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // Also works on leader
        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW POPULATED ROLES" ).columnAs( "role" );
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
                tx.execute( "CREATE ROLE " + roleName );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to create the specified role '" + roleName + "': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW ROLES" ).columnAs( "role" );
            assertEquals(
                    Set.of( PredefinedRoles.ADMIN, PredefinedRoles.ARCHITECT, PredefinedRoles.PUBLISHER,
                            PredefinedRoles.EDITOR, PredefinedRoles.READER ),
                    Set.of( result.next().toString(), result.next().toString(), result.next().toString(),
                            result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE ROLE " + roleName );

            var result = tx.execute( "SHOW ROLES" ).columnAs( "role" );
            assertEquals(
                    Set.of( PredefinedRoles.ADMIN, PredefinedRoles.ARCHITECT, PredefinedRoles.PUBLISHER,
                            PredefinedRoles.EDITOR, PredefinedRoles.READER, roleName ),
                    Set.of( result.next().toString(), result.next().toString(), result.next().toString(),
                            result.next().toString(), result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void createRoleAsCopyOfAnother() throws Exception
    {
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE ROLE " + roleName );

            var result = tx.execute( "SHOW ROLES" ).columnAs( "role" );
            assertEquals(
                    Set.of( PredefinedRoles.ADMIN, PredefinedRoles.ARCHITECT, PredefinedRoles.PUBLISHER,
                            PredefinedRoles.EDITOR, PredefinedRoles.READER, roleName ),
                    Set.of( result.next().toString(), result.next().toString(), result.next().toString(),
                            result.next().toString(), result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        followerTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "CREATE ROLE " + roleName2 + " AS COPY OF " + roleName );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                // First fail on checking that roleName exists
                assertEquals( "Failed to create a role as copy of '" + roleName + "': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW ROLES" ).columnAs( "role" );
            assertEquals(
                    Set.of( PredefinedRoles.ADMIN, PredefinedRoles.ARCHITECT, PredefinedRoles.PUBLISHER,
                            PredefinedRoles.EDITOR, PredefinedRoles.READER, roleName ),
                    Set.of( result.next().toString(), result.next().toString(), result.next().toString(),
                            result.next().toString(), result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE ROLE " + roleName2 + " AS COPY OF "+ roleName );

            var result = tx.execute( "SHOW ROLES" ).columnAs( "role" );
            assertEquals(
                    Set.of( PredefinedRoles.ADMIN, PredefinedRoles.ARCHITECT, PredefinedRoles.PUBLISHER,
                            PredefinedRoles.EDITOR, PredefinedRoles.READER, roleName, roleName2 ),
                    Set.of( result.next().toString(), result.next().toString(), result.next().toString(),
                            result.next().toString(), result.next().toString(), result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void createRoleIfNotExists() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "CREATE ROLE " + roleName + " IF NOT EXISTS" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to create the specified role '" + roleName + "': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW ROLES" ).columnAs( "role" );
            assertEquals(
                    Set.of( PredefinedRoles.ADMIN, PredefinedRoles.ARCHITECT, PredefinedRoles.PUBLISHER,
                            PredefinedRoles.EDITOR, PredefinedRoles.READER ),
                    Set.of( result.next().toString(), result.next().toString(), result.next().toString(),
                            result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE ROLE " + roleName + " IF NOT EXISTS" );

            var result = tx.execute( "SHOW ROLES" ).columnAs( "role" );
            assertEquals(
                    Set.of( PredefinedRoles.ADMIN, PredefinedRoles.ARCHITECT, PredefinedRoles.PUBLISHER,
                            PredefinedRoles.EDITOR, PredefinedRoles.READER, roleName ),
                    Set.of( result.next().toString(), result.next().toString(), result.next().toString(),
                            result.next().toString(), result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void createOrReplaceRole() throws Exception
    {
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE ROLE " + roleName );
            tx.execute( "CREATE USER " + userName + " SET PASSWORD '123'" );
            tx.execute( "GRANT ROLE " + roleName + " TO " + userName );

            var result = tx.execute( "SHOW POPULATED ROLES" ).columnAs( "role" );
            assertEquals( Set.of( PredefinedRoles.ADMIN, roleName ), Set.of( result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        followerTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "CREATE OR REPLACE ROLE " + roleName );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                // First fail on deleting the old user
                assertEquals( "Failed to delete the specified role '" + roleName + "': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW POPULATED ROLES" ).columnAs( "role" );
            assertEquals( Set.of( PredefinedRoles.ADMIN, roleName ), Set.of( result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            // replaces role
            tx.execute( "CREATE OR REPLACE ROLE " + roleName );

            var result = tx.execute( "SHOW POPULATED ROLES" ).columnAs( "role" );
            assertEquals( PredefinedRoles.ADMIN, result.next() );
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
                tx.execute( "DROP ROLE " + roleName );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to delete the specified role '" + roleName + "': " + followerError, e.getMessage() );
            }
        } );

        // But it works on leader (gives correct error for the command and setup)
        leaderTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "DROP ROLE " + roleName );
                fail( "Should have failed to drop non-existing role, but succeeded." );
            }
            catch ( QueryExecutionException e )
            {
                assertEquals( "Failed to delete the specified role '" + roleName + "': Role does not exist.", e.getMessage() );
            }
        } );
    }

    @Test
    void deleteRoleIfExists() throws Exception
    {
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE ROLE " + roleName );

            var result = tx.execute( "SHOW ROLES" ).columnAs( "role" );
            assertEquals(
                    Set.of( PredefinedRoles.ADMIN, PredefinedRoles.ARCHITECT, PredefinedRoles.PUBLISHER,
                            PredefinedRoles.EDITOR, PredefinedRoles.READER, roleName ),
                    Set.of( result.next().toString(), result.next().toString(), result.next().toString(),
                            result.next().toString(), result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        followerTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "DROP ROLE " + roleName + " IF EXISTS" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to delete the specified role '" + roleName + "': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW ROLES" ).columnAs( "role" );
            assertEquals(
                    Set.of( PredefinedRoles.ADMIN, PredefinedRoles.ARCHITECT, PredefinedRoles.PUBLISHER,
                            PredefinedRoles.EDITOR, PredefinedRoles.READER, roleName ),
                    Set.of( result.next().toString(), result.next().toString(), result.next().toString(),
                            result.next().toString(), result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "DROP ROLE " + roleName + " IF EXISTS" );

            var result = tx.execute( "SHOW ROLES" ).columnAs( "role" );
            assertEquals(
                    Set.of( PredefinedRoles.ADMIN, PredefinedRoles.ARCHITECT, PredefinedRoles.PUBLISHER,
                            PredefinedRoles.EDITOR, PredefinedRoles.READER ),
                    Set.of( result.next().toString(), result.next().toString(), result.next().toString(),
                            result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void grantRoleToUser() throws Exception
    {
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE USER " + userName + " SET PASSWORD '123'" );
            tx.execute( "CREATE ROLE " + roleName );
            tx.commit();
        } );

        followerTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "GRANT ROLE " + roleName + " TO " + userName );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to grant role '" + roleName + "' to user '" + userName + "': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW POPULATED ROLES" ).columnAs( "role" );
            assertEquals( PredefinedRoles.ADMIN, result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "GRANT ROLE " + roleName + " TO " + userName );

            var result = tx.execute( "SHOW POPULATED ROLES" ).columnAs( "role" );
            assertEquals( Set.of( PredefinedRoles.ADMIN, roleName ), Set.of( result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void revokeRoleFromUser() throws Exception
    {
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE USER " + userName + " SET PASSWORD '123'" );
            tx.execute( "CREATE ROLE " + roleName );
            tx.execute( "GRANT ROLE " + roleName + " TO " + userName );
            tx.commit();
        } );

        followerTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "REVOKE ROLE " + roleName + " FROM " + userName );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to revoke role '" + roleName + "' from user '" + userName + "': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW POPULATED ROLES" ).columnAs( "role" );
            assertEquals( Set.of( PredefinedRoles.ADMIN, roleName ), Set.of( result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "REVOKE ROLE " + roleName + " FROM " + userName );

            var result = tx.execute( "SHOW POPULATED ROLES" ).columnAs( "role" );
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
            var result = tx.execute( "SHOW ROLE " + PredefinedRoles.READER + " PRIVILEGES" ).columnAs( "role" );
            assertEquals( PredefinedRoles.READER, result.next() );
            result.close();
            tx.commit();
        } );

        // Also works on leader
        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW ROLE " + PredefinedRoles.READER + " PRIVILEGES" ).columnAs( "role" );
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
                tx.execute( "GRANT TRAVERSE ON GRAPH * TO " + roleName );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to grant traversal privilege to role '" + roleName + "': " + followerError, e.getMessage() );
            }
        } );

        // But it works on leader (gives correct error for the command and setup)
        leaderTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "GRANT TRAVERSE ON GRAPH * TO " + roleName );
                fail( "Should have failed to grant traverse to non-existing role, but succeeded." );
            }
            catch ( IllegalStateException | QueryExecutionException e )
            {
                assertEquals( "Failed to grant traversal privilege to role '" + roleName + "': Role '" + roleName + "' does not exist.", e.getMessage() );
            }
        } );
    }

    @Test
    void denyRead() throws Exception
    {
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE ROLE " + roleName );
            tx.commit();
        } );

        followerTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "DENY READ {prop} ON GRAPH * TO " + roleName );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to deny read privilege to role '" + roleName + "': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW ROLE " + roleName + " PRIVILEGES" ).columnAs( "role" );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "DENY READ {prop} ON GRAPH * NODES * TO " + roleName );

            var result = tx.execute( "SHOW ROLE " + roleName + " PRIVILEGES" ).columnAs( "role" );
            assertEquals( roleName, result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void revokeWrite() throws Exception
    {
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE ROLE " + roleName );
            tx.execute( "GRANT WRITE {*} ON GRAPH * TO " + roleName );
            tx.commit();
        } );

        followerTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "REVOKE WRITE {*} ON GRAPH * FROM " + roleName );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to revoke write privilege from role '" + roleName + "': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW ROLE " + roleName + " PRIVILEGES" ).columnAs( "grant" );
            assertTrue( result.hasNext() );
            result.close();
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "REVOKE WRITE {*} ON GRAPH * FROM " + roleName );

            var result = tx.execute( "SHOW ROLE " + roleName + " PRIVILEGES" ).columnAs( "grant" );
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
            var result = tx.execute( "SHOW DATABASES" ).columnAs( "name" );
            assertEquals( Set.of( DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME ), Set.of( result.next().toString(), result.next().toString() ) );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // Also works on leader
        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW DATABASES" ).columnAs( "name" );
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
            var result = tx.execute( "SHOW DEFAULT DATABASE" ).columnAs( "name" );
            assertEquals( DEFAULT_DATABASE_NAME, result.next().toString() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // Also works on leader
        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW DEFAULT DATABASE" ).columnAs( "name" );
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
            var result = tx.execute( "SHOW DATABASE " + DEFAULT_DATABASE_NAME ).columnAs( "name" );
            assertEquals( DEFAULT_DATABASE_NAME, result.next().toString() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // Also works on leader
        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW DATABASE " + DEFAULT_DATABASE_NAME ).columnAs( "name" );
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
                tx.execute( "CREATE DATABASE " + dbName );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to create the specified database '" + dbName + "': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW DATABASE " + dbName ).columnAs( "name" );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE DATABASE " + dbName );

            var result = tx.execute( "SHOW DATABASE " + dbName ).columnAs( "name" );
            assertTrue( result.hasNext() );
            result.close();
            tx.commit();
        } );
        CausalClusteringTestHelpers.assertDatabaseEventuallyStarted( dbName, cluster );
    }

    @Test
    void createDatabaseIfExists() throws Exception
    {
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE DATABASE " + dbName + " IF NOT EXISTS" );
            tx.commit();
        } );
        CausalClusteringTestHelpers.assertDatabaseEventuallyStarted( dbName, cluster );
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "STOP DATABASE " + dbName );

            var result = tx.execute( "SHOW DATABASE " + dbName ).columnAs( "status" );
            assertEquals( DatabaseStatus.Offline().stringValue(), result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        followerTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "CREATE DATABASE " + dbName + " IF NOT EXISTS" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to create the specified database '" + dbName + "': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW DATABASE " + dbName ).columnAs( "status" );
            assertEquals( DatabaseStatus.Offline().stringValue(), result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            // gives no error, does nothing
            tx.execute( "CREATE DATABASE " + dbName + " IF NOT EXISTS" );

            var result = tx.execute( "SHOW DATABASE " + dbName ).columnAs( "status" );
            assertEquals( DatabaseStatus.Offline().stringValue(), result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
    }

    @Test
    void createOrReplaceDatabase() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "CREATE OR REPLACE DATABASE " + dbName );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                // First fail on trying to delete the old user
                assertEquals( "Failed to delete the specified database '" + dbName + "': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW DATABASE " + dbName ).columnAs( "name" );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE OR REPLACE DATABASE " + dbName );

            var result = tx.execute( "SHOW DATABASE " + dbName ).columnAs( "name" );
            assertTrue( result.hasNext() );
            result.close();
            tx.commit();
        } );
        CausalClusteringTestHelpers.assertDatabaseEventuallyStarted( dbName, cluster );
    }

    @Test
    void deleteDatabase() throws Exception
    {
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE DATABASE " + dbName );
            tx.commit();
        } );
        CausalClusteringTestHelpers.assertDatabaseEventuallyStarted( dbName, cluster );

        followerTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "DROP DATABASE " + dbName );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to drop the specified database '" + dbName + "': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW DATABASE " + dbName ).columnAs( "name" );
            assertTrue( result.hasNext() );
            result.close();
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "DROP DATABASE " + dbName );

            var result = tx.execute( "SHOW DATABASE " + dbName ).columnAs( "name" );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
        CausalClusteringTestHelpers.assertDatabaseEventuallyDoesNotExist( dbName, cluster );
    }

    @Test
    void deleteDatabaseIfExists() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "DROP DATABASE " + dbName + " IF EXISTS" );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to delete the specified database '" + dbName + "': " + followerError, e.getMessage() );
            }
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            // gives no error, does nothing
            tx.execute( "DROP DATABASE " + dbName + " IF EXISTS" );
        } );
    }

    @Test
    void startDatabase() throws Exception
    {
        followerTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "START DATABASE " + dbName );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to start the specified database '" + dbName + "': " + followerError, e.getMessage() );
            }
        } );

        // But it works on leader (gives correct error for the command and setup)
        leaderTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "START DATABASE " + dbName );
                fail( "Should have failed to start non-existing database, but succeeded." );
            }
            catch ( DatabaseNotFoundException | QueryExecutionException e )
            {
                assertEquals( "Failed to start the specified database '" + dbName + "': Database does not exist.", e.getMessage() );
            }
        } );
    }

    @Test
    void stopDatabase() throws Exception
    {
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE DATABASE " + dbName );
            tx.commit();
        } );
        CausalClusteringTestHelpers.assertDatabaseEventuallyStarted( dbName, cluster );

        followerTx( ( sys, tx ) ->
        {
            try
            {
                tx.execute( "STOP DATABASE " + dbName );
                fail( "Should have failed to write on a FOLLOWER, but succeeded." );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( "Failed to stop the specified database '" + dbName + "': " + followerError, e.getMessage() );
            }
        } );

        leaderTx( ( sys, tx ) ->
        {
            var result = tx.execute( "SHOW DATABASE " + dbName ).columnAs( "status" );
            assertEquals( DatabaseStatus.Online().stringValue(), result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );

        // But it works on leader
        leaderTx( ( sys, tx ) ->
        {
            tx.execute( "STOP DATABASE " + dbName );

            var result = tx.execute( "SHOW DATABASE " + dbName ).columnAs( "status" );
            assertEquals( DatabaseStatus.Offline().stringValue(), result.next() );
            assertFalse( result.hasNext() );
            tx.commit();
        } );
        CausalClusteringTestHelpers.assertDatabaseEventuallyStopped( dbName, cluster );
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
