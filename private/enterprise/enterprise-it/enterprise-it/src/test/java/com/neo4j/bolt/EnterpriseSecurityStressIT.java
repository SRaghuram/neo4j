/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import com.neo4j.harness.junit.rule.EnterpriseNeo4jRule;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.harness.junit.rule.Neo4jRule;

import static com.neo4j.bolt.BoltDriverHelper.graphDatabaseDriver;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.driver.AuthTokens.basic;

public class EnterpriseSecurityStressIT
{
    @Rule
    public Neo4jRule db = new EnterpriseNeo4jRule()
            .withConfig( GraphDatabaseSettings.auth_enabled, true )
            .withConfig( OnlineBackupSettings.online_backup_enabled, false );

    private Driver adminDriver;
    private final Set<Throwable> errors = ConcurrentHashMap.newKeySet();

    @Before
    public void setup()
    {
        adminDriver = graphDatabaseDriver( db.boltURI(), basic( "neo4j", "neo4j" ) );
        try ( Session session = adminDriver.session( SessionConfig.forDatabase( SYSTEM_DATABASE_NAME ) );
              Transaction tx = session.beginTransaction() )
        {
            tx.run( "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO 'abc'" ).consume();
            tx.commit();
        }
        adminDriver.close();
        adminDriver = graphDatabaseDriver( db.boltURI(), basic( "neo4j", "abc" ) );
    }

    // Concurrency tests for authentication

    @Test
    public void shouldHandleConcurrentAuthAndDropUser() throws InterruptedException
    {
        ExecutorService service = Executors.newFixedThreadPool( 7 );

        // Run several transaction threads to increase the risk of the user
        // being dropped during user management in authorization.
        service.submit( createUserWithRoleWork );
        service.submit( deleteUserWork );
        service.submit( transactionWork );
        service.submit( transactionWork );
        service.submit( transactionWork );
        service.submit( transactionWork );
        service.submit( transactionWork );

        service.awaitTermination( 30, TimeUnit.SECONDS );
        assertNoUnexpectedErrors();
    }

    @Test
    public void shouldHandleConcurrentAuthAndDropRole() throws InterruptedException
    {
        try ( Session session = adminDriver.session( SessionConfig.forDatabase( SYSTEM_DATABASE_NAME ));
                Transaction tx = session.beginTransaction() )
        {
            tx.run( "CREATE USER alice SET PASSWORD 'secret' CHANGE NOT REQUIRED" ).consume();
            tx.commit();
        }

        ExecutorService service = Executors.newFixedThreadPool( 3 );

        service.submit( createRoleWithAssignmentWork );
        service.submit( deleteRoleWork );
        service.submit( transactionWork );

        service.awaitTermination( 10, TimeUnit.SECONDS );
        assertNoUnexpectedErrors();
    }

    @Test
    public void shouldHandleConcurrentAuthAndChangePrivileges() throws InterruptedException
    {
        try ( Session session = adminDriver.session( SessionConfig.forDatabase( SYSTEM_DATABASE_NAME ));
                Transaction tx = session.beginTransaction() )
        {
            tx.run( "CREATE USER alice SET PASSWORD 'secret' CHANGE NOT REQUIRED" ).consume();
            tx.run( "CREATE ROLE custom AS COPY OF architect" ).consume();
            tx.run( "GRANT ROLE custom TO alice" ).consume();
            tx.commit();
        }

        ExecutorService service = Executors.newFixedThreadPool( 3 );

        service.submit( assignPrivilegeWork );
        service.submit( revokePrivilegeWork );
        service.submit( transactionWork );

        service.awaitTermination( 30, TimeUnit.SECONDS );
        assertNoUnexpectedErrors();
    }

    // Concurrency tests for altering users and roles

    @Test
    public void shouldHandleConcurrentAlterAndDropUser() throws InterruptedException
    {
        ExecutorService service = Executors.newFixedThreadPool( 3 );

        service.submit( createUserWork );
        service.submit( deleteUserWork );
        service.submit( alterUserWork );

        service.awaitTermination( 10, TimeUnit.SECONDS );
        assertNoUnexpectedErrors();
    }

    @Test
    public void shouldHandleConcurrentGrantAndRevokeRoleAndDrop() throws InterruptedException
    {
        ExecutorService service = Executors.newFixedThreadPool( 6 );

        service.submit( createUserWork );
        service.submit( deleteUserWork );
        service.submit( createRoleWork );
        service.submit( deleteRoleWork );
        service.submit( grantRoleWork );
        service.submit( revokeRoleWork );

        service.awaitTermination( 10, TimeUnit.SECONDS );
        assertNoUnexpectedErrors();
    }

    @Test
    public void shouldHandleConcurrentCreateCopyOfAndDropRole() throws InterruptedException
    {
        ExecutorService service = Executors.newFixedThreadPool( 4 );

        service.submit( createRoleWork );
        service.submit( deleteRoleWork );
        service.submit( createCopyRoleWork );
        service.submit( deleteCopyRoleWork );

        service.awaitTermination( 10, TimeUnit.SECONDS );
        assertNoUnexpectedErrors();
    }

    @Test
    public void shouldHandleConcurrentPrivilegeChangesAndDropRole() throws InterruptedException
    {
        ExecutorService service = Executors.newFixedThreadPool( 4 );

        service.submit( createRoleWork );
        service.submit( deleteRoleWork );
        service.submit( assignPrivilegeWork );
        service.submit( revokePrivilegeWork );

        service.awaitTermination( 10, TimeUnit.SECONDS );
        assertNoUnexpectedErrors();
    }

    // Concurrency tests for SHOW commands

    @Test
    public void shouldHandleConcurrentShowAndDropUser() throws InterruptedException
    {
        ExecutorService service = Executors.newFixedThreadPool( 3 );

        service.submit( createUserWork );
        service.submit( deleteUserWork );
        service.submit( showUserWork );

        service.awaitTermination( 10, TimeUnit.SECONDS );
        assertNoUnexpectedErrors();
    }

    @Test
    public void shouldHandleConcurrentShowRolesAndDrop() throws InterruptedException
    {
        ExecutorService service = Executors.newFixedThreadPool( 5 );

        service.submit( createUserWork );
        service.submit( deleteUserWork );
        service.submit( createRoleWithAssignmentWork );
        service.submit( deleteRoleWork );
        service.submit( showRolesWork );

        service.awaitTermination( 10, TimeUnit.SECONDS );
        assertNoUnexpectedErrors();
    }

    @Test
    public void shouldHandleConcurrentShowPrivilegesForUserAndDrop() throws InterruptedException
    {
        ExecutorService service = Executors.newFixedThreadPool( 5 );

        service.submit( createUserWork );
        service.submit( deleteUserWork );
        service.submit( createRoleWithAssignmentWork );
        service.submit( deleteRoleWork );
        service.submit( showPrivilegesForUserWork );

        service.awaitTermination( 10, TimeUnit.SECONDS );
        assertNoUnexpectedErrors();
    }

    @Test
    public void shouldHandleConcurrentShowPrivilegesForRoleAndDropRole() throws InterruptedException
    {
        ExecutorService service = Executors.newFixedThreadPool( 3 );

        service.submit( createRoleWork );
        service.submit( deleteRoleWork );
        service.submit( showPrivilegesForRoleWork );

        service.awaitTermination( 10, TimeUnit.SECONDS );
        assertNoUnexpectedErrors();
    }

    @SuppressWarnings( "InfiniteLoopStatement" )
    private final Runnable transactionWork = () ->
    {
        // Expected if the user is dropped before or during authentication
        String authenticationError = "The client is unauthorized due to authentication failure.";

        // Expected if the user is dropped before or during authorization
        String userAuthorizationError = "Database access is not allowed for user 'alice' with roles [].";

        // Expected if the role is dropped during authorization privilege lookup.
        String roleAuthorizationError = "Database access is not allowed for user 'alice' with roles [custom].";

        for (; ; )
        {
            try ( Driver driver = graphDatabaseDriver( db.boltURI(), basic( "alice", "secret" ) ) )
            {

                try ( Session session = driver.session();
                      Transaction tx = session.beginTransaction() )
                {
                    tx.run( "UNWIND range(1, 10) AS n RETURN n" ).consume();
                    tx.commit();
                }
            }
            catch ( ClientException e )
            {
                if ( !(e.getMessage().equals( authenticationError ) || e.getMessage().equals( userAuthorizationError ) ||
                        e.getMessage().equals( roleAuthorizationError )) )
                {
                    errors.add( e );
                }
            }
        }
    };

    private final Runnable deleteUserWork =
            () -> systemDbWork( List.of( "DROP USER alice" ), Set.of( "Failed to delete the specified user 'alice': User does not exist." ) );

    private final Runnable createUserWork =
            () -> systemDbWork( List.of( "CREATE USER alice SET PASSWORD 'secret' CHANGE NOT REQUIRED" ),
                    Set.of( "Failed to create the specified user 'alice': User already exists." ) );

    private final Runnable createUserWithRoleWork =
            () -> systemDbWork( List.of( "CREATE USER alice SET PASSWORD 'secret' CHANGE NOT REQUIRED", "GRANT ROLE architect TO alice" ),
                    Set.of( "Failed to create the specified user 'alice': User already exists." ) );

    private final Runnable alterUserWork =
            () -> systemDbWork( List.of( "ALTER USER alice SET STATUS suspended" ),
                    Set.of( "Failed to alter the specified user 'alice': User does not exist." ) );

    private final Runnable deleteRoleWork =
            () -> systemDbWork( List.of( "DROP ROLE custom" ), Set.of( "Failed to delete the specified role 'custom': Role does not exist." ) );

    private final Runnable deleteCopyRoleWork =
            () -> systemDbWork( List.of( "DROP ROLE customCopy" ),
                    Set.of( "Failed to delete the specified role 'customCopy': Role does not exist." ) );

    private final Runnable createRoleWork =
            () -> systemDbWork( List.of( "CREATE role custom AS COPY OF architect" ),
                    Set.of( "Failed to create the specified role 'custom': Role already exists." ) );

    private final Runnable createCopyRoleWork =
            () -> systemDbWork( List.of( "CREATE role customCopy AS COPY OF custom" ),
                    Set.of( "Failed to create the specified role 'customCopy': Role already exists.",
                            "Failed to create a role as copy of 'custom': Role does not exist." ) );

    private final Runnable createRoleWithAssignmentWork =
            () -> systemDbWork( List.of( "CREATE role custom AS COPY OF architect", "GRANT role custom TO alice" ),
                    Set.of( "Failed to create the specified role 'custom': Role already exists.",
                            "Failed to grant role 'custom' to user 'alice': User does not exist." ) );

    private final Runnable grantRoleWork =
            () -> systemDbWork( List.of( "GRANT role custom TO alice" ),
                    Set.of( "Failed to grant role 'custom' to user 'alice': Role does not exist.",
                            "Failed to grant role 'custom' to user 'alice': User does not exist." ) );

    private final Runnable assignPrivilegeWork =
            () -> systemDbWork( List.of( "GRANT TRAVERSE ON GRAPH * NODES A TO custom", "DENY READ {prop} ON GRAPH * NODES * TO custom" ),
                    Set.of( "Failed to grant traversal privilege to role 'custom': Role 'custom' does not exist.",
                            "Failed to deny read privilege to role 'custom': Role 'custom' does not exist.") );

    private final Runnable revokePrivilegeWork =
            () -> systemDbWork( List.of( "REVOKE TRAVERSE ON GRAPH * NODES C FROM custom", "REVOKE DENY READ {prop} ON GRAPH * NODES * FROM custom" ),
                    Set.of() );

    private final Runnable revokeRoleWork = () -> systemDbWork( List.of( "REVOKE role custom FROM alice" ), Set.of() );

    private final Runnable showUserWork = () -> systemDbWork( List.of( "SHOW USERS" ), Set.of() );
    private final Runnable showRolesWork = () -> systemDbWork( List.of( "SHOW ROLES WITH USERS" ), Set.of() );
    private final Runnable showPrivilegesForUserWork = () -> systemDbWork( List.of( "SHOW USER alice PRIVILEGES" ), Set.of() );
    private final Runnable showPrivilegesForRoleWork = () -> systemDbWork( List.of( "SHOW ROLE custom PRIVILEGES" ), Set.of() );

    @SuppressWarnings( "InfiniteLoopStatement" )
    private void systemDbWork( List<String> commands, Set<String> expectedErrorMessages )
    {
        for (; ; )
        {
            try ( Session session = adminDriver.session( SessionConfig.forDatabase( SYSTEM_DATABASE_NAME ) );
                    Transaction tx = session.beginTransaction() )
            {
                for ( String command: commands )
                {
                    tx.run( command ).consume();
                }
                tx.commit();
            }
            catch ( ClientException e )
            {
                if ( !expectedErrorMessages.contains( e.getMessage() ) )
                {
                    errors.add( e );
                }
            }
        }
    }

    private void assertNoUnexpectedErrors()
    {
        String msg = errors.stream().map( Throwable::getMessage ).collect( Collectors.joining( System.lineSeparator() ) );
        assertThat( msg, errors, empty() );
    }
}
