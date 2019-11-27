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
            () -> systemDbWork( List.of( "DROP USER alice" ), "Failed to delete the specified user 'alice': User does not exist." );

    private final Runnable createUserWithRoleWork =
            () -> systemDbWork( List.of( "CREATE USER alice SET PASSWORD 'secret' CHANGE NOT REQUIRED", "GRANT ROLE architect TO alice" ),
                    "Failed to create the specified user 'alice': User already exists." );

    private final Runnable deleteRoleWork =
            () -> systemDbWork( List.of( "DROP ROLE custom" ), "Failed to delete the specified role 'custom': Role does not exist." );

    private final Runnable createRoleWithAssignmentWork =
            () -> systemDbWork( List.of( "CREATE role custom AS COPY OF architect", "GRANT role custom TO alice" ),
                    "Failed to create the specified role 'custom': Role already exists." );

    @SuppressWarnings( "InfiniteLoopStatement" )
    private void systemDbWork( List<String> commands, String expectedErrorMessage )
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
                if ( !e.getMessage().equals( expectedErrorMessage ) )
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
