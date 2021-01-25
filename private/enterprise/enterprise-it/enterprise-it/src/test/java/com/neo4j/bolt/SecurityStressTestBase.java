/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.harness.junit.rule.Neo4jRule;

import static com.neo4j.bolt.BoltDriverHelper.graphDatabaseDriver;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.driver.AuthTokens.basic;

abstract class SecurityStressTestBase
{
    Driver adminDriver;
    private final Set<Throwable> errors = ConcurrentHashMap.newKeySet();

    // Expected if the user is dropped before or during authentication
    String authenticationError = "The client is unauthorized due to authentication failure.";

    void setupAdminDriver( Neo4jRule db )
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

    void assertNoUnexpectedErrors()
    {
        String msg = errors.stream().map( Throwable::getMessage ).collect( Collectors.joining( System.lineSeparator() ) );
        assertThat( errors ).as( msg ).isEmpty();
    }

    final Runnable createUserWork =
            () -> systemDbWork( List.of( "CREATE USER alice SET PASSWORD 'secret' CHANGE NOT REQUIRED" ),
                    Set.of( "Failed to create the specified user 'alice': User already exists." ) );

    final Runnable deleteUserWork =
            () -> systemDbWork( List.of( "DROP USER alice" ), Set.of( "Failed to delete the specified user 'alice': User does not exist." ) );

    @SuppressWarnings( "InfiniteLoopStatement" )
    void systemDbWork( List<String> commands, Set<String> expectedErrorMessages )
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

    @SuppressWarnings( "InfiniteLoopStatement" )
    void defaultDbWork( Neo4jRule db, Set<String> expectedErrorMessages )
    {
        for (; ; )
        {
            try ( Driver driver = graphDatabaseDriver( db.boltURI(), basic( "alice", "secret" ) ) )
            {

                try ( Session session = driver.session();
                        Transaction tx = session.beginTransaction() )
                {
                    tx.run( "UNWIND range(1, 100000) AS n RETURN n" ).consume();
                    tx.commit();
                }
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
}
