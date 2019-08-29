/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.SessionConfig;
import org.neo4j.harness.junit.rule.Neo4jRule;

import static com.neo4j.bolt.BoltDriverHelper.graphDatabaseDriver;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.driver.AuthTokens.basic;

public class DeleteUserStressIT
{
    @Rule
    public Neo4jRule db = new Neo4jRule()
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
            tx.success();
        }
        adminDriver.close();
        adminDriver = graphDatabaseDriver( db.boltURI(), basic( "neo4j", "abc" ) );
    }

    @Test
    public void shouldRun() throws InterruptedException
    {
        ExecutorService service = Executors.newFixedThreadPool( 3 );
        service.submit( createUserWork );
        service.submit( deleteUserWork );
        service.submit( transactionWork );

        service.awaitTermination( 30, TimeUnit.SECONDS );

        String msg = String.join( System.lineSeparator(),
                errors.stream().map( Throwable::getMessage ).collect( Collectors.toList() ) );
        assertThat( msg, errors, empty() );
    }

    @SuppressWarnings( "InfiniteLoopStatement" )
    private final Runnable transactionWork = () ->
    {

        for (; ; )
        {
            try ( Driver driver = graphDatabaseDriver( db.boltURI(), basic( "pontus", "sutnop" ) ) )
            {

                try ( Session session = driver.session();
                      Transaction tx = session.beginTransaction() )
                {
                    tx.run( "UNWIND range(1, 100000) AS n RETURN n" ).consume();
                    tx.success();
                }
            }
            catch ( ClientException e )
            {
                if ( !e.getMessage().equals( "The client is unauthorized due to authentication failure." ) )
                {
                    errors.add( e );
                }
            }
        }
    };

    @SuppressWarnings( "InfiniteLoopStatement" )
    private final Runnable deleteUserWork = () ->
    {

        for (; ; )
        {
            try ( Session session = adminDriver.session( SessionConfig.forDatabase( SYSTEM_DATABASE_NAME ) );
                  Transaction tx = session.beginTransaction() )
            {
                tx.run( "DROP USER pontus" ).consume();
                tx.success();
            }
            catch ( ClientException e )
            {
                if ( !e.getMessage().equals( "Failed to delete the specified user 'pontus': User does not exist." ) )
                {
                    errors.add( e );
                }
            }
        }
    };

    @SuppressWarnings( "InfiniteLoopStatement" )
    private final Runnable createUserWork = () ->
    {
        for (; ; )
        {
            try ( Session session = adminDriver.session( SessionConfig.forDatabase( SYSTEM_DATABASE_NAME ));
                  Transaction tx = session.beginTransaction() )
            {
                tx.run( "CREATE USER pontus SET PASSWORD 'sutnop' CHANGE NOT REQUIRED" ).consume();
                tx.success();
            }
            catch ( ClientException e )
            {
                if ( !e.getMessage().equals( "Failed to create the specified user 'pontus': User already exists." ) )
                {
                    errors.add( e );
                }
            }
        }
    };
}
