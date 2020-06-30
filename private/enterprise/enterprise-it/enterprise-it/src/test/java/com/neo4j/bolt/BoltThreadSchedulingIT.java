/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bolt.BoltDriverHelper.graphDatabaseDriver;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.IOUtils.closeAllSilently;
import static org.neo4j.test.PortUtils.getBoltPort;

@TestDirectoryExtension
class BoltThreadSchedulingIT
{
    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseService db;
    private Driver driver;
    private DatabaseManagementService managementService;

    @AfterEach
    void shutdownDb() throws InterruptedException
    {
        closeAllSilently( driver );
        Thread.sleep( 100 );
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void shouldFinishAllQueries() throws Throwable
    {
        // create server with limited thread pool size and rejecting jobs immediately if no thread available.
        db = startDbWithBolt( 1, 1, 0 );
        driver = createDriver( getBoltPort( db ) );

        // submits some jobs to executor, shooting at server at the same time.
        ExecutorService executorService = Executors.newFixedThreadPool( 4 );
        int count = 4;
        CountDownLatch latch = new CountDownLatch( count );
        Future<?>[] futures = new Future<?>[count];

        for ( int i = 0; i < count; i++ )
        {
            Future<?> future = executorService.submit( () -> {
                try ( Session session = driver.session() )
                {
                    Transaction tx;
                    // try to begin tx and run a query
                    try
                    {
                        tx = session.beginTransaction();
                        tx.run( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" );
                    }
                    finally
                    {
                        // regardless we success or not
                        latch.countDown();
                    }

                    try
                    {
                        // I only commit my tx when I have started all transactions.
                        latch.await();
                    }
                    catch ( InterruptedException e )
                    {
                        throw new RuntimeException( e );
                    }
                    tx.commit();
                }
            } );
            futures[i] = future;
        }

        executorService.shutdown();
        executorService.awaitTermination( 10, TimeUnit.SECONDS );

        List<Throwable> errors = new ArrayList<>();
        for ( Future<?> f : futures )
        {
            try
            {
                f.get();
            }
            catch ( ExecutionException e )
            {
                errors.add( e.getCause() );
            }
        }
        // The server should reject a few requests due to missing threads handing incoming requests.
        assertTrue( errors.size() > 0 );
        for ( Throwable e : errors )
        {
            assertThat( e, anyOf( instanceOf( TransientException.class ) ) );
        }
    }

    private GraphDatabaseService startDbWithBolt( int threadPoolMinSize, int threadPoolMaxSize,
            int threadPoolQueueSize )
    {
        DatabaseManagementServiceBuilder dbFactory = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() );
        managementService = dbFactory
                .setConfig( BoltConnector.enabled, true )
                .setConfig( BoltConnector.listen_address, new SocketAddress( "localhost", 0 ) )
                .setConfig( BoltConnectorInternalSettings.unsupported_thread_pool_queue_size, threadPoolQueueSize )
                .setConfig( BoltConnector.thread_pool_min_size, threadPoolMinSize )
                .setConfig( BoltConnector.thread_pool_max_size, threadPoolMaxSize )
                .setConfig( GraphDatabaseSettings.auth_enabled, false )
                .setConfig( OnlineBackupSettings.online_backup_enabled, false ).build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private static Driver createDriver( int port )
    {
        return graphDatabaseDriver( "bolt://localhost:" + port );
    }
}
