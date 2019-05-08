/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.bolt;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.TransientException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.Connector.ConnectorType.BOLT;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.test.PortUtils.getBoltPort;

public class BoltThreadSchedulingIT
{
    private static final int TEST_TIMEOUT_SECONDS = 120;

    private final TestDirectory dir = TestDirectory.testDirectory();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( Timeout.seconds( TEST_TIMEOUT_SECONDS ) ).around( dir );

    private GraphDatabaseService db;
    private Driver driver;

    @After
    public void shutdownDb() throws InterruptedException
    {
        IOUtils.closeAllSilently( driver );
        Thread.sleep( 100 );
        if ( db != null )
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldFinishAllQueries() throws Throwable
    {
        // create server with limited thread pool threads.
        db = startDbWithBolt( new GraphDatabaseFactory(), 1, 2 );
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
                    tx.success();
                    tx.close();
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
        // The server will at least reject 2 requests due to missing threads handing incoming requests.
        assertTrue( errors.size() == 2 || errors.size() == 3 );
        for ( Throwable e : errors )
        {
            // Driver 1.7.4 will surface TransientException error (no thread available) properly.
            // Earlier version might get ServiceUnavailableException with closed channels due to a bug in error report.
            assertThat( e, anyOf( instanceOf( TransientException.class ), instanceOf( ServiceUnavailableException.class ) ) );
        }
    }

    private GraphDatabaseService startDbWithBolt( GraphDatabaseFactory dbFactory, int threadPoolMinSize, int threadPoolMaxSize )
    {
        return dbFactory.newEmbeddedDatabaseBuilder( dir.storeDir() )
                .setConfig( new BoltConnector( "bolt" ).type, BOLT.name() )
                .setConfig( new BoltConnector( "bolt" ).enabled, TRUE )
                .setConfig( new BoltConnector( "bolt" ).listen_address, "localhost:0" )
                .setConfig( new BoltConnector( "bolt" ).thread_pool_min_size, String.valueOf( threadPoolMinSize ) )
                .setConfig( new BoltConnector( "bolt" ).thread_pool_max_size, String.valueOf( threadPoolMaxSize ) )
                .setConfig( GraphDatabaseSettings.auth_enabled, FALSE )
                .setConfig( OnlineBackupSettings.online_backup_enabled, FALSE ).newGraphDatabase();
    }

    private static Driver createDriver( int port )
    {
        return GraphDatabase.driver( "bolt://localhost:" + port, Config.build().withoutEncryption().toConfig() );
    }
}
