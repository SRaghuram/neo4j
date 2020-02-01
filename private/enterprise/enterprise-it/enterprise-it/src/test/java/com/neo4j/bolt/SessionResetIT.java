/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import com.neo4j.harness.junit.rule.EnterpriseNeo4jRule;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.nio.channels.ClosedChannelException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.graphdb.Result;
import org.neo4j.harness.junit.rule.Neo4jRule;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.VerboseTimeout;

import static com.neo4j.bolt.BoltDriverHelper.graphDatabaseDriver;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.function.Predicates.await;
import static org.neo4j.internal.helpers.NamedThreadFactory.daemon;
import static org.neo4j.internal.helpers.collection.Iterators.single;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

public class SessionResetIT
{
    private static final String SHORT_QUERY_1 = "CREATE (n:Node {name: 'foo', occupation: 'bar'})";
    private static final String SHORT_QUERY_2 = "MATCH (n:Node {name: 'foo'}) RETURN count(n)";
    private static final String LONG_QUERY = "UNWIND range(0, 10000000) AS i CREATE (n:Node {idx: i}) DELETE n";

    private static final int STRESS_IT_THREAD_COUNT = Runtime.getRuntime().availableProcessors() * 2;
    private static final long STRESS_IT_DURATION_MS = SECONDS.toMillis( 5 );
    private static final String[] STRESS_IT_QUERIES = {SHORT_QUERY_1, SHORT_QUERY_2, LONG_QUERY};

    private final VerboseTimeout timeout = VerboseTimeout.builder().withTimeout( 6, MINUTES ).build();
    private final Neo4jRule db = new EnterpriseNeo4jRule()
            .withConfig( GraphDatabaseSettings.load_csv_file_url_root, Path.of( "import" ) )
            .withConfig( OnlineBackupSettings.online_backup_enabled, false )
            .dumpLogsOnFailure( System.out );

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( timeout ).around( db );

    private Driver driver;

    @Before
    public void setUp()
    {
        driver = graphDatabaseDriver( db.boltURI() );
    }

    @After
    public void tearDown()
    {
        IOUtils.closeAllSilently( driver );
    }

    @Test
    public void shouldTerminateAutoCommitQuery() throws Exception
    {
        testQueryTermination( LONG_QUERY, true );
    }

    @Test
    public void shouldTerminateQueryInExplicitTransaction() throws Exception
    {
        testQueryTermination( LONG_QUERY, false );
    }

    @Test
    public void shouldTerminateAutoCommitQueriesRandomly() throws Exception
    {
        testRandomQueryTermination( true );
    }

    @Test
    public void shouldTerminateQueriesInExplicitTransactionsRandomly() throws Exception
    {
        testRandomQueryTermination( false );
    }

    private void testRandomQueryTermination( boolean autoCommit ) throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool( STRESS_IT_THREAD_COUNT, daemon( "test-worker" ) );
        Set<Session> runningSessions = ConcurrentHashMap.newKeySet();
        AtomicBoolean stop = new AtomicBoolean();
        List<Future<?>> futures = new ArrayList<>();

        for ( int i = 0; i < STRESS_IT_THREAD_COUNT; i++ )
        {
            futures.add( executor.submit( () ->
            {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                while ( !stop.get() )
                {
                    runRandomQuery( autoCommit, random, runningSessions, stop );
                }
            } ) );
        }

        long deadline = System.currentTimeMillis() + STRESS_IT_DURATION_MS;
        while ( !stop.get() )
        {
            if ( System.currentTimeMillis() > deadline )
            {
                stop.set( true );
            }

            resetAny( runningSessions );

            MILLISECONDS.sleep( 30 );
        }

        driver.close();
        awaitAll( futures );
        assertDatabaseIsIdle();
    }

    private void runRandomQuery( boolean autoCommit, Random random, Set<Session> runningSessions, AtomicBoolean stop )
    {
        try
        {
            Session session = driver.session();
            runningSessions.add( session );
            try
            {
                String query = STRESS_IT_QUERIES[random.nextInt( STRESS_IT_QUERIES.length - 1 )];
                runQuery( session, query, autoCommit );
            }
            finally
            {
                runningSessions.remove( session );
                session.close();
            }
        }
        catch ( Throwable error )
        {
            if ( !stop.get() && !isAcceptable( error ) )
            {
                stop.set( true );
                throw error;
            }
            // else it is fine to receive some errors from the driver because
            // sessions are being reset concurrently by the main thread, driver can also be closed concurrently
        }
    }

    private void testQueryTermination( String query, boolean autoCommit ) throws Exception
    {
        Future<Void> queryResult = runQueryInDifferentThreadAndResetSession( query, autoCommit );

        try
        {
            queryResult.get( 10, SECONDS );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ExecutionException.class ) );
            assertTrue( isTransactionTerminatedException( e.getCause() ) );
        }

        assertDatabaseIsIdle();
    }

    private Future<Void> runQueryInDifferentThreadAndResetSession( String query, boolean autoCommit ) throws Exception
    {
        AtomicReference<Session> sessionRef = new AtomicReference<>();

        Future<Void> queryResult = runAsync( () ->
        {
            try ( Session session = driver.session() )
            {
                sessionRef.set( session );
                runQuery( session, query, autoCommit );
            }
        } );

        await( () -> activeQueriesCount() == 1, 10, SECONDS );
        SECONDS.sleep( 1 ); // additionally wait a bit before resetting the session

        Session session = sessionRef.get();
        assertNotNull( session );
        session.reset();

        return queryResult;
    }

    private static void runQuery( Session session, String query, boolean autoCommit )
    {
        if ( autoCommit )
        {
            session.run( query ).consume();
        }
        else
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( query );
                tx.commit();
            }
        }
    }

    private void assertDatabaseIsIdle() throws InterruptedException
    {
        assertEventually( "Wrong number of active queries", this::activeQueriesCount, equalityCondition( 0L ), 10, SECONDS );
        assertEventually( "Wrong number of active transactions", this::activeTransactionsCount, equalityCondition( 0L ), 10, SECONDS );
    }

    private long activeQueriesCount()
    {
        var db = db();
        try ( var transaction = db.beginTx() )
        {
            try ( Result result = transaction.execute( "CALL dbms.listQueries() YIELD queryId RETURN count(queryId) AS result" ) )
            {
                return (long) single( result ).get( "result" ) - 1; // do not count listQueries procedure invocation
            }
        }
    }

    private long activeTransactionsCount()
    {
        DependencyResolver resolver = db().getDependencyResolver();
        KernelTransactions kernelTransactions = resolver.resolveDependency( KernelTransactions.class );
        return kernelTransactions.activeTransactions().size();
    }

    private GraphDatabaseAPI db()
    {
        return (GraphDatabaseAPI) db.defaultDatabaseService();
    }

    private static void resetAny( Set<Session> sessions )
    {
        sessions.stream().findAny().ifPresent( session ->
        {
            if ( sessions.remove( session ) )
            {
                resetSafely( session );
            }
        } );
    }

    private static void resetSafely( Session session )
    {
        try
        {
            if ( session.isOpen() )
            {
                session.reset();
            }
        }
        catch ( ClientException e )
        {
            if ( session.isOpen() )
            {
                throw e;
            }
            // else this thread lost race with close and it's fine
        }
    }

    private static boolean isAcceptable( Throwable error )
    {
        Throwable cause = getRootCause( error );

        return isTransactionTerminatedException( cause ) ||
               cause instanceof ServiceUnavailableException ||
               cause instanceof ClientException ||
               cause instanceof ClosedChannelException;
    }

    private static boolean isTransactionTerminatedException( Throwable error )
    {
        return error instanceof TransientException &&
               error.getMessage().startsWith( "The transaction has been terminated" );
    }

    private static void awaitAll( List<Future<?>> futures ) throws Exception
    {
        for ( Future<?> future : futures )
        {
            assertNull( future.get( 1, MINUTES ) );
        }
    }
}
