/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.utils.DriverUtils;
import com.neo4j.utils.ReactorDebugging;
import com.neo4j.utils.TestFabric;
import com.neo4j.utils.TestFabricFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.fabric.FabricDatabaseManager;
import org.neo4j.fabric.bolt.BoltFabricDatabaseManagementService;
import org.neo4j.fabric.bolt.FabricBookmark;
import org.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.bookmark.TransactionBookmarkManagerFactory;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.executor.FabricExecutor;
import org.neo4j.fabric.stream.Record;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.fabric.stream.summary.EmptySummary;
import org.neo4j.fabric.transaction.FabricTransaction;
import org.neo4j.fabric.transaction.TransactionManager;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class BoltAdapterTest
{

    private static FabricExecutor fabricExecutor = mock( FabricExecutor.class );
    private static TransactionManager transactionManager = mock( TransactionManager.class );
    private static TestFabric testFabric;
    private static Driver driver;
    private static FabricConfig fabricConfig;
    private static DriverUtils driverUtils;
    private final ResultPublisher publisher = new ResultPublisher();
    private final StatementResult statementResult = mock( StatementResult.class );
    private final CountDownLatch transactionLatch = new CountDownLatch( 1 );
    private final FabricTransaction fabricTransaction = mock( FabricTransaction.class );
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    @BeforeAll
    static void setUpServer() throws UnavailableException
    {
        fabricConfig = mock( FabricConfig.class );
        FabricDatabaseManager databaseManager = mock( FabricDatabaseManager.class );

        var databaseId = DatabaseIdFactory.from( "mega", UUID.randomUUID() );
        var graphDatabaseFacade = mock( GraphDatabaseFacade.class );
        when( graphDatabaseFacade.databaseId() ).thenReturn( databaseId );
        when( databaseManager.getDatabase( "mega" ) ).thenReturn( graphDatabaseFacade );

        var transactionIdTracker = mock( LocalGraphTransactionIdTracker.class);
        var databaseManagementService = new BoltFabricDatabaseManagementService( fabricExecutor, fabricConfig, transactionManager, databaseManager,
                transactionIdTracker, new TransactionBookmarkManagerFactory( databaseManager ) );

        testFabric = new TestFabricFactory()
                .addMocks( databaseManagementService, databaseManager )
                .build();

        driver = testFabric.directClientDriver();

        driverUtils = new DriverUtils( "mega" );
    }

    @BeforeEach
    void setUp()
    {
        reset( fabricExecutor, transactionManager, fabricTransaction, statementResult );

        publisher.reset();

        when( statementResult.columns() ).thenReturn( Flux.just( "c1", "c2" ) );
        when( statementResult.records() ).thenReturn( Flux.from( publisher ) );
        when( statementResult.summary() ).thenReturn( Mono.just( new EmptySummary() ) );
        when( statementResult.executionType() ).thenReturn( Mono.just( QueryExecutionType.query( QueryExecutionType.QueryType.READ_WRITE ) ) );

        when( fabricExecutor.run( any(), any(), any() ) ).thenReturn( statementResult );

        mockFabricTransaction();
    }

    @AfterEach
    void tearDown()
    {
        executorService.shutdown();
    }

    @AfterAll
    static void tearDownServer()
    {
        testFabric.close();
    }

    @Test
    void testSimpleStatementWithExplicitTransaction() throws InterruptedException
    {
        mockConfig();

        CountDownLatch latch = new CountDownLatch( 1 );
        executorService.submit( () -> driverUtils.doInTx( driver, tx ->
        {
            var result =  tx.run( "Some Cypher query" );
            verifyDefaultResult( result );

            latch.countDown();
        }) );

        publishDefaultResult();
        assertTrue( latch.await( 10, TimeUnit.SECONDS ) );

        waitForCommitOrRollback();
        verify( transactionManager ).begin( any(), any() );
        verify( fabricTransaction ).commit();
        verify( fabricTransaction, never() ).rollback();
    }

    @Test
    void testSimpleStatementWithImplicitTransaction() throws InterruptedException
    {
        mockConfig();

        CountDownLatch latch = new CountDownLatch( 1 );
        executorService.submit( () -> driverUtils.doInSession( driver, session ->
        {
            var result =  session.run( "Some Cypher query" );
            verifyDefaultResult( result );

            latch.countDown();
        }) );

        publishDefaultResult();
        assertTrue( latch.await( 10, TimeUnit.SECONDS ) );

        waitForCommitOrRollback();
        verify( transactionManager ).begin( any(), any() );
        verify( fabricTransaction ).commit();
        verify( fabricTransaction, never() ).rollback();
    }

    @Test
    void testRollback() throws InterruptedException
    {
        mockConfig();

        CountDownLatch latch = new CountDownLatch( 1 );
        executorService.submit( () -> driverUtils.doInTx( driver, tx ->
        {
            var result = tx.run( "Some Cypher query" );
            verifyDefaultResult( result );
            tx.rollback();
            latch.countDown();
        } ) );

        publishDefaultResult();
        assertTrue( latch.await( 10, TimeUnit.SECONDS ) );

        waitForCommitOrRollback();
        verify( transactionManager ).begin( any(), any() );
        verify( fabricTransaction, never() ).commit();
        verify( fabricTransaction ).rollback();
    }

    @Test
    void testErrorWhenExecutingStatementInExplicitTransaction() throws InterruptedException
    {
        mockConfig();

        when( fabricExecutor.run( any(), any(), any() ) ).thenThrow( new IllegalStateException( "Something went wrong" ) );

        CountDownLatch latch = new CountDownLatch( 1 );
        executorService.submit( () ->
        {
            var e = assertThrows( DatabaseException.class, () -> driverUtils.doInTx( driver, tx ->
            {
                var result = tx.run( "Some Cypher query" );
                verifyDefaultResult( result );
            } ) );

            assertThat( e.getMessage() ).contains( "Something went wrong" );

            latch.countDown();
        } );

        assertTrue( latch.await( 10, TimeUnit.SECONDS ) );

        waitForCommitOrRollback();
        verify( transactionManager ).begin( any(), any() );
        verify( fabricTransaction, never() ).commit();
        verify( fabricTransaction ).rollback();
    }

    @Test
    void testErrorWhenExecutingStatementInImplicitTransaction() throws InterruptedException
    {
        mockConfig();

        when( fabricExecutor.run( any(), any(), any() ) ).thenThrow( new IllegalStateException( "Something went wrong" ) );

        CountDownLatch latch = new CountDownLatch( 1 );
        executorService.submit( () ->
        {
            var e = assertThrows( DatabaseException.class, () -> driverUtils.doInSession( driver, session ->
            {
                var result = session.run( "Some Cypher query" );
                verifyDefaultResult( result );
            } ) );

            assertThat( e.getMessage() ).contains( "Something went wrong" );

            latch.countDown();
        } );

        assertTrue( latch.await( 10, TimeUnit.SECONDS ) );

        waitForCommitOrRollback();
        verify( transactionManager ).begin( any(), any() );
        verify( fabricTransaction, never() ).commit();
        verify( fabricTransaction ).rollback();
    }

    @Test
    void testErrorWhenStreamingResultInExplicitTransaction() throws InterruptedException
    {
        mockConfig();

        CountDownLatch latch = new CountDownLatch( 1 );
        executorService.submit( () ->
        {
            var e = assertThrows( DatabaseException.class, () -> driverUtils.doInTx( driver, tx ->
            {
                var result = tx.run( "Some Cypher query" );
                verifyDefaultResult( result );
            } ) );

            assertThat( e.getMessage() ).contains( "Something went wrong" );

            latch.countDown();
        } );

        assertTrue( publisher.subscriptionHappened.await( 10, TimeUnit.SECONDS ) );
        publisher.publishRecord( record( "v1", "v2" ) );
        publisher.publishRecord( record( "v3", "v4" ) );
        publisher.error( new IllegalStateException( "Something went wrong" ) );
        assertTrue( latch.await( 10, TimeUnit.SECONDS ) );

        waitForCommitOrRollback();
        verify( transactionManager ).begin( any(), any() );
        verify( fabricTransaction, never() ).commit();
        verify( fabricTransaction ).rollback();
    }

    @Test
    void testErrorWhenStreamingResultInImplicitTransaction() throws InterruptedException
    {
        mockConfig();

        CountDownLatch latch = new CountDownLatch( 1 );
        executorService.submit( () ->
        {
            var e = assertThrows( DatabaseException.class, () -> driverUtils.doInSession( driver, session ->
            {
                var result = session.run( "Some Cypher query" );
                verifyDefaultResult( result );
            } ) );

            assertThat( e.getMessage() ).contains( "Something went wrong" );

            latch.countDown();
        } );

        assertTrue( publisher.subscriptionHappened.await( 10, TimeUnit.SECONDS ) );
        publisher.publishRecord( record( "v1", "v2" ) );
        publisher.publishRecord( record( "v3", "v4" ) );
        publisher.error( new IllegalStateException( "Something went wrong" ) );
        assertTrue( latch.await( 10, TimeUnit.SECONDS ) );

        waitForCommitOrRollback();
        verify( transactionManager ).begin( any(), any() );
        verify( fabricTransaction, never() ).commit();
        verify( fabricTransaction ).rollback();
    }

    @Test
    void testCompletionWhenLastRecordRead() throws InterruptedException
    {
        mockConfig();

        RxSession session = null;
        try
        {
            var tx = Mono.from( session.beginTransaction() ).block();
            var result = tx.run( "Some Cypher query" );
            List<org.neo4j.driver.Record> records = new ArrayList<>();

            var onCompleteCalled = new CountDownLatch( 1 );
            Flux.from( result.records() )
                .subscribe( new Subscriber<>()
                {
                    @Override
                    public void onSubscribe( Subscription subscription )
                    {
                        subscription.request( 2 );
                    }

                    @Override
                    public void onNext( org.neo4j.driver.Record record )
                    {
                        records.add( record );
                    }

                    @Override
                    public void onError( Throwable throwable )
                    {
                        throwable.printStackTrace();
                    }

                    @Override
                    public void onComplete()
                    {
                        onCompleteCalled.countDown();
                    }
                } );

            publishDefaultResult();
            assertTrue( onCompleteCalled.await( 60, TimeUnit.SECONDS ) );
            verifyDefaultResult( records );
        }
        finally
        {
            Mono.from( session.close() ).block();
        }
    }

    private void mockConfig()
    {
        var streamConfig = new FabricConfig.DataStream( 1, 1000, 1000, 10 );
        when( fabricConfig.getDataStream() ).thenReturn( streamConfig );
    }

    private Record record( Object... values )
    {
        return new RecordImpl( Arrays.asList( values ) );
    }

    private void mockFabricTransaction()
    {
        when( transactionManager.begin( any(), any() ) ).thenReturn( fabricTransaction );

        doAnswer( invocationOnMock ->
        {
            transactionLatch.countDown();
            return null;
        } ).when( fabricTransaction ).commit();
        doAnswer( invocationOnMock ->
        {
            transactionLatch.countDown();
            return null;
        } ).when( fabricTransaction ).rollback();

        var bookmarkManager = mock( TransactionBookmarkManager.class );
        when( bookmarkManager.constructFinalBookmark() ).thenReturn( new FabricBookmark( List.of(), List.of() ) );
        when( fabricTransaction.getBookmarkManager() ).thenReturn( bookmarkManager );
    }

    private void waitForCommitOrRollback()
    {
        try
        {
            assertTrue( transactionLatch.await( 10, TimeUnit.SECONDS ) );
        }
        catch ( InterruptedException e )
        {
            fail( e );
        }
    }

    private void publishDefaultResult() throws InterruptedException
    {
        assertTrue( publisher.subscriptionHappened.await( 10, TimeUnit.SECONDS ) );
        publisher.publishRecord( record( "v1", "v2" ) );
        publisher.publishRecord( record( "v3", "v4" ) );
        publisher.complete();
    }

    private void verifyDefaultResult( Result result )
    {
        var records = result.list();
        verifyDefaultResult( records );
    }

    private void verifyDefaultResult( List<org.neo4j.driver.Record> records )
    {
        assertEquals( 2, records.size() );
        var r1 = records.get( 0 );
        assertEquals( "v1", r1.get( "c1" ).asString() );
        assertEquals( "v2", r1.get( "c2" ).asString() );
        var r2 = records.get( 1 );
        assertEquals( "v3", r2.get( "c1" ).asString() );
        assertEquals( "v4", r2.get( "c2" ).asString() );
    }

    private static class ResultPublisher implements Publisher<Record>
    {

        private CountDownLatch subscriptionHappened;
        private Subscriber<? super Record> subscriber;

        @Override
        public void subscribe( Subscriber<? super Record> subscriber )
        {
            this.subscriber = subscriber;
            subscriptionHappened.countDown();
            subscriber.onSubscribe( new Subscription()
            {
                @Override
                public void request( long l )
                {

                }

                @Override
                public void cancel()
                {

                }
            } );
        }

        void publishRecord( Record record )
        {
            subscriber.onNext( record );
        }

        void complete()
        {
            subscriber.onComplete();
        }

        void error( Exception e )
        {
            subscriber.onError( e );
        }

        void reset()
        {
            subscriptionHappened = new CountDownLatch( 1 );
            subscriber = null;
        }
    }

    private static class RecordImpl extends org.neo4j.fabric.stream.Record
    {

        private final List<Object> values;

        RecordImpl( List<Object> values )
        {
            this.values = values;
        }

        @Override
        public AnyValue getValue( int offset )
        {
            return Values.of( values.get( offset ) );
        }

        @Override
        public int size()
        {
            return values.size();
        }
    }
}
