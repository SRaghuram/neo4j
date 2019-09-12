/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.fabric.bolt.BoltFabricDatabaseManagementService;
import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.executor.FabricExecutor;
import com.neo4j.fabric.localdb.FabricDatabaseManager;
import com.neo4j.fabric.stream.Record;
import com.neo4j.fabric.stream.StatementResult;
import com.neo4j.fabric.transaction.FabricTransaction;
import com.neo4j.fabric.transaction.TransactionManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BoltAdapterTest
{

    private static FabricExecutor fabricExecutor = mock( FabricExecutor.class );
    private static TransactionManager transactionManager = mock( TransactionManager.class );
    private static TestServer testServer;
    private static Driver driver;
    private static FabricConfig fabricConfig;
    private final ResultPublisher publisher = new ResultPublisher();
    private final StatementResult statementResult = mock( StatementResult.class );
    private final CountDownLatch transactionLatch = new CountDownLatch( 1 );
    private final FabricTransaction fabricTransaction = mock( FabricTransaction.class );
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    @BeforeAll
    static void setUpServer()
    {
        PortUtils.Ports ports = PortUtils.findFreePorts();

        var configProperties = Map.of(
                "fabric.routing.servers", "localhost:" + ports.bolt,
                "dbms.connector.bolt.listen_address", "0.0.0.0:" + ports.bolt,
                "dbms.connector.bolt.enabled", "true"
        );

        var config = org.neo4j.configuration.Config.newBuilder()
                .set( configProperties )
                .build();

        testServer = new TestServer( config );

        fabricConfig = mock( FabricConfig.class );
        FabricDatabaseManager databaseManager = mock( FabricDatabaseManager.class );
        var databaseManagementService = new BoltFabricDatabaseManagementService( fabricExecutor, fabricConfig, transactionManager, databaseManager );
        testServer.addMocks( databaseManagementService, databaseManager );
        testServer.start();
        driver = GraphDatabase.driver( "bolt://localhost:" +  ports.bolt, AuthTokens.none(), Config.builder().withMaxConnectionPoolSize( 3 ).build() );
    }

    @BeforeEach
    void setUp()
    {
        reset( fabricExecutor, transactionManager, fabricTransaction );

        when( statementResult.columns() ).thenReturn( Flux.just( "c1", "c2" ) );
        when( statementResult.records() ).thenReturn( Flux.from( publisher ) );

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
        testServer.stop();
        driver.close();
    }

    @Test
    void testSimpleStatementWithExplicitTransaction() throws InterruptedException
    {
        mockConfig( 1, 1000, 1 );

        CountDownLatch latch = new CountDownLatch( 1 );
        executorService.submit( () ->
        {
            try ( Session session = driver.session() )
            {
                try ( Transaction transaction = session.beginTransaction() )
                {
                    var result = transaction.run( "Some Cypher query" );
                    verifyDefaultResult( result );
                    transaction.success();
                }
            }

            latch.countDown();
        } );

        publishDefaultResult();
        assertTrue( latch.await( 1, TimeUnit.SECONDS ) );

        waitForCommitOrRollback();
        verify( transactionManager ).begin( any() );
        verify( fabricTransaction ).commit();
        verify( fabricTransaction, never() ).rollback();
    }

    @Test
    void testSimpleStatementWithImplicitTransaction() throws InterruptedException
    {
        mockConfig( 1, 1000, 1 );

        CountDownLatch latch = new CountDownLatch( 1 );
        executorService.submit( () ->
        {
            try ( Session session = driver.session() )
            {
                var result = session.run( "Some Cypher query" );
                verifyDefaultResult( result );
            }

            latch.countDown();
        } );

        publishDefaultResult();
        assertTrue( latch.await( 5, TimeUnit.SECONDS ) );

        waitForCommitOrRollback();
        verify( transactionManager ).begin( any() );
        verify( fabricTransaction ).commit();
        verify( fabricTransaction, never() ).rollback();
    }

    @Test
    void testRollback() throws InterruptedException
    {
        mockConfig( 1, 1000, 1 );

        CountDownLatch latch = new CountDownLatch( 1 );
        executorService.submit( () ->
        {
            try ( Session session = driver.session() )
            {
                try ( Transaction transaction = session.beginTransaction() )
                {
                    var result = transaction.run( "Some Cypher query" );
                    verifyDefaultResult( result );
                    transaction.failure();
                }
            }

            latch.countDown();
        } );

        publishDefaultResult();
        assertTrue( latch.await( 5, TimeUnit.SECONDS ) );

        waitForCommitOrRollback();
        verify( transactionManager ).begin( any() );
        verify( fabricTransaction, never() ).commit();
        verify( fabricTransaction ).rollback();
    }

    @Test
    void testErrorWhenExecutingStatementInExplicitTransaction() throws InterruptedException
    {
        mockConfig( 1, 1000, 1 );

        when( fabricExecutor.run( any(), any(), any() ) ).thenThrow( new IllegalStateException( "Something went wrong" ) );

        CountDownLatch latch = new CountDownLatch( 1 );
        executorService.submit( () ->
        {
            try ( Session session = driver.session() )
            {
                try ( Transaction transaction = session.beginTransaction() )
                {
                    var result = transaction.run( "Some Cypher query" );
                    verifyDefaultResult( result );
                    fail( "exception expected" );
                }
            }
            catch ( DatabaseException e )
            {

                assertThat( e.getMessage(), containsString( "Something went wrong" ) );
            }

            latch.countDown();
        } );

        assertTrue( latch.await( 5, TimeUnit.SECONDS ) );

        waitForCommitOrRollback();
        verify( transactionManager ).begin( any() );
        verify( fabricTransaction, never() ).commit();
        verify( fabricTransaction ).rollback();
    }

    @Test
    void testErrorWhenExecutingStatementInImplicitTransaction() throws InterruptedException
    {
        mockConfig( 1, 1000, 1 );

        when( fabricExecutor.run( any(), any(), any() ) ).thenThrow( new IllegalStateException( "Something went wrong" ) );

        CountDownLatch latch = new CountDownLatch( 1 );
        executorService.submit( () ->
        {
            try ( Session session = driver.session() )
            {
                var result = session.run( "Some Cypher query" );
                verifyDefaultResult( result );
                fail( "exception expected" );
            }
            catch ( DatabaseException e )
            {

                assertThat( e.getMessage(), containsString( "Something went wrong" ) );
            }

            latch.countDown();
        } );

        assertTrue( latch.await( 5, TimeUnit.SECONDS ) );

        waitForCommitOrRollback();
        verify( transactionManager ).begin( any() );
        verify( fabricTransaction, never() ).commit();
        verify( fabricTransaction ).rollback();
    }

    @Test
    void testErrorWhenStreamingResultInExplicitTransaction() throws InterruptedException
    {
        mockConfig( 1, 1000, 1 );

        CountDownLatch latch = new CountDownLatch( 1 );
        executorService.submit( () ->
        {
            try ( Session session = driver.session() )
            {
                try ( Transaction transaction = session.beginTransaction() )
                {
                    var result = transaction.run( "Some Cypher query" );
                    verifyDefaultResult( result );
                    fail( "exception expected" );
                }
            }
            catch ( DatabaseException e )
            {

                assertThat( e.getMessage(), containsString( "Something went wrong" ) );
            }

            latch.countDown();
        } );

        assertTrue( publisher.latch.await( 5, TimeUnit.SECONDS ) );
        publisher.publishRecord( record( "v1", "v2" ) );
        publisher.publishRecord( record( "v3", "v4" ) );
        publisher.error( new IllegalStateException( "Something went wrong" ) );
        assertTrue( latch.await( 5, TimeUnit.SECONDS ) );

        waitForCommitOrRollback();
        verify( transactionManager ).begin( any() );
        verify( fabricTransaction, never() ).commit();
        verify( fabricTransaction ).rollback();
    }

    @Test
    void testErrorWhenStreamingResultInImplicitTransaction() throws InterruptedException
    {
        mockConfig( 1, 1000, 1 );

        CountDownLatch latch = new CountDownLatch( 1 );
        executorService.submit( () ->
        {
            try ( Session session = driver.session() )
            {
                var result = session.run( "Some Cypher query" );
                verifyDefaultResult( result );
                fail( "exception expected" );
            }
            catch ( DatabaseException e )
            {

                assertThat( e.getMessage(), containsString( "Something went wrong" ) );
            }

            latch.countDown();
        } );

        assertTrue( publisher.latch.await( 5, TimeUnit.SECONDS ) );
        publisher.publishRecord( record( "v1", "v2" ) );
        publisher.publishRecord( record( "v3", "v4" ) );
        publisher.error( new IllegalStateException( "Something went wrong" ) );
        assertTrue( latch.await( 5, TimeUnit.SECONDS ) );

        waitForCommitOrRollback();
        verify( transactionManager ).begin( any() );
        verify( fabricTransaction, never() ).commit();
        verify( fabricTransaction ).rollback();
    }

    private void mockConfig( int bufferLowWatermark, int bufferSize, int syncBatchSize )
    {
        var streamConfig = new FabricConfig.DataStream( bufferLowWatermark, bufferSize, syncBatchSize );
        when( fabricConfig.getDataStream() ).thenReturn( streamConfig );
    }

    private Record record( Object... values )
    {
        return new RecordImpl( Arrays.asList( values ) );
    }

    private void mockFabricTransaction()
    {
        when( transactionManager.begin( any() ) ).thenReturn( fabricTransaction );

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
    }

    private void waitForCommitOrRollback()
    {
        try
        {
            assertTrue( transactionLatch.await( 5, TimeUnit.SECONDS ) );
        }
        catch ( InterruptedException e )
        {
            fail( e );
        }
    }

    private void publishDefaultResult() throws InterruptedException
    {
        assertTrue( publisher.latch.await( 1, TimeUnit.SECONDS ) );
        publisher.publishRecord( record( "v1", "v2" ) );
        publisher.publishRecord( record( "v3", "v4" ) );
        publisher.complete();
    }

    private void verifyDefaultResult( org.neo4j.driver.StatementResult result )
    {
        var records = result.list();
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

        private final CountDownLatch latch = new CountDownLatch( 1 );
        private Subscriber<? super Record> subscriber;

        @Override
        public void subscribe( Subscriber<? super Record> subscriber )
        {
            this.subscriber = subscriber;
            latch.countDown();
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
    }

    private static class RecordImpl extends com.neo4j.fabric.stream.Record
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
