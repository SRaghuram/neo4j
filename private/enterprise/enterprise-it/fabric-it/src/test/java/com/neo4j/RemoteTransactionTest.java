/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.driver.AutoCommitStatementResult;
import com.neo4j.fabric.driver.DriverPool;
import com.neo4j.fabric.driver.FabricDriverTransaction;
import com.neo4j.fabric.driver.PooledDriver;
import com.neo4j.fabric.localdb.FabricDatabaseManager;
import com.neo4j.fabric.stream.StatementResult;
import com.neo4j.fabric.stream.summary.EmptySummary;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.internal.SessionConfig;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RemoteTransactionTest
{
    private static Driver clientDriver;
    private static TestServer testServer;
    private static final DriverPool driverPool = mock( DriverPool.class );
    private static final PooledDriver shard1Driver = mock( PooledDriver.class );
    private static final PooledDriver shard2Driver = mock( PooledDriver.class );
    private static final PooledDriver shard3Driver = mock( PooledDriver.class );
    private static final JobScheduler jobScheduler = mock( JobScheduler.class );
    private static final DatabaseManagementService databaseManagementService = mock( DatabaseManagementService.class );
    private static final FabricDatabaseManager fabricDatabaseManager = mock( FabricDatabaseManager.class );

    private final CountDownLatch latch = new CountDownLatch( 3 );
    private final FabricDriverTransaction tx1 = mockTransactionWithDefaultResult();
    private final FabricDriverTransaction tx2 = mockTransactionWithDefaultResult();
    private final FabricDriverTransaction tx3 = mockTransactionWithDefaultResult();

    @BeforeAll
    static void beforeAll() throws UnavailableException
    {
        FabricConfig.Graph graph1 = new FabricConfig.Graph( 1, URI.create( "bolt://somewhere:1001" ), null, null, emptyDriverConfig() );
        FabricConfig.Graph graph2 = new FabricConfig.Graph( 2, URI.create( "bolt://somewhere:1002" ), null, null, emptyDriverConfig() );
        FabricConfig.Graph graph3 = new FabricConfig.Graph( 3, URI.create( "bolt://somewhere:1003" ), null, null, emptyDriverConfig() );

        var ports = PortUtils.findFreePorts();
        var configProperties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.1.uri", graph1.getUri().toString(),
                "fabric.graph.2.uri", graph2.getUri().toString(),
                "fabric.graph.3.uri", graph3.getUri().toString(),
                "fabric.routing.servers", "localhost:" + ports.bolt,
                "dbms.transaction.timeout", "1000",
                "dbms.connector.bolt.listen_address", "0.0.0.0:" + ports.bolt,
                "dbms.connector.bolt.enabled", "true"
        );
        var config = Config.newBuilder().setRaw( configProperties ).build();

        testServer = new TestServer( config );
        testServer.addMocks( driverPool, jobScheduler, databaseManagementService, fabricDatabaseManager );

        JobHandle timeoutHandle = mock( JobHandle.class );
        when( jobScheduler.schedule( any(), any(), anyLong(), any() ) ).thenReturn( timeoutHandle );

        testServer.start();

        mockFabricDatabaseManager();

        clientDriver = GraphDatabase.driver( "bolt://localhost:" + ports.bolt, AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                        .withMaxConnectionPoolSize( 3 )
                        .withoutEncryption().build() );

        mockDriverPool( graph1, shard1Driver );
        mockDriverPool( graph2, shard2Driver );
        mockDriverPool( graph3, shard3Driver );
    }

    @AfterAll
    static void afterAll()
    {
        testServer.stop();
        clientDriver.close();
    }

    @BeforeEach
    void beforeEach()
    {
        mockShardDriver( shard1Driver, Mono.just( tx1 ) );
        mockShardDriver( shard2Driver, Mono.just( tx2 ) );
        mockShardDriver( shard3Driver, Mono.just( tx3 ) );
    }

    private static void mockDriverPool( FabricConfig.Graph graph, PooledDriver pooledDriver )
    {
        when( driverPool.getDriver( eq( graph ), any() ) ).thenReturn( pooledDriver );
    }

    private static void mockFabricDatabaseManager() throws UnavailableException
    {
        GraphDatabaseFacade graphDatabaseFacade = mock( GraphDatabaseFacade.class );
        when( databaseManagementService.database( any() ) ).thenReturn( graphDatabaseFacade );

        DatabaseId databaseId = mock( DatabaseId.class );
        when( databaseId.name() ).thenReturn( "mega" );

        when( graphDatabaseFacade.databaseId() ).thenReturn( databaseId );

        when( fabricDatabaseManager.getDatabase( any() ) ).thenReturn( graphDatabaseFacade );
        when( fabricDatabaseManager.isFabricDatabase( "mega" ) ).thenReturn( true );

        DatabaseIdRepository idRepository = mock( DatabaseIdRepository.class );
        when( idRepository.getByName( "mega" ) ).thenReturn( Optional.of( databaseId ) );
        when( fabricDatabaseManager.databaseIdRepository() ).thenReturn( idRepository );

        InternalTransaction internalTransaction = mock( InternalTransaction.class );
        when( graphDatabaseFacade.beginTransaction( any(), any(), any(), anyLong(), any() ) ).thenReturn( internalTransaction );

        DependencyResolver dr = mock( DependencyResolver.class );
        when( graphDatabaseFacade.getDependencyResolver() ).thenReturn( dr );

        GraphDatabaseQueryService graphDatabaseQueryService = mock( GraphDatabaseQueryService.class );
        when( dr.resolveDependency( GraphDatabaseQueryService.class ) ).thenReturn( graphDatabaseQueryService );

        when( graphDatabaseQueryService.getDependencyResolver() ).thenReturn( dr );
    }

    private void mockShardDriver( PooledDriver shardDriver, Mono<FabricDriverTransaction> transaction )
    {
        reset( shardDriver );
        when( shardDriver.beginTransaction( any(), any(), any(), any() ) ).thenReturn( transaction );

        var result = mock( AutoCommitStatementResult.class );
        when( result.columns() ).thenReturn( Flux.fromIterable( List.of( "a", "b" ) ) );
        when( result.records() ).thenReturn( Flux.empty() );
        when( result.summary() ).thenReturn( Mono.just( new EmptySummary() ) );

        when( shardDriver.run( any(), any(), any(), any(), any(), any() ) ).thenReturn( result );

        doAnswer( invocationOnMock ->
        {
            latch.countDown();
            return Mono.empty();
        } ).when( shardDriver ).release();
    }

    private FabricDriverTransaction mockTransactionWithDefaultResult()
    {
        var tx = mock( FabricDriverTransaction.class );
        var result = mock( StatementResult.class );
        when( result.columns() ).thenReturn( Flux.fromIterable( List.of( "a", "b" ) ) );
        when( result.records() ).thenReturn( Flux.empty() );
        when( result.summary() ).thenReturn( Mono.just( new EmptySummary() ) );

        when( tx.run( any(), any() ) ).thenReturn( result );

        when( tx.commit() ).thenReturn( Mono.empty() );
        when( tx.rollback() ).thenReturn( Mono.empty() );

        return tx;
    }

    @Test
    void testCommit()
    {
        try ( var session = openSession() )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                writeToShard1( tx );
                readFromShard2( tx );
                tx.success();
            }
        }

        waitForDriverRelease( 2 );
        verifyCommitted( tx1 );
        verifyDriverReturned( shard1Driver, shard2Driver );
    }

    @Test
    void testRollback()
    {
        try ( var session = openSession() )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                writeToShard1( tx );
                readFromShard2( tx );
                tx.failure();
            }
        }

        waitForDriverRelease( 2 );
        verifyRolledBack( tx1 );
        verifyDriverReturned( shard1Driver, shard2Driver );
    }

    @Test
    void testShardTxBeginFailure()
    {

        mockShardDriver( shard1Driver, Mono.error( new IllegalStateException( "Begin failed on shard 1" ) ) );

        try ( Session session = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ) )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                readFromShard2( tx );
                readFromShard3( tx );
                writeToShard1( tx );
                fail( "Exception expected" );
            }
        }
        catch ( DatabaseException e )
        {
            assertEquals( "Neo.DatabaseError.Statement.ExecutionFailed", e.code() );
            assertThat( e.getMessage(), containsString( "Begin failed on shard 1" ) );
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception", e );
        }

        waitForDriverRelease( 3 );
        verifyDriverReturned( shard1Driver, shard2Driver, shard3Driver );
    }

    @Test
    void testShardTxCommitFailure()
    {
        when( tx1.commit() ).thenReturn( Mono.error( new IllegalStateException( "Commit failed on shard 1" ) ) );

        try ( var session = openSession() )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                writeToShard1( tx );
                readFromShard2( tx );
                readFromShard3( tx );
                tx.success();
            }
            fail( "Exception expected" );
        }
        catch ( DatabaseException e )
        {
            assertEquals( "Neo.DatabaseError.Transaction.TransactionCommitFailed", e.code() );
            assertThat( e.getMessage(), containsString( "Failed to commit remote transaction" ) );
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception", e );
        }

        waitForDriverRelease( 3 );
        verifyCommitted( tx1 );
        verifyDriverReturned( shard1Driver, shard2Driver, shard3Driver );
    }

    @Test
    void testShardTxRollbackFailure()
    {
        when( tx1.rollback() ).thenReturn( Mono.error( new IllegalStateException( "Rollback failed on shard 1" ) ) );

        try ( var session = openSession() )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                writeToShard1( tx );
                readFromShard2( tx );
                readFromShard3( tx );
                tx.failure();
            }
            fail( "Exception expected" );
        }
        catch ( DatabaseException e )
        {
            assertEquals( "Neo.DatabaseError.Transaction.TransactionRollbackFailed", e.code() );
            assertThat( e.getMessage(), containsString( "Failed to rollback remote transaction" ) );
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception", e );
        }

        waitForDriverRelease( 3 );
        verifyRolledBack( tx1 );
        verifyDriverReturned( shard1Driver, shard2Driver, shard3Driver );
    }

    @Test
    void testShardRunFailure()
    {
        when( shard3Driver.run( any(), any(), any(), any(), any(), any() ) ).thenThrow( new IllegalStateException( "Query on shard 3 failed" ) );

        try ( var session = openSession() )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                writeToShard1( tx );
                readFromShard2( tx );
                try
                {
                    readFromShard3( tx );
                    fail( "Exception expected" );
                }
                catch ( DatabaseException e )
                {
                    assertEquals( "Neo.DatabaseError.Statement.ExecutionFailed", e.code() );
                    assertThat( e.getMessage(), containsString( "Query on shard 3 failed" ) );
                }
                catch ( Exception e )
                {
                    fail( "Unexpected exception", e );
                }
                tx.success();
            }
        }

        waitForDriverRelease( 3 );
        verifyRolledBack( tx1 );
        verifyDriverReturned( shard1Driver, shard2Driver, shard3Driver );
    }

    @Test
    void testShardResultStreamFailure()
    {
        var result = mock( AutoCommitStatementResult.class );
        when( result.columns() ).thenReturn( Flux.fromIterable( List.of( "a", "b" ) ) );
        when( result.records() ).thenReturn( Flux.error( new IllegalStateException( "Result stream from shard 3 failed" ) ) );

        when( shard3Driver.run( any(), any(), any(), any(), any(), any() ) ).thenReturn( result );

        try ( var session = openSession() )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                writeToShard1( tx );
                readFromShard2( tx );
                try
                {
                    readFromShard3( tx );
                    fail( "Exception expected" );
                }
                catch ( DatabaseException e )
                {
                    assertEquals( "Neo.DatabaseError.Statement.ExecutionFailed", e.code() );
                    assertThat( e.getMessage(), containsString( "Result stream from shard 3 failed" ) );
                }
                catch ( Exception e )
                {
                    fail( "Unexpected exception", e );
                }
                tx.success();
            }
        }

        waitForDriverRelease( 3 );
        verifyRolledBack( tx1 );
        verifyDriverReturned( shard1Driver, shard2Driver, shard3Driver );
    }

    @Test
    void testReset()
    {
        try ( var session = openSession() )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                writeToShard1( tx );
                readFromShard2( tx );
                session.reset();

                verifyRolledBack( tx1 );
                verifyDriverReturned( shard1Driver, shard2Driver );
            }
        }
    }

    @Test
    void testTimeout()
    {
        ArgumentCaptor<Runnable> timeoutCallback = ArgumentCaptor.forClass( Runnable.class );
        when( jobScheduler.schedule( any(), timeoutCallback.capture(), anyLong(), any() ) ).thenReturn( mock( JobHandle.class ) );

        try ( var session = openSession() )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                writeToShard1( tx );
                readFromShard2( tx );

                timeoutCallback.getValue().run();

                tx.run( "USE mega.shard1 MATCH (n) RETURN n" ).consume();

                fail( "Exception expected" );
            }
        }
        catch ( ClientException e )
        {
            assertEquals( "Neo.ClientError.Transaction.TransactionTimedOut", e.code() );
            assertThat( e.getMessage(), containsString( "Trying to execute query in a terminated transaction" ) );
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception", e );
        }

        verifyRolledBack( tx1 );
        verifyDriverReturned( shard1Driver, shard2Driver );
    }

    private void verifyCommitted( FabricDriverTransaction tx )
    {
        verify( tx ).commit();
        verify( tx, never() ).rollback();
    }

    private void verifyRolledBack( FabricDriverTransaction tx )
    {
        verify( tx ).rollback();
        verify( tx, never() ).commit();
    }

    private void writeToShard1( Transaction tx )
    {
        tx.run( "USE mega.graph(1) CREATE(n) RETURN n" ).consume();
    }

    private void readFromShard2( Transaction tx )
    {
        tx.run( "USE mega.graph(2) MATCH (n) RETURN n" ).consume();
    }

    private void readFromShard3( Transaction tx )
    {
        tx.run( "USE mega.graph(3) MATCH (n) RETURN n" ).consume();
    }

    private void verifyDriverReturned( PooledDriver... driver )
    {
        Arrays.asList( driver ).forEach( d -> verify( d ).release() );
    }

    private void waitForDriverRelease( int count )
    {
        IntStream.range( count, 3 ).forEach( i -> latch.countDown() );
        try
        {
            assertTrue( latch.await( 1, TimeUnit.SECONDS ) );
        }
        catch ( InterruptedException e )
        {
            fail( e );
        }
    }

    private static FabricConfig.DriverConfig emptyDriverConfig()
    {
        return new FabricConfig.DriverConfig( null, null, null, null, null, null, null, null, null, null );
    }

    private Session openSession()
    {
        return clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() );
    }
}
