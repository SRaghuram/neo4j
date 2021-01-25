/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.utils.TestFabric;
import com.neo4j.utils.TestFabricFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.shaded.reactor.core.publisher.Mono;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.fabric.transaction.TransactionManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.harness.Neo4j;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.helpers.Strings.joinAsLines;

class TransactionIntegrationTest
{
    private static Driver clientDriver;
    private static TestFabric testFabric;
    private static Driver shard0Driver;
    private static Driver shard1Driver;

    private static final ExecutorService executorService = Executors.newCachedThreadPool();

    private final TransactionEventCountingListener remote0Listener = new TransactionEventCountingListener();
    private final TransactionEventCountingListener remote1Listener = new TransactionEventCountingListener();

    @BeforeAll
    static void beforeAll()
    {
        var additionalShardSettings = Map.of(
                "dbms.security.auth_enabled", "true"
        );

        var additionalSettings = Map.of(
                "fabric.driver.connection.encrypted", "false",
                "fabric.stream.buffer.low_watermark", "1",
                "fabric.stream.batch_size", "1",
                "fabric.stream.buffer.size", "10",
                "dbms.security.auth_enabled", "true"
        );

        testFabric = new TestFabricFactory()
                .withFabricDatabase( "mega" )
                .withShards( 2 )
                .withAdditionalShardSettings( additionalShardSettings )
                .withAdditionalSettings( additionalSettings )
                .build();

        var testServer = testFabric.getTestServer();

        var localDbms = testServer.getDependencies().resolveDependency( DatabaseManagementService.class );
        createUser( localDbms );
        createUser( testFabric.getShard( 0 ).databaseManagementService() );
        createUser( testFabric.getShard( 1 ).databaseManagementService() );

        clientDriver = GraphDatabase.driver(
                testServer.getBoltRoutingUri(),
                AuthTokens.basic( "myUser", "hello" ),
                org.neo4j.driver.Config.builder()
                        .withoutEncryption()
                        .withMaxConnectionPoolSize( 3 )
                        .build() );

        shard0Driver = GraphDatabase.driver(
                testFabric.getShard( 0 ).boltURI(),
                AuthTokens.basic( "myUser", "hello" ),
                org.neo4j.driver.Config.builder()
                        .withoutEncryption()
                        .withMaxConnectionPoolSize( 3 )
                        .build() );
        shard1Driver = GraphDatabase.driver(
                testFabric.getShard( 1 ).boltURI(),
                AuthTokens.basic( "myUser", "hello" ),
                org.neo4j.driver.Config.builder()
                        .withoutEncryption()
                        .withMaxConnectionPoolSize( 3 )
                        .build() );

        createDatabase( "dbA" );
        createDatabase( "dbB" );
    }

    @BeforeEach
    void beforeEach()
    {
        try ( Transaction tx = shard0Driver.session().beginTransaction() )
        {
            tx.run( "MATCH (n) DETACH DELETE n" );
            tx.commit();
        }
        try ( Transaction tx = shard1Driver.session().beginTransaction() )
        {
            tx.run( "MATCH (n) DETACH DELETE n" ).consume();
            tx.commit();
        }

        testFabric.getShard( 0 ).databaseManagementService().registerTransactionEventListener( "neo4j", remote0Listener );
        testFabric.getShard( 1 ).databaseManagementService().registerTransactionEventListener( "neo4j", remote1Listener );
    }

    @AfterAll
    static void afterAll()
    {
        executorService.shutdownNow();

        List.<Runnable>of(
                () -> testFabric.close(),
                () -> clientDriver.close(),
                () -> shard0Driver.close(),
                () -> shard1Driver.close()
        ).parallelStream().forEach( Runnable::run );
    }

    @Test
    void testBasicTransactionHandling()
    {
        var tx = begin();

        var statement = joinAsLines(
                "UNWIND [0, 1] AS gid",
                "CALL {",
                "  USE mega.graph(gid)",
                "  RETURN 1",
                "}",
                "WITH count(*) AS c",
                "CALL {",
                "  USE mega.graph(0)",
                "  CREATE ({someProperty: 'someValue'})",
                "  RETURN 2",
                "}",
                "UNWIND [0, 1] AS gid2",
                "CALL {",
                "  USE mega.graph(gid2)",
                "  RETURN 3",
                "}",
                "WITH count(*) AS c",
                "CALL {",
                "  USE mega.graph(0)",
                "  MATCH (n)",
                "  RETURN n",
                "}",
                "RETURN n"
        );

        var records = Flux.from( tx.run( statement ).records() ).collectList().block();
        assertEquals( 1, records.size() );
        var node = records.get( 0 ).get( 0 ).asNode();
        assertEquals( "someValue", node.get( "someProperty" ).asString() );

        var records2 = Flux.from( tx.run( "USE mega.graph(0) MATCH (n) RETURN n.someProperty AS val" ).records() ).collectList().block();
        assertEquals( 1, records2.size() );
        assertEquals( "someValue", records2.get( 0 ).get( 0 ).asString() );

        commit( tx );

        verifyNoOpenTransactions();
        verify( remote0Listener, 1, 0 );
    }

    @Test
    void testReadWriteOnSameRemote()
    {
        var tx = begin();

        var statement = joinAsLines(
                "CALL {",
                "  USE mega.graph(0)",
                "  CREATE ({someProperty: 'someValue'})",
                "  RETURN 1",
                "}",
                "CALL {",
                "  USE mega.graph(0)",
                "  UNWIND range(1, 100) AS i",
                "  RETURN i",
                "}",
                "CALL {",
                "  USE mega.graph(0)",
                "  WITH i",
                "  CREATE (n {counter: i})",
                "  RETURN n",
                "}",
                "RETURN count(n) AS count"
        );

        var records = Flux.from( tx.run( statement ).records() ).collectList().block();
        assertEquals( 1, records.size() );
        assertEquals( 100, records.get( 0 ).get( 0 ).asInt() );

        commit( tx );

        verifyNoOpenTransactions();
        verify( remote0Listener, 1, 0 );
    }

    @Test
    void errorWhileStreamingBetweenRemotes()
    {
        var tx = begin();

        var statement = joinAsLines(
                "CALL {",
                "  USE mega.graph(0)",
                "  UNWIND range(0, 100) AS i",
                "  RETURN i",
                "}",
                "CALL {",
                "  USE mega.graph(1)",
                "  WITH i",
                "  RETURN 1/i AS x",
                "}",
                "RETURN count(x) AS count"
        );

        try
        {
            Flux.from( tx.run( statement ).records() ).collectList().block();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e.getMessage() ).contains( "/ by zero" );
        }

        verifyNoOpenTransactions();
    }

    @Test
    void errorWhileStreamingBetweenRemotes2()
    {
        var tx = begin();

        var statement = joinAsLines(
                "CALL {",
                "  USE mega.graph(0)",
                "  UNWIND range(0, 100) AS i",
                "  RETURN i",
                "}",
                "CALL {",
                "  USE mega.graph(1)",
                "  WITH i",
                "  CREATE (n {counter: i})",
                "  RETURN n",
                "}",
                "CALL {",
                "  USE mega.graph(0)",
                "  WITH i",
                "  RETURN 1/i AS x",
                "}",
                "RETURN count(n) AS count"
        );

        try
        {
            Flux.from( tx.run( statement ).records() ).collectList().block();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e.getMessage() ).contains( "/ by zero" );
        }

        verifyNoOpenTransactions();
    }

    @Test
    void testRollbackWhileStreaming() throws InterruptedException
    {
        var tx = begin();

        var statement = joinAsLines(
                "CALL {",
                "  USE mega.graph(0)",
                "  CREATE ({someProperty: 'someValue'})",
                "  RETURN 1",
                "}",
                "CALL {",
                "  USE mega.graph(0)",
                "  UNWIND range(1, 1000) AS i",
                "  RETURN i",
                "}",
                "RETURN i"
        );

        var subscriber = new ActionSubscriber( 10, s -> rollback( tx ) );
        Flux.from( tx.run( statement ).records() ).subscribe( subscriber );
        assertTrue( subscriber.latch.await( 10, TimeUnit.SECONDS ) );

        verifyNoOpenTransactions();
    }

    @Test
    void testCommitWhileStreaming() throws InterruptedException
    {
        var tx = begin();

        var statement = joinAsLines(
                "CALL {",
                "  USE mega.graph(0)",
                "  CREATE ({someProperty: 'someValue'})",
                "  RETURN 1",
                "}",
                "CALL {",
                "  USE mega.graph(0)",
                "  UNWIND range(1, 1000) AS i",
                "  RETURN i",
                "}",
                "RETURN i"
        );

        var subscriber = new ActionSubscriber( 10, s -> commit( tx ) );
        Flux.from( tx.run( statement ).records() ).subscribe( subscriber );
        assertTrue( subscriber.latch.await( 10, TimeUnit.SECONDS ) );

        verifyNoOpenTransactions();
    }

    @Test
    void testCancelWhileStreaming() throws InterruptedException
    {
        var tx = begin();

        var statement = joinAsLines(
                "CALL {",
                "  USE mega.graph(0)",
                "  CREATE ({someProperty: 'someValue'})",
                "  RETURN 1",
                "}",
                "CALL {",
                "  USE mega.graph(0)",
                "  UNWIND range(1, 1000) AS i",
                "  RETURN i",
                "}",
                "RETURN i"
        );

        var subscriber = new ActionSubscriber( 10, Subscription::cancel );
        Flux.from( tx.run( statement ).records() ).subscribe( subscriber );
        assertTrue( subscriber.latch.await( 10, TimeUnit.SECONDS ) );

        // the result stream is canceled, but the transaction should be still open with all its data
        var records = Flux.from( tx.run( "USE mega.graph(0) MATCH (n) RETURN n.someProperty AS val" ).records() ).collectList().block();
        assertEquals( 1, records.size() );
        assertEquals( "someValue", records.get( 0 ).get( 0 ).asString() );

        commit( tx );

        verifyNoOpenTransactions();
        verify( remote0Listener, 1, 0 );

        verifyNoOpenTransactions();
    }

    @Test
    void testErrorPropagation()
    {
        var tx = begin();

        var statement = joinAsLines(
                "UNWIND [1, 1, 0, 1, 1] AS gid",
                "WITH gid, 1/gid AS x",
                "CALL {",
                "  USE mega.graph(gid)",
                "  RETURN 1 As y",
                "}",
                "RETURN x, y"
        );

        var exception = assertThrows( ClientException.class, () -> Flux.from( tx.run( statement ).records() )
                .collectList()
                .block() );

        assertThat( exception.getMessage() ).contains( "/ by zero" );

        verifyNoOpenTransactions();
    }

    @Test
    void testTransactionTermination()
    {
        var sessionConfig = SessionConfig.builder().withDatabase( "mega" ).build();
        try ( var session = clientDriver.session( sessionConfig ) )
        {
            var txConfig = TransactionConfig.builder().withMetadata( Map.of( "tx-marker", "tx1" ) ).build();
            var tx = session.beginTransaction( txConfig );
            run( tx, "dbA" );
            run( tx, "dbB" );
            run( tx, "mega.graph(0)" );

            assertEquals( 2, runningTransactionsByMarker( clientDriver, "tx1" ) );
            assertEquals( 1, runningTransactionsByMarker( shard0Driver, "tx1" ) );

            var testServer = testFabric.getTestServer();
            var transactionManager = testServer.getDependencies().resolveDependency( TransactionManager.class );
            var compositeTransaction = transactionManager.getOpenTransactions().stream()
                                                         .filter( compositeTx -> "tx1"
                                                                 .equals( compositeTx.getTransactionInfo().getTxMetadata().get( "tx-marker" ) ) )
                                                         .findAny().get();

            var dbms = testServer.getDependencies().resolveDependency( DatabaseManagementService.class );
            var db = (GraphDatabaseFacade) dbms.database( "dbA" );
            var kernelTransactions = db.getDependencyResolver().resolveDependency( KernelTransactions.class );
            var childTransaction = kernelTransactions.executingTransactions()
                                                     .stream()
                                                     .filter( kernelTx -> "tx1".equals( kernelTx.getMetaData().get( "tx-marker" ) ) )
                                                     .findAny()
                                                     .get();

            // just a random status to see if it gets propagated
            childTransaction.markForTermination( Status.Transaction.LockAcquisitionTimeout );

            assertTrue( compositeTransaction.getReasonIfTerminated().isPresent() );
            assertEquals( Status.Transaction.LockAcquisitionTimeout, compositeTransaction.getReasonIfTerminated().get() );

            assertEquals( 0, runningTransactionsByMarker( clientDriver, "tx1" ) );
            assertEquals( 0, runningTransactionsByMarker( shard0Driver, "tx1" ) );
        }
    }

    private RxTransaction begin()
    {
        var tx = clientDriver.rxSession( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction();
        return Mono.from( tx ).block();
    }

    private void commit( RxTransaction tx )
    {
        Mono.from( tx.commit() ).block();
    }

    private void rollback( RxTransaction tx )
    {
        Mono.from( tx.rollback() ).block();
    }

    private void verify( TransactionEventCountingListener listener, int commits, int rollbacks )
    {
        assertEquals( commits, listener.afterCommitInvocations.get() );
        assertEquals( rollbacks, listener.afterRollbackInvocations.get() );
    }

    private void verifyNoOpenTransactions()
    {
        verifyNoFabricTransactions();
        verifyNoRemoteTransactions( testFabric.getShard( 0 ) );
        verifyNoRemoteTransactions( testFabric.getShard( 1 ) );
    }

    private void verifyNoFabricTransactions()
    {
        var dbms = getFabricDbms();
        var db = (GraphDatabaseAPI) dbms.database( "mega" );
        verifyNoTransactions( db );
    }

    private DatabaseManagementService getFabricDbms()
    {
        return testFabric.getTestServer().getDependencies().resolveDependency( DatabaseManagementService.class );
    }

    private void verifyNoRemoteTransactions( Neo4j remote )
    {
        var db = (GraphDatabaseAPI) remote.defaultDatabaseService();
        verifyNoTransactions( db );
    }

    private void verifyNoTransactions( GraphDatabaseAPI db )
    {
        var start = System.currentTimeMillis();
        while ( true )
        {
            var activeTransactions = db.getDependencyResolver().resolveDependency( KernelTransactions.class ).activeTransactions();

            if ( activeTransactions.isEmpty() )
            {
                return;
            }

            if ( System.currentTimeMillis() - start < 1000 )
            {
                try
                {
                    Thread.sleep( 1 );
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
            }
            else
            {
                assertEquals( Set.of(), activeTransactions );
            }
        }
    }

    private static void createUser( DatabaseManagementService dbms )
    {
        var db = dbms.database( "system" );
        db.executeTransactionally( "CREATE USER myUser SET PASSWORD 'hello' CHANGE NOT REQUIRED" );
        db.executeTransactionally( "GRANT ROLE admin TO myUser" );
    }

    private void run( Transaction tx, String dbName )
    {
        var query = joinAsLines( "USE " + dbName,
                "MATCH (n:Person) RETURN n"
        );
        tx.run( query ).list();
    }

    private int runningTransactionsByMarker( Driver driver, String marker )
    {
        try ( var session = driver.session( SessionConfig.defaultConfig() ) )
        {
            var records = session.run( "CALL dbms.listTransactions" ).list();
            return (int) records.stream()
                                .filter( tx -> marker.equals( tx.get( "metaData" ).get( "tx-marker" ).asObject() ) )
                                .filter( tx -> getColumn( tx, "status" ).equals( "Running" ) )
                                .count();
        }
    }

    private String getColumn( Record record, String column )
    {
        return (String) record.get( column ).asObject();
    }

    private static void createDatabase( String name )
    {
        var localDbms = testFabric.getTestServer().getDependencies().resolveDependency( DatabaseManagementService.class );
        var systemDb = localDbms.database( "system" );
        systemDb.executeTransactionally( "CREATE DATABASE " + name );
    }

    private static class TransactionEventCountingListener extends TransactionEventListenerAdapter<Object>
    {
        private final AtomicInteger afterCommitInvocations = new AtomicInteger();
        private final AtomicInteger afterRollbackInvocations = new AtomicInteger();

        @Override
        public Object beforeCommit( TransactionData data, org.neo4j.graphdb.Transaction transaction, GraphDatabaseService databaseService )
        {
            return null;
        }

        @Override
        public void afterCommit( TransactionData data, Object state, GraphDatabaseService databaseService )
        {
            afterCommitInvocations.incrementAndGet();
        }

        @Override
        public void afterRollback( TransactionData data, Object state, GraphDatabaseService databaseService )
        {
            afterRollbackInvocations.incrementAndGet();
        }
    }

    private class ActionSubscriber implements Subscriber<Record>
    {

        private final AtomicInteger counter = new AtomicInteger( 0 );
        private final CountDownLatch latch = new CountDownLatch( 1 );

        private final int skip;
        private final Consumer<Subscription> action;
        private Subscription subscription;

        ActionSubscriber( int skip, Consumer<Subscription> action )
        {
            this.skip = skip;
            this.action = action;
        }

        @Override
        public void onSubscribe( Subscription subscription )
        {
            this.subscription = subscription;
            subscription.request( 10 );
        }

        @Override
        public void onNext( Record record )
        {
            if ( counter.incrementAndGet() == skip )
            {
                executorService.submit( () ->
                {
                    action.accept( subscription );
                    latch.countDown();
                } );
            }
        }

        @Override
        public void onError( Throwable throwable )
        {
            throwable.printStackTrace();
        }

        @Override
        public void onComplete()
        {

        }
    }
}
