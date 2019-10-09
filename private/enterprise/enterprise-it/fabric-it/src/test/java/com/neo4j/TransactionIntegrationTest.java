/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.utils.CustomFunctions;
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

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.internal.SessionConfig;
import org.neo4j.driver.internal.shaded.reactor.core.publisher.Mono;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.harness.internal.InProcessNeo4j;
import org.neo4j.harness.internal.TestNeo4jBuilders;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class TransactionIntegrationTest
{
    private static Driver clientDriver;
    private static TestServer testServer;
    private static InProcessNeo4j remote0;
    private static InProcessNeo4j remote1;
    private static Driver shard0Driver;
    private static Driver shard1Driver;

    private static final ExecutorService executorService = Executors.newCachedThreadPool();

    private final TransactionEventCountingListener remote0Listener = new TransactionEventCountingListener();
    private final TransactionEventCountingListener remote1Listener = new TransactionEventCountingListener();

    @BeforeAll
    static void beforeAll() throws KernelException
    {

        remote0 = TestNeo4jBuilders.newInProcessBuilder().build();
        remote1 = TestNeo4jBuilders.newInProcessBuilder().build();

        PortUtils.Ports ports = PortUtils.findFreePorts();

        var configProperties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", remote0.boltURI().toString(),
                "fabric.graph.1.uri", remote1.boltURI().toString(),
                "fabric.routing.servers", "localhost:" + ports.bolt,
                "fabric.driver.connection.encrypted", "false",
                "fabric.stream.buffer.low.watermark", "1",
                "fabric.stream.sync.batch.size", "1",
                "fabric.stream.buffer.size", "10",
                "dbms.connector.bolt.listen_address", "0.0.0.0:" + ports.bolt,
                "dbms.connector.bolt.enabled", "true"
        );

        var config = Config.newBuilder()
                .setRaw( configProperties )
                .build();
        testServer = new TestServer( config );

        testServer.start();

        testServer.getDependencies().resolveDependency( GlobalProceduresRegistry.class )
                .registerFunction( CustomFunctions.class );

        clientDriver = GraphDatabase.driver(
                "neo4j://localhost:" + ports.bolt,
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                        .withoutEncryption()
                        .withMaxConnectionPoolSize( 3 )
                        .build() );

        shard0Driver = GraphDatabase.driver(
                remote0.boltURI(),
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                        .withoutEncryption()
                        .withMaxConnectionPoolSize( 3 )
                        .build() );
        shard1Driver = GraphDatabase.driver(
                remote1.boltURI(),
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                        .withoutEncryption()
                        .withMaxConnectionPoolSize( 3 )
                        .build() );
    }

    @BeforeEach
    void beforeEach()
    {
        try ( Transaction tx = shard0Driver.session().beginTransaction() )
        {
            tx.run( "MATCH (n) DETACH DELETE n" );
            tx.success();
        }
        try ( Transaction tx = shard1Driver.session().beginTransaction() )
        {
            tx.run( "MATCH (n) DETACH DELETE n" ).consume();
            tx.success();
        }

        remote0.databaseManagementService().registerTransactionEventListener( "neo4j", remote0Listener );
        remote1.databaseManagementService().registerTransactionEventListener( "neo4j", remote1Listener );
    }

    @AfterAll
    static void afterAll()
    {
        executorService.shutdownNow();

        List.<Runnable>of(
                () -> testServer.stop(),
                () -> clientDriver.close(),
                () -> shard0Driver.close(),
                () -> shard1Driver.close(),
                () -> remote0.close(),
                () -> remote1.close()
        ).parallelStream().forEach( Runnable::run );
    }

    @Test
    void testBasicTransactionHandling()
    {
        var tx = begin();

        var statement = String.join( "\n",
                "UNWIND [0, 1] AS gid",
                "CALL {",
                "  USE mega.graph(gid)",
                "  RETURN 1",
                "}",
                "WITH count(*) AS c",
                "CALL {",
                "  USE mega.graph0",
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
                "  USE mega.graph0",
                "  MATCH (n)",
                "  RETURN n",
                "}",
                "RETURN n"
            );

        var records = Flux.from(tx.run( statement ).records()).collectList().block();
        assertEquals(1, records.size());
        var node = records.get( 0 ).get( 0 ).asNode();
        assertEquals( "someValue", node.get( "someProperty" ).asString() );

        var records2 = Flux.from(tx.run( "USE mega.graph0 MATCH (n) RETURN n.someProperty AS val" ).records()).collectList().block();
        assertEquals(1, records2.size());
        assertEquals( "someValue", records2.get( 0 ).get( 0 ).asString() );

        commit( tx );

        verifyNoOpenTransactions();
        verify( remote0Listener, 1, 0 );
    }

    @Test
    void testReadWriteOnSameRemote()
    {
        var tx = begin();

        var statement = String.join( "\n",
                "CALL {",
                "  USE mega.graph0",
                "  CREATE ({someProperty: 'someValue'})",
                "  RETURN 1",
                "}",
                "CALL {",
                "  USE mega.graph0",
                "  UNWIND range(1, 100) AS i",
                "  RETURN i",
                "}",
                "CALL {",
                "  USE mega.graph0",
                "  WITH i",
                "  CREATE (n {counter: i})",
                "  RETURN n",
                "}",
                "RETURN count(n) AS count"
        );

        var records = Flux.from(tx.run( statement ).records()).collectList().block();
        assertEquals(1, records.size());
        assertEquals( 100, records.get( 0 ).get( 0 ).asInt() );

        commit( tx );

        verifyNoOpenTransactions();
        verify( remote0Listener, 1, 0 );
    }

    @Test
    void errorWhileStreamingBetweenRemotes()
    {
        var tx = begin();

        var statement = String.join( "\n",
                "CALL {",
                "  USE mega.graph0",
                "  UNWIND range(0, 100) AS i",
                "  RETURN i",
                "}",
                "CALL {",
                "  USE mega.graph1",
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
            assertThat( e.getMessage(), containsString( "/ by zero" ) );
        }

        verifyNoOpenTransactions();
    }

    @Test
    void errorWhileStreamingBetweenRemotes2()
    {
        var tx = begin();

        var statement = String.join( "\n",
                "CALL {",
                "  USE mega.graph0",
                "  UNWIND range(0, 100) AS i",
                "  RETURN i",
                "}",
                "CALL {",
                "  USE mega.graph1",
                "  WITH i",
                "  CREATE (n {counter: i})",
                "  RETURN n",
                "}",
                "CALL {",
                "  USE mega.graph0",
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
            assertThat( e.getMessage(), containsString( "/ by zero" ) );
        }

        verifyNoOpenTransactions();
    }

    @Test
    void testRollbackWhileStreaming() throws InterruptedException
    {
        var tx = begin();

        var statement = String.join( "\n",
                "CALL {",
                "  USE mega.graph0",
                "  CREATE ({someProperty: 'someValue'})",
                "  RETURN 1",
                "}",
                "CALL {",
                "  USE mega.graph0",
                "  UNWIND range(1, 1000) AS i",
                "  RETURN i",
                "}",
                "RETURN i"
        );

        var subscriber = new ActionSubscriber( 10, s -> rollback( tx ) );
        Flux.from( tx.run( statement ).records() ).subscribe( subscriber );
        assertTrue( subscriber.latch.await( 1, TimeUnit.SECONDS ) );

        verifyNoOpenTransactions();
    }

    @Test
    void testCommitWhileStreaming() throws InterruptedException
    {
        var tx = begin();

        var statement = String.join( "\n",
                "CALL {",
                "  USE mega.graph0",
                "  CREATE ({someProperty: 'someValue'})",
                "  RETURN 1",
                "}",
                "CALL {",
                "  USE mega.graph0",
                "  UNWIND range(1, 1000) AS i",
                "  RETURN i",
                "}",
                "RETURN i"
        );

        var subscriber = new ActionSubscriber( 10, s -> commit( tx ) );
        Flux.from( tx.run( statement ).records() ).subscribe( subscriber );
        assertTrue( subscriber.latch.await( 1, TimeUnit.SECONDS ) );

        verifyNoOpenTransactions();
    }

    @Test
    void testCancelWhileStreaming() throws InterruptedException
    {
        var tx = begin();

        var statement = String.join( "\n",
                "CALL {",
                "  USE mega.graph0",
                "  CREATE ({someProperty: 'someValue'})",
                "  RETURN 1",
                "}",
                "CALL {",
                "  USE mega.graph0",
                "  UNWIND range(1, 1000) AS i",
                "  RETURN i",
                "}",
                "RETURN i"
        );

        var subscriber = new ActionSubscriber( 10, Subscription::cancel );
        Flux.from( tx.run( statement ).records() ).subscribe( subscriber );
        assertTrue( subscriber.latch.await( 1, TimeUnit.SECONDS ) );

        // the result stream is canceled, but the transaction should be still open with all its data
        var records = Flux.from(tx.run( "USE mega.graph0 MATCH (n) RETURN n.someProperty AS val" ).records()).collectList().block();
        assertEquals(1, records.size());
        assertEquals( "someValue", records.get( 0 ).get( 0 ).asString() );

        commit( tx );

        verifyNoOpenTransactions();
        verify( remote0Listener, 1, 0 );

        verifyNoOpenTransactions();
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
        verifyNoRemoteTransactions(remote0);
        verifyNoRemoteTransactions(remote1);
    }

    private void verifyNoFabricTransactions()
    {
        var dbms = getFabricDbms();
        var db = (GraphDatabaseAPI) dbms.database( "mega" );
        verifyNoTransactions( db );
    }

    private DatabaseManagementService getFabricDbms()
    {
        return testServer.getDependencies().resolveDependency( DatabaseManagementService.class );
    }

    private void verifyNoRemoteTransactions( InProcessNeo4j remote )
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
