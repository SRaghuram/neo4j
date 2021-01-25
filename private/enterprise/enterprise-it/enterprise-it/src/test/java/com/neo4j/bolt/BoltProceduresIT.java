/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import com.neo4j.configuration.OnlineBackupSettings;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.stream.Stream;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.rule.Neo4jRule;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

import static com.neo4j.bolt.BoltDriverHelper.graphDatabaseDriver;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.driver.SessionConfig.forDatabase;

public class BoltProceduresIT
{
    @ClassRule
    public static final Neo4jRule db = new Neo4jRule()
            .withProcedure( BoltTestProcedures.class )
            .withConfig( OnlineBackupSettings.online_backup_enabled, false );

    private static Driver driver;

    @BeforeClass
    public static void setUp() throws Exception
    {
        driver = graphDatabaseDriver( db.boltURI() );
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        if ( driver != null )
        {
            driver.close();
        }
    }

    /**
     * Test creates a situation where streaming of a node fails when accessing node labels/properties.
     * It fails because transaction is terminated. Bolt server should not send half-written message.
     * Driver should receive a regular FAILURE message saying that transaction has been terminated.
     */
    @Test
    public void shouldTransmitStreamingFailure()
    {
        try ( Session session = driver.session() )
        {
            Map<String,Object> params = new HashMap<>();
            params.put( "name1", randomLongString() );
            params.put( "name2", randomLongString() );
            session.run( "CREATE (n1 :Person {name: $name1}), (n2 :Person {name: $name2}) RETURN n1, n2", params ).consume();

            var result = session.run( "CALL test.readNodesReturnThemAndTerminateTheTransaction() YIELD node" );
            //we cannot know for sure when the error occurs since it depends on whether the result is being materialized
            //or not in the runtime
            try
            {
                assertTrue( result.hasNext() );
                Record record = result.next();
                assertEquals( "Person", Iterables.single( record.get( 0 ).asNode().labels() ) );
                assertNotNull( record.get( 0 ).asNode().get( "name" ) );
                assertFalse( result.hasNext() );
                fail( "Exception expected" );
            }
            catch ( TransientException e )
            {
                assertEquals( Status.Transaction.Terminated.code().serialize(), e.code() );
            }
        }
    }

    @Test
    public void shouldHaveAccessToRoutingProcedureWithDatabaseNameOnSystemDb()
    {
        try ( var session = driver.session( forDatabase( "system" ) ) )
        {
            var params = new HashMap<String,Object>();
            params.put( "context", NullValue.NULL );
            params.put( "database", NullValue.NULL );
            var result = session.run( "CALL dbms.routing.getRoutingTable($context, $database)", params );

            var servers = result.single().get( "servers" ).asList( Values.ofMap( Values.ofValue() ) );
            assertThat( servers.size() ).isEqualTo( 3 );
            var routingTable = new HashMap<String,String>();
            for ( var entry : servers )
            {
                var addresses = entry.get( "addresses" ).asList( Values.ofString() );
                assertThat( addresses.size() ).isEqualTo( 1 );
                routingTable.put( entry.get( "role" ).asString(), addresses.get( 0 ) );
            }

            assertThat( routingTable.keySet() ).contains( "WRITE", "READ", "ROUTE" );
            assertThat( routingTable.get( "WRITE" ) ).isEqualTo( routingTable.get( "READ" ) );
            assertThat( routingTable.get( "WRITE" ) ).isEqualTo( routingTable.get( "ROUTE" ) );
        }
    }

    @Test
    public void shouldHaveAccessToRoutingProcedureWithoutDatabaseNameOnDefaultDb()
    {
        try ( var session = driver.session() )
        {
            var params = new HashMap<String,Object>();
            params.put( "context", NullValue.NULL );
            var result = session.run( "CALL dbms.cluster.routing.getRoutingTable($context)", params );

            var servers = result.single().get( "servers" ).asList( Values.ofMap( Values.ofValue() ) );
            assertThat( servers.size() ).isEqualTo( 3 );
            var routingTable = new HashMap<String,String>();
            for ( var entry : servers )
            {
                var addresses = entry.get( "addresses" ).asList( Values.ofString() );
                assertThat( addresses.size() ).isEqualTo( 1 );
                routingTable.put( entry.get( "role" ).asString(), addresses.get( 0 ) );
            }

            assertThat( routingTable.keySet() ).contains( "WRITE", "READ", "ROUTE" );
            assertThat( routingTable.get( "WRITE" ) ).isEqualTo( routingTable.get( "READ" ) );
            assertThat( routingTable.get( "WRITE" ) ).isEqualTo( routingTable.get( "ROUTE" ) );
        }
    }

    private static String randomLongString()
    {
        return RandomStringUtils.randomAlphanumeric( 10_000 );
    }

    public static class BoltTestProcedures
    {
        @Context
        public GraphDatabaseService db;

        @Context
        public KernelTransaction tx;

        @Context
        public Transaction transaction;

        @Procedure( name = "test.readNodesReturnThemAndTerminateTheTransaction", mode = Mode.READ )
        public Stream<NodeResult> readNodesReturnThemAndTerminateTheTransaction()
        {
            Result result = transaction.execute( "MATCH (n) RETURN n" );

            NodeResult[] results = result.stream()
                    .map( record -> (Node) record.get( "n" ) )
                    .map( NodeResult::new )
                    .toArray( NodeResult[]::new );

            return Iterators.stream( new TransactionTerminatingIterator<>( tx, results ) );
        }
    }

    public static class NodeResult
    {
        public Node node;

        NodeResult( Node node )
        {
            this.node = node;
        }
    }

    /**
     * Returnes given elements, terminates the transaction before returning the very last one.
     *
     * @param <T> type of elements.
     */
    private static class TransactionTerminatingIterator<T> implements Iterator<T>
    {
        final KernelTransaction tx;
        final Queue<T> elements;

        @SafeVarargs
        private TransactionTerminatingIterator( KernelTransaction tx, T... elements )
        {
            this.tx = tx;
            this.elements = new ArrayDeque<>();
            Collections.addAll( this.elements, elements );
        }

        @Override
        public boolean hasNext()
        {
            return !elements.isEmpty();
        }

        @Override
        public T next()
        {
            if ( elements.size() == 1 )
            {
                // terminate transaction before returning the last element
                tx.markForTermination( Status.Transaction.Terminated );
            }
            T element = elements.poll();
            if ( element == null )
            {
                throw new NoSuchElementException();
            }
            return element;
        }
    }
}
