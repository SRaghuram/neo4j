/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.locking;

import com.neo4j.test.rule.CommercialDbmsRule;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.test.Barrier;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.OtherThreadRule;

import static com.neo4j.kernel.impl.locking.DeferringStatementLocksFactory.Configuration.deferred_locks_enabled;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.internal.helpers.collection.Iterables.count;

@Ignore // currently does not run because deferred locks also take effect on system graph?
public class DeferringLocksIT
{
    private static final long TEST_TIMEOUT = 30_000;

    private static final Label LABEL = Label.label( "label" );
    private static final String PROPERTY_KEY = "key";
    private static final String VALUE_1 = "value1";
    private static final String VALUE_2 = "value2";

    @Rule
    public final DbmsRule dbRule = new CommercialDbmsRule().startLazily();
    @Rule
    public final OtherThreadRule<Void> t2 = new OtherThreadRule<>();
    @Rule
    public final OtherThreadRule<Void> t3 = new OtherThreadRule<>();

    private GraphDatabaseService db;

    @Before
    public void initDb()
    {
        dbRule.withSetting( deferred_locks_enabled, true );
        db = dbRule.getGraphDatabaseAPI();
    }

    @Test( timeout = TEST_TIMEOUT )
    public void shouldNotFreakOutIfTwoTransactionsDecideToEachAddTheSameProperty() throws Exception
    {
        // GIVEN
        final Barrier.Control barrier = new Barrier.Control();
        final Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            tx.commit();
        }

        // WHEN
        t2.execute( (WorkerCommand<Void,Void>) state ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                node.setProperty( PROPERTY_KEY, VALUE_1 );
                tx.commit();
                barrier.reached();
            }
            return null;
        } );
        try ( Transaction tx = db.beginTx() )
        {
            barrier.await();
            node.setProperty( PROPERTY_KEY, VALUE_2 );
            tx.commit();
            barrier.release();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 1, count( node.getPropertyKeys() ) );
            tx.commit();
        }
    }

    @Test( timeout = TEST_TIMEOUT )
    public void firstRemoveSecondChangeProperty() throws Exception
    {
        // GIVEN
        final Barrier.Control barrier = new Barrier.Control();
        final Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            node.setProperty( PROPERTY_KEY, VALUE_1 );
            tx.commit();
        }

        // WHEN
        Future<Void> future = t2.execute( state ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                node.removeProperty( PROPERTY_KEY );
                tx.commit();
                barrier.reached();
            }
            return null;
        } );
        try ( Transaction tx = db.beginTx() )
        {
            barrier.await();
            node.setProperty( PROPERTY_KEY, VALUE_2 );
            tx.commit();
            barrier.release();
        }

        future.get();
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( VALUE_2, node.getProperty( PROPERTY_KEY, VALUE_2 ) );
            tx.commit();
        }
    }

    @Test( timeout = TEST_TIMEOUT )
    public void removeNodeChangeNodeProperty() throws Exception
    {
        // GIVEN
        final Barrier.Control barrier = new Barrier.Control();
        final long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            nodeId = node.getId();
            node.setProperty( PROPERTY_KEY, VALUE_1 );
            tx.commit();
        }

        // WHEN
        Future<Void> future = t2.execute( state ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.getNodeById( nodeId ).delete();
                tx.commit();
                barrier.reached();
            }
            return null;
        } );
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                barrier.await();
                db.getNodeById( nodeId ).setProperty( PROPERTY_KEY, VALUE_2 );
                tx.commit();
                barrier.release();
            }
        }
        catch ( TransactionFailureException e )
        {
            // Node was already deleted, fine.
            assertThat( e.getCause(), instanceOf( InvalidRecordException.class ) );
        }

        future.get();
        try ( Transaction tx = db.beginTx() )
        {
            try
            {
                db.getNodeById( nodeId );
                assertEquals( VALUE_2, db.getNodeById( nodeId ).getProperty( PROPERTY_KEY, VALUE_2 ) );
            }
            catch ( NotFoundException e )
            {
                // Fine, its gone
            }
            tx.commit();
        }
    }

    @Test( timeout = TEST_TIMEOUT )
    public void readOwnChangesFromRacingIndexNoBlock() throws Throwable
    {
        Future<Void> t2Future = t2.execute( state ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                createNodeWithProperty( LABEL, PROPERTY_KEY, VALUE_1 );
                assertNodeWith( LABEL, PROPERTY_KEY, VALUE_1 );

                tx.commit();
            }
            return null;
        } );

        Future<Void> t3Future = t3.execute( state ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                createAndAwaitIndex( LABEL, PROPERTY_KEY );
                tx.commit();
            }
            return null;
        } );

        t3Future.get();
        t2Future.get();

        assertInTxNodeWith( LABEL, PROPERTY_KEY, VALUE_1 );
    }

    @Test( timeout = TEST_TIMEOUT )
    public void readOwnChangesWithoutIndex()
    {
        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( LABEL );
            node.setProperty( PROPERTY_KEY, VALUE_1 );

            assertNodeWith( LABEL, PROPERTY_KEY, VALUE_1 );

            tx.commit();
        }

        assertInTxNodeWith( LABEL, PROPERTY_KEY, VALUE_1 );
    }

    private void assertInTxNodeWith( Label label, String key, Object value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertNodeWith( label, key, value );
            tx.commit();
        }
    }

    private void assertNodeWith( Label label, String key, Object value )
    {
        try ( ResourceIterator<Node> nodes = db.findNodes( label, key, value ) )
        {
            assertTrue( nodes.hasNext() );
            Node foundNode = nodes.next();
            assertTrue( foundNode.hasLabel( label ) );
            assertEquals( value, foundNode.getProperty( key ) );
        }
    }

    private Node createNodeWithProperty( Label label, String key, Object value )
    {
        Node node = db.createNode( label );
        node.setProperty( key, value );
        return node;
    }

    private WorkerCommand<Void,Void> createAndAwaitIndex( final Label label, final String key )
    {
        return state ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().indexFor( label ).on( key ).create();
                tx.commit();
            }
            try ( Transaction ignore = db.beginTx() )
            {
                db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            }
            return null;
        };
    }
}
