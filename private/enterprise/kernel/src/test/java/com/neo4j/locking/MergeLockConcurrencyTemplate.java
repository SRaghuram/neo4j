/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.locking;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.concurrent.ThreadingExtension;
import org.neo4j.test.rule.concurrent.ThreadingRule;
import org.neo4j.util.concurrent.Futures;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.lock_manager;
import static org.neo4j.internal.helpers.collection.Iterators.single;

@ImpermanentDbmsExtension( configurationCallback = "configure" )
@ExtendWith( ThreadingExtension.class )
abstract class MergeLockConcurrencyTemplate
{
    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private ThreadingRule threads;

    private final String lockManager;

    MergeLockConcurrencyTemplate( String lockManager )
    {
        this.lockManager = lockManager;
    }

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( lock_manager, lockManager );
    }

    @Test
    void shouldNotDeadlockOnMergeFollowedByPropertyAssignment() throws Exception
    {
        withConstraint( mergeThen( this::reassignProperties ) );
    }

    @Test
    void shouldNotDeadlockOnMergeFollowedByLabelReAddition() throws Exception
    {
        withConstraint( mergeThen( this::reassignLabels ) );
    }

    private void withConstraint( ThrowingFunction<CyclicBarrier,Node,Exception> action ) throws Exception
    {
        db.executeTransactionally( "CREATE CONSTRAINT ON (foo:Foo) ASSERT foo.bar IS UNIQUE" );
        CyclicBarrier barrier = new CyclicBarrier( 2 );
        Node node = createMergeNode();

        // when
        List<Node> result = Futures.getAllResults( threads.multiple( barrier.getParties(), action, barrier ) );

        // then
        assertEquals( 2, result.size(), "size of result" );
        assertEquals( node, result.get( 0 ) );
        assertEquals( node, result.get( 1 ) );
    }

    private Node createMergeNode()
    {
        Node node;
        try ( Transaction transaction = db.beginTx() )
        {
            node = mergeNode( transaction );
            transaction.commit();
        }
        return node;
    }

    private ThrowingFunction<CyclicBarrier,Node,Exception> mergeThen( ThrowingConsumer<Node,? extends Exception> action )
    {
        return barrier ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = mergeNode( tx );

                barrier.await();

                action.accept( node );

                tx.commit();
                return node;
            }
        };
    }

    private Node mergeNode( Transaction tx )
    {
        return (Node) single( tx.execute( "MERGE (foo:Foo{bar:'baz'}) RETURN foo" ) ).get( "foo" );
    }

    private void reassignProperties( Node node )
    {
        for ( Map.Entry<String,Object> property : node.getAllProperties().entrySet() )
        {
            node.setProperty( property.getKey(), property.getValue() );
        }
    }

    private void reassignLabels( Node node )
    {
        for ( Label label : node.getLabels() )
        {
            node.addLabel( label );
        }
    }
}
