/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cypher.internal;

import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.Test;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static java.util.Arrays.stream;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.cypher.operations.ExpandIntoCursors.connectingRelationships;
import static org.neo4j.cypher.operations.ExpandIntoCursors.nodeGetDegreeDense;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.kernel.api.KernelTransaction.Type.implicit;

@DbmsExtension( configurationCallback = "config" )
class ExpandIntoCursorsTest
{
    @Inject
    private GraphDatabaseAPI db;

    private static final int DENSE_THRESHOLD = 10;

    @ExtensionCallback
    void config( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( GraphDatabaseSettings.dense_node_threshold, DENSE_THRESHOLD );
    }

    private KernelTransaction transaction() throws TransactionFailureException
    {
        DependencyResolver resolver = db.getDependencyResolver();
        return resolver.resolveDependency( Kernel.class ).beginTransaction( implicit, LoginContext.AUTH_DISABLED );
    }

    @Test
    void shouldFindConnectingRelationshipBetweenTwoDenseNodesWhereStartNodeHasHigherDegree() throws KernelException
    {
        //given
        long start, end, r1, r2, r3;
        try ( KernelTransaction tx = transaction() )
        {
            start = nodeWithDegree( tx, 43 );
            end = nodeWithDegree( tx, 11 );
            r1 = relate( tx, start, "R1", end );
            r2 = relate( tx, start, "R2", end );
            r3 = relate( tx, end, "R3", start );
            tx.commit();
        }

        // Then
        assertThat( connections( start, OUTGOING, end ), equalTo( LongSets.immutable.of( r1, r2 ) ) );
        assertThat( connections( start, OUTGOING, end, "R1" ), equalTo( LongSets.immutable.of( r1 ) ) );
        assertThat( connections( start, INCOMING, end ), equalTo( LongSets.immutable.of( r3 ) ) );
        assertThat( connections( start, INCOMING, end, "R1" ), equalTo( LongSets.immutable.empty()) );
        assertThat( connections( start, BOTH, end ), equalTo( LongSets.immutable.of( r1, r2, r3 ) ) );
        assertThat( connections( start, BOTH, end, "R2", "R3" ), equalTo( LongSets.immutable.of(  r2, r3 ) ) );
    }

    @Test
    void shouldFindConnectingRelationshipBetweenTwoDenseNodesWhereEndNodeHasHigherDegree() throws KernelException
    {
        //given
        long start, end, r1, r2, r3;
        try ( KernelTransaction tx = transaction() )
        {
            start = nodeWithDegree( tx, 11 );
            end = nodeWithDegree( tx, 43 );
            r1 = relate( tx, start, "R1", end );
            r2 = relate( tx, start, "R2", end );
            r3 = relate( tx, end, "R3", start );
            tx.commit();
        }

        // Then
        assertThat( connections( start, OUTGOING, end ), equalTo( LongSets.immutable.of( r1, r2 ) ) );
        assertThat( connections( start, OUTGOING, end, "R1" ), equalTo( LongSets.immutable.of( r1 ) ) );
        assertThat( connections( start, INCOMING, end ), equalTo( LongSets.immutable.of( r3 ) ) );
        assertThat( connections( start, INCOMING, end, "R1" ), equalTo( LongSets.immutable.empty()) );
        assertThat( connections( start, BOTH, end ), equalTo( LongSets.immutable.of( r1, r2, r3 ) ) );
        assertThat( connections( start, BOTH, end, "R2", "R3" ), equalTo( LongSets.immutable.of(  r2, r3 ) ) );
    }

    @Test
    void shouldFindConnectingRelationshipBetweenSparseAndDenseNodes() throws KernelException
    {
        //given
        long start, end, r1, r2, r3;
        try ( KernelTransaction tx = transaction() )
        {
            start = nodeWithDegree( tx, 0 );
            end = nodeWithDegree( tx, 44 );
            r1 = relate( tx, start, "R1", end );
            r2 = relate( tx, start, "R2", end );
            r3 = relate( tx, end, "R3", start );
            tx.commit();
        }

        // Then
        assertThat( connections( start, OUTGOING, end ), equalTo( LongSets.immutable.of( r1, r2 ) ) );
        assertThat( connections( start, OUTGOING, end, "R1" ), equalTo( LongSets.immutable.of( r1 ) ) );
        assertThat( connections( start, INCOMING, end ), equalTo( LongSets.immutable.of( r3 ) ) );
        assertThat( connections( start, INCOMING, end, "R1" ), equalTo( LongSets.immutable.empty()) );
        assertThat( connections( start, BOTH, end ), equalTo( LongSets.immutable.of( r1, r2, r3 ) ) );
        assertThat( connections( start, BOTH, end, "R2", "R3" ), equalTo( LongSets.immutable.of(  r2, r3 ) ) );
    }

    @Test
    void shouldFindConnectingRelationshipBetweenDenseAndSparseNodes() throws KernelException
    {
        //given
        long start, end, r1, r2, r3;
        try ( KernelTransaction tx = transaction() )
        {
            start = nodeWithDegree( tx, 56 );
            end = nodeWithDegree( tx, 0 );
            r1 = relate( tx, start, "R1", end );
            r2 = relate( tx, start, "R2", end );
            r3 = relate( tx, end, "R3", start );
            tx.commit();
        }

        // Then
        assertThat( connections( start, OUTGOING, end ), equalTo( LongSets.immutable.of( r1, r2 ) ) );
        assertThat( connections( start, OUTGOING, end, "R1" ), equalTo( LongSets.immutable.of( r1 ) ) );
        assertThat( connections( start, INCOMING, end ), equalTo( LongSets.immutable.of( r3 ) ) );
        assertThat( connections( start, INCOMING, end, "R1" ), equalTo( LongSets.immutable.empty()) );
        assertThat( connections( start, BOTH, end ), equalTo( LongSets.immutable.of( r1, r2, r3 ) ) );
        assertThat( connections( start, BOTH, end, "R2", "R3" ), equalTo( LongSets.immutable.of(  r2, r3 ) ) );
    }

    @Test
    void shouldFindConnectingRelationshipBetweenTwoSparseNodes() throws KernelException
    {
        //given
        long start, end, r1, r2, r3;
        try ( KernelTransaction tx = transaction() )
        {
            start = nodeWithDegree( tx, 0 );
            end = nodeWithDegree( tx, 0 );
            r1 = relate( tx, start, "R1", end );
            r2 = relate( tx, start, "R2", end );
            r3 = relate( tx, end, "R3", start );
            tx.commit();
        }

        // Then
        assertThat( connections( start, OUTGOING, end ), equalTo( LongSets.immutable.of( r1, r2 ) ) );
        assertThat( connections( start, OUTGOING, end, "R1" ), equalTo( LongSets.immutable.of( r1 ) ) );
        assertThat( connections( start, INCOMING, end ), equalTo( LongSets.immutable.of( r3 ) ) );
        assertThat( connections( start, INCOMING, end, "R1" ), equalTo( LongSets.immutable.empty()) );
        assertThat( connections( start, BOTH, end ), equalTo( LongSets.immutable.of( r1, r2, r3 ) ) );
        assertThat( connections( start, BOTH, end, "R2", "R3" ), equalTo( LongSets.immutable.of(  r2, r3 ) ) );
    }

    @Test
    void shouldComputeDegreeWithoutType() throws Exception
    {
        // GIVEN
        long node;
        try ( KernelTransaction tx = transaction() )
        {
            Write write = tx.dataWrite();
            node = nodeWithDegree( tx, 42 );
            relate( tx, node, "R1", write.nodeCreate() );
            relate( tx, node, "R2", write.nodeCreate() );
            relate( tx, write.nodeCreate(), "R3", node );
            relate( tx, node, "R4", node );

            tx.commit();
        }

        try ( KernelTransaction tx = transaction() )
        {
            Read read = tx.dataRead();
            CursorFactory cursors = tx.cursors();
            try ( NodeCursor nodes = cursors.allocateNodeCursor();
                  RelationshipGroupCursor groupCursor = cursors.allocateRelationshipGroupCursor() )
            {
                read.singleNode( node, nodes );
                assertThat( nodes.next(), equalTo( true ) );
                assertThat( nodes.isDense(), equalTo( true ) );
                assertThat( nodeGetDegreeDense( nodes, groupCursor, OUTGOING ), equalTo( 45 ) );
                assertThat( nodeGetDegreeDense( nodes, groupCursor, INCOMING ), equalTo( 2 ) );
                assertThat( nodeGetDegreeDense( nodes, groupCursor, BOTH ), equalTo( 46 ) );
            }
        }
    }

    @Test
    void shouldComputeDegreeWithType() throws Exception
    {
        // GIVEN
        long node;
        int in, out, loop;
        try ( KernelTransaction tx = transaction() )
        {
            Write write = tx.dataWrite();
            node = denseNode( tx );
            TokenWrite tokenWrite = tx.tokenWrite();
            out = tokenWrite.relationshipTypeGetOrCreateForName( "OUT" );
            in = tokenWrite.relationshipTypeGetOrCreateForName( "IN" );
            loop = tokenWrite.relationshipTypeGetOrCreateForName( "LOOP" );
            write.relationshipCreate( node, out, write.nodeCreate() );
            write.relationshipCreate( node, out, write.nodeCreate() );
            write.relationshipCreate( write.nodeCreate(), in, node );
            write.relationshipCreate( node, loop, node );

            tx.commit();
        }

        try ( KernelTransaction tx = transaction() )
        {
            Read read = tx.dataRead();
            CursorFactory cursors = tx.cursors();
            try ( NodeCursor nodes = cursors.allocateNodeCursor();
                  RelationshipGroupCursor groupCursor = cursors.allocateRelationshipGroupCursor() )
            {
                read.singleNode( node, nodes );
                assertThat( nodes.next(), equalTo( true ) );
                assertThat( nodes.isDense(), equalTo( true ) );
                assertThat( nodeGetDegreeDense( nodes, groupCursor, OUTGOING, out ), equalTo( 2 ) );
                assertThat( nodeGetDegreeDense( nodes, groupCursor, OUTGOING, in ), equalTo( 0 ) );
                assertThat( nodeGetDegreeDense( nodes, groupCursor, OUTGOING, loop ), equalTo( 1 ) );

                assertThat( nodeGetDegreeDense( nodes, groupCursor, INCOMING, out ), equalTo( 0 ) );
                assertThat( nodeGetDegreeDense( nodes, groupCursor, INCOMING, in ), equalTo( 1 ) );
                assertThat( nodeGetDegreeDense( nodes, groupCursor, INCOMING, loop ), equalTo( 1 ) );

                assertThat( nodeGetDegreeDense( nodes, groupCursor, BOTH, out ), equalTo( 2 ) );
                assertThat( nodeGetDegreeDense( nodes, groupCursor, BOTH, in ), equalTo( 1 ) );
                assertThat( nodeGetDegreeDense( nodes, groupCursor, BOTH, loop ), equalTo( 1 ) );
            }
        }
    }

    private LongSet connections( long start, Direction direction, long end, String...types )
            throws TransactionFailureException
    {
        try ( KernelTransaction tx = transaction();
              NodeCursor nodeCursor = tx.cursors().allocateNodeCursor();
              RelationshipGroupCursor groupCursor = tx.cursors().allocateRelationshipGroupCursor();
              RelationshipTraversalCursor traversalCursor = tx.cursors().allocateRelationshipTraversalCursor() )
        {
            int[] typeIds = types.length == 0 ? null : stream( types ).mapToInt( tx.tokenRead()::relationshipType ).toArray( );
            RelationshipSelectionCursor connections =
                    connectingRelationships(
                            tx.dataRead(),
                            nodeCursor, groupCursor,
                            traversalCursor,
                            start,
                            direction,
                            end,
                            typeIds );
            MutableLongSet rels = LongSets.mutable.empty();
            while ( connections.next() )
            {
                rels.add( connections.relationshipReference() );
            }
            return rels;
        }
    }

    private long denseNode( KernelTransaction tx ) throws KernelException
    {
        return nodeWithDegree( tx, DENSE_THRESHOLD + 1 );
    }

    private long relate( KernelTransaction tx, long start, String rel, long end ) throws KernelException
    {
        return tx.dataWrite().relationshipCreate( start,
                tx.tokenWrite().relationshipTypeGetOrCreateForName( rel ), end );
    }

    private long nodeWithDegree( KernelTransaction tx, int degree ) throws KernelException
    {
        Write write = tx.dataWrite();
        long node = write.nodeCreate();
        for ( int i = 0; i < degree; i++ )
        {
            relate( tx, node, "JUNK", write.nodeCreate() );
        }
        return node;
    }
}
