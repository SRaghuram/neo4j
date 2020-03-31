/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.newapi;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.internal.kernel.api.Token;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.newapi.KernelAPIReadTestBase;
import org.neo4j.kernel.impl.newapi.KernelAPIReadTestSupport;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.RelationshipSelection;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

public abstract class ReadWithSecurityTestBase<G extends KernelAPIReadTestSupport> extends KernelAPIReadTestBase<G>
{
    private static long foo, bar, baz, barBaz;
    private static long fooAbar, fooBbar, barAfoo, fooCbar, fooAbaz;
    private static int fooLabel, barLabel, bazLabel;
    private static int aType, bType, cType;
    private static int prop1Key, prop2Key;
    private static AuthManager authManager;

    @Override
    public void createTestGraph( GraphDatabaseService graphDb )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            tx.schema().indexFor( label( "Bar" ) ).on( "prop1" ).withName( "barIndex" ).create();
            tx.schema().indexFor( label( "Baz" ) ).on( "prop1" ).withName( "bazIndex" ).create();
            tx.schema().indexFor( label( "Bar" ) ).on( "prop2" ).withName( "distinctBarIndex" ).create();
            tx.commit();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            // (foo :Foo), (bar :Bar), (baz: Baz), (barbaz :Bar:Baz)
            // (foo)-[:A]->(bar)
            // (foo)-[:B]->(bar)
            // (foo)<-[:A]-(bar)

            // (foo)-[:C]->(bar)
            // (foo)-[:A]->(baz)

            Node fooNode = tx.createNode( label( "Foo" ) );
            Node barNode = tx.createNode( label( "Bar" ) );
            Node bazNode = tx.createNode( label( "Baz" ) );
            Node barbazNode = tx.createNode( label( "Bar" ), label( "Baz" ) );

            fooNode.setProperty( "prop1", 1 );
            bazNode.setProperty( "prop1", 1 );
            barNode.setProperty( "prop1", 1 );
            barNode.setProperty( "prop2", 3 );
            barbazNode.setProperty( "prop1", 1 );
            barbazNode.setProperty( "prop2", 4 );

            // Traversable relationships
            fooAbar = fooNode.createRelationshipTo( barNode, RelationshipType.withName( "A" ) ).getId();
            fooBbar = fooNode.createRelationshipTo( barNode, RelationshipType.withName( "B" ) ).getId();
            barAfoo = barNode.createRelationshipTo( fooNode, RelationshipType.withName( "A" ) ).getId();

            // Not traversable relationship type
            fooCbar = fooNode.createRelationshipTo( barNode, RelationshipType.withName( "C" ) ).getId();

            // Not traversable end node
            fooAbaz = fooNode.createRelationshipTo( bazNode, RelationshipType.withName( "A" ) ).getId();

            foo = fooNode.getId();
            bar = barNode.getId();
            baz = bazNode.getId();
            barBaz = barbazNode.getId();

            tx.createNode().getId();

            tx.commit();
        }

        Kernel kernel = testSupport.kernelToTest();
        try ( KernelTransaction tx = kernel.beginTransaction( KernelTransaction.Type.IMPLICIT, LoginContext.AUTH_DISABLED ) )
        {
            Token token = tx.token();
            fooLabel = token.nodeLabel( "Foo" );
            barLabel = token.nodeLabel( "Bar" );
            bazLabel = token.nodeLabel( "Baz" );
            aType = token.relationshipType( "A" );
            bType = token.relationshipType( "B" );
            cType = token.relationshipType( "C" );
            prop1Key = token.propertyKey( "prop1" );
            prop2Key = token.propertyKey( "prop2" );
            tx.commit();
        }
        catch ( TransactionFailureException e )
        {
            fail( e );
        }
    }

    @Override
    public void createSystemGraph( GraphDatabaseService graphDb )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            tx.execute( "CREATE USER testUser SET PASSWORD 'abc123' CHANGE NOT REQUIRED" );
            tx.execute( "CREATE ROLE testRole" );
            tx.execute( "GRANT ROLE testRole TO testUser" );
            tx.execute( "GRANT ACCESS ON DATABASE * TO testRole" );
            tx.execute( "GRANT TRAVERSE ON GRAPH * NODES Foo, Bar TO testRole" );
            tx.execute( "GRANT TRAVERSE ON GRAPH * RELATIONSHIPS A, B TO testRole" );
            tx.execute( "DENY TRAVERSE ON GRAPH * NODES Baz TO testRole" );
            tx.execute( "GRANT READ {prop1,prop2} ON GRAPH * NODES Bar TO testRole" );

            tx.commit();
        }
        authManager = ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency( AuthManager.class );
    }

    // Tests for nodes

    @Test
    void allNodesScan() throws Throwable
    {
        // given
        changeUser( getLoginContext() );
        List<Long> ids = new ArrayList<>();
        try ( NodeCursor nodes = cursors.allocateNodeCursor( NULL ) )
        {
            // when
            read.allNodesScan( nodes );
            while ( nodes.next() )
            {
                ids.add( nodes.nodeReference() );
            }
        }

        // then
        assertThat( ids, containsInAnyOrder( foo, bar ) );
    }

    @Test
    void allNodesScan2() throws Throwable
    {
        // given
        changeUser( getLoginContext() );
        List<Long> ids = new ArrayList<>();
        try ( NodeCursor nodes = cursors.allocateNodeCursor( NULL ) )
        {
            // when
            Scan<NodeCursor> scan = read.allNodesScan();
            scan.reserveBatch( nodes, 5 );
            while ( nodes.next() )
            {
                ids.add( nodes.nodeReference() );
            }
        }

        // then
        assertThat( ids, containsInAnyOrder( foo, bar ) );
    }

    @Test
    void nodeLabelScan() throws Throwable
    {
        // given
        changeUser( getLoginContext() );
        List<Long> ids = new ArrayList<>();
        try ( NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor() )
        {
            // when
            read.nodeLabelScan( barLabel, nodes );
            while ( nodes.next() )
            {
                ids.add( nodes.nodeReference() );
            }
        }

        // then
        ArrayList<Long> expected = new ArrayList<>();
        expected.add( bar );
        assertEquals( expected, ids );
    }

    @Test
    void nodeLabelScan2() throws Throwable
    {
        // given
        changeUser( getLoginContext() );
        List<Long> ids = new ArrayList<>();
        try ( NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor() )
        {
            Scan<NodeLabelIndexCursor> scan = read.nodeLabelScan( barLabel );
            scan.reserveBatch( nodes, 5 );
            // when
            while ( nodes.next() )
            {
                ids.add( nodes.nodeReference() );
            }
        }

        // then
        ArrayList<Long> expected = new ArrayList<>();
        expected.add( bar );
        assertEquals( expected, ids );
    }

    @Test
    void nodesGetCount() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        // when
        long nodesCount = read.nodesGetCount();

        // then
        assertThat( nodesCount, equalTo( 2L ) );
    }

    @Test
    void countsForNode() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        // when
        long nodesCount = read.countsForNode( barLabel );

        // then
        assertThat( nodesCount, equalTo( 1L ) );
    }

    @Test
    void countsForNodeWithoutTxState() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        // when
        long nodesCount = read.countsForNode( barLabel );

        // then
        assertThat( nodesCount, equalTo( 1L ) );
    }

    @Test
    void nodeExists() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        // then
        assertTrue( read.nodeExists( foo ) );
        assertTrue( read.nodeExists( bar ) );
        assertFalse( read.nodeExists( baz ) );
        assertFalse( read.nodeExists( barBaz ) );
    }

    @Test
    void singleNode() throws Throwable
    {
        // given
        changeUser( getLoginContext() );
        List<Long> ids = new ArrayList<>();
        try ( NodeCursor nodes = cursors.allocateNodeCursor( NULL ) )
        {
            // when
            read.singleNode( foo, nodes );
            while ( nodes.next() )
            {
                ids.add( nodes.nodeReference() );
            }
        }

        // then
        assertThat( ids, containsInAnyOrder( foo ) );
    }

    @Test
    void singleNode2() throws Throwable
    {
        // given
        changeUser( getLoginContext() );
        List<Long> ids = new ArrayList<>();
        try ( NodeCursor nodes = cursors.allocateNodeCursor( NULL ) )
        {
            // when
            read.singleNode( barBaz, nodes );
            while ( nodes.next() )
            {
                ids.add( nodes.nodeReference() );
            }
        }

        // then
        assertThat( ids, equalTo( Collections.emptyList() ) );
    }

    @Test
    void nodeIndexSeek() throws Throwable
    {
        // given
        changeUser( getLoginContext() );
        List<Long> ids = new ArrayList<>();
        IndexDescriptor index = schemaRead.indexGetForName( "barIndex" );
        IndexReadSession indexSession = read.indexReadSession( index );
        try ( NodeValueIndexCursor cursor = cursors.allocateNodeValueIndexCursor() )
        {
            read.nodeIndexSeek( indexSession, cursor, IndexQueryConstraints.unconstrained(), IndexQuery.exact( prop1Key, 1 ) );
            while ( cursor.next() )
            {
                ids.add( cursor.nodeReference() );
            }
        }

        // then
        assertThat( ids, containsInAnyOrder( bar ) );
    }

    @Test
    void lockingNodeUniqueIndexSeek() throws Throwable
    {
        // given
        changeUser( getLoginContext() );
        List<Long> ids = new ArrayList<>();
        IndexDescriptor index = schemaRead.indexGetForName( "distinctBarIndex" );
        try ( NodeValueIndexCursor cursor1 = cursors.allocateNodeValueIndexCursor();
              NodeValueIndexCursor cursor2 = cursors.allocateNodeValueIndexCursor() )
        {
            ids.add( read.lockingNodeUniqueIndexSeek( index, cursor1, IndexQuery.exact( prop2Key, 3 ) ) );
            ids.add( read.lockingNodeUniqueIndexSeek( index, cursor2, IndexQuery.exact( prop2Key, 4 ) ) );
        }

        // then
        assertThat( ids, containsInAnyOrder( -1L, bar ) );
    }

    @Test
    void nodeIndexScan() throws Throwable
    {
        // given
        changeUser( getLoginContext() );
        List<Long> ids = new ArrayList<>();
        IndexDescriptor index = schemaRead.indexGetForName( "barIndex" );
        IndexReadSession indexSession = read.indexReadSession( index );
        try ( NodeValueIndexCursor cursor = cursors.allocateNodeValueIndexCursor() )
        {
            read.nodeIndexScan( indexSession, cursor, IndexQueryConstraints.unconstrained() );
            while ( cursor.next() )
            {
                ids.add( cursor.nodeReference() );
            }
        }

        // then
        assertThat( ids, containsInAnyOrder( bar ) );
    }

    // Tests for relationships

    @Test
    void allRelationshipsScan() throws Throwable
    {
        // given
        changeUser( getLoginContext() );
        List<Long> ids = new ArrayList<>();
        try ( RelationshipScanCursor rels = cursors.allocateRelationshipScanCursor( NULL ) )
        {
            // when
            read.allRelationshipsScan( rels );
            while ( rels.next() )
            {
                ids.add( rels.relationshipReference() );
            }
        }

        // then
        assertThat( ids, containsInAnyOrder( fooAbar, fooBbar, barAfoo ) );
    }

    @Test
    void allRelationshipsScan2() throws Throwable
    {
        // given
        changeUser( getLoginContext() );
        List<Long> ids = new ArrayList<>();
        try ( RelationshipScanCursor rels = cursors.allocateRelationshipScanCursor( NULL ) )
        {
            // when
            Scan<RelationshipScanCursor> scan = read.allRelationshipsScan();
            scan.reserveBatch( rels, 5 );
            while ( rels.next() )
            {
                ids.add( rels.relationshipReference() );
            }
        }

        // then
        assertThat( ids, containsInAnyOrder( fooAbar, fooBbar, barAfoo ) );
    }

    @Test
    void relationshipTypeScan() throws Throwable
    {
        // given
        changeUser( getLoginContext() );
        List<Long> ids = new ArrayList<>();
        try ( RelationshipScanCursor rels = cursors.allocateRelationshipScanCursor( NULL ) )
        {
            // when
            read.relationshipTypeScan( aType, rels );
            while ( rels.next() )
            {
                ids.add( rels.relationshipReference() );
            }
        }

        // then
        assertThat( ids, containsInAnyOrder( fooAbar, barAfoo ) );
    }

    @Test
    void countsForRelationshipWithStartNode() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        // when
        long relCount = read.countsForRelationship( fooLabel, ANY_RELATIONSHIP_TYPE, ANY_LABEL );

        // then
        assertThat( relCount, equalTo( 2L ) );
    }

    @Test
    void countsForRelationshipWithStartNodeAndType() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        // when
        long relCount = read.countsForRelationship( fooLabel, aType, ANY_LABEL );

        // then
        assertThat( relCount, equalTo( 1L ) );
    }

    @Test
    void countsForRelationshipWithEndNode() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        // when
        long relCount = read.countsForRelationship( ANY_LABEL, ANY_RELATIONSHIP_TYPE, fooLabel );

        // then
        assertThat( relCount, equalTo( 1L ) );
    }

    @Test
    void relationshipsGetCount() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        // when
        long relCount = read.relationshipsGetCount();

        // then
        assertThat( relCount, equalTo( 3L ) );
    }

    @Test
    void relationshipExists() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        // then
        assertTrue( read.relationshipExists( fooAbar ) );
        assertTrue( read.relationshipExists( fooBbar ) );
        assertTrue( read.relationshipExists( barAfoo ) );
        assertFalse( read.relationshipExists( fooCbar ) );
        assertFalse( read.relationshipExists( fooAbaz ) );
    }

    @Test
    void singleRelationship() throws Throwable
    {
        // given
        changeUser( getLoginContext() );
        List<Long> ids = new ArrayList<>();
        try ( RelationshipScanCursor scan = cursors.allocateRelationshipScanCursor( NULL ) )
        {
            // when
            read.singleRelationship( fooAbar, scan );
            while ( scan.next() )
            {
                ids.add( scan.relationshipReference() );
            }
        }

        // then
        assertThat( ids, containsInAnyOrder( fooAbar ) );
    }

    @Test
    void singleRelationship2() throws Throwable
    {
        // given
        changeUser( getLoginContext() );
        List<Long> ids = new ArrayList<>();
        try ( RelationshipScanCursor scan = cursors.allocateRelationshipScanCursor( NULL ) )
        {
            // when
            read.singleRelationship( fooAbaz, scan );
            while ( scan.next() )
            {
                ids.add( scan.relationshipReference() );
            }
        }

        // then
        assertThat( ids, equalTo( Collections.emptyList() ) );
    }

    @Test
    void singleRelationship3() throws Throwable
    {
        // given
        changeUser( getLoginContext() );
        List<Long> ids = new ArrayList<>();
        try ( RelationshipScanCursor scan = cursors.allocateRelationshipScanCursor( NULL ) )
        {
            // when
            read.singleRelationship( fooCbar, scan );
            while ( scan.next() )
            {
                ids.add( scan.relationshipReference() );
            }
        }

        // then
        assertThat( ids, equalTo( Collections.emptyList() ) );
    }

    // Node + relationships
    @Test
    void nodeAllRelationships() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        List<Long> ids = new ArrayList<>();
        try ( NodeCursor node = cursors.allocateNodeCursor( NULL );
              RelationshipTraversalCursor rels = cursors.allocateRelationshipTraversalCursor( NULL ) )
        {
            // when
            read.singleNode( foo, node );
            node.next();

            node.relationships( rels, RelationshipSelection.ALL_RELATIONSHIPS );
            while ( rels.next() )
            {
                ids.add( rels.relationshipReference() );
            }
        }

        // then
        assertThat( ids, containsInAnyOrder( fooAbar, fooBbar, barAfoo ) );
    }

    @Test
    void relationships() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        List<Long> ids = new ArrayList<>();
        try ( NodeCursor node = cursors.allocateNodeCursor( NULL );
              RelationshipTraversalCursor rels = cursors.allocateRelationshipTraversalCursor( NULL ) )
        {
            // when
            read.singleNode( foo, node );
            node.next();
            final long nodeReference = node.nodeReference();

            read.relationships( nodeReference, node.relationshipsReference(), RelationshipSelection.selection( Direction.OUTGOING ), rels );
            while ( rels.next() )
            {
                ids.add( rels.relationshipReference() );
            }
        }

        // then
        assertThat( ids, containsInAnyOrder( fooAbar, fooBbar ) );
    }

    private LoginContext getLoginContext() throws InvalidAuthTokenException
    {
        return authManager.login( Map.of( "principal", "testUser", "credentials", "abc123".getBytes( StandardCharsets.UTF_8 ), "scheme", "basic" ) );
    }
}
