/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.newapi;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.internal.kernel.api.Token;
import org.neo4j.internal.kernel.api.TokenRead;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.newapi.AllStoreHolder.NO_ID;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

public abstract class ReadWithSecurityTestBase<G extends KernelAPIReadTestSupport> extends KernelAPIReadTestBase<G>
{
    protected long foo, bar, baz, barBaz;
    protected long fooAbar, fooBbar, barAfoo, fooCbar, fooAbaz;
    protected int fooLabel, barLabel, bazLabel;
    protected int aType, bType, cType;
    protected int prop1Key, prop2Key;
    protected AuthManager authManager;

    @Override
    public void createTestGraph( GraphDatabaseService graphDb )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            tx.schema().indexFor( label( "Bar" ) ).on( "prop1" ).withName( "barIndex" ).create();
            tx.schema().indexFor( label( "Baz" ) ).on( "prop1" ).withName( "bazIndex" ).create();
            tx.schema().constraintFor( label( "Bar" ) ).assertPropertyIsUnique( "prop2" ).withName( "distinctBarIndex" ).create();
            tx.schema().indexFor( RelationshipType.withName( "A" ) ).on( "prop1" ).withName( "aIndex" ).create();
            tx.schema().indexFor( RelationshipType.withName( "C" ) ).on( "prop1" ).withName( "cIndex" ).create();
            tx.schema().indexFor( RelationshipType.withName( "A" ) ).on( "prop2" ).withName( "ap2Index" ).create();
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
            Relationship fooAbarRel = fooNode.createRelationshipTo( barNode, RelationshipType.withName( "A" ) );
            Relationship fooBbarRel = fooNode.createRelationshipTo( barNode, RelationshipType.withName( "B" ) );
            Relationship barAfooRel = barNode.createRelationshipTo( fooNode, RelationshipType.withName( "A" ) );

            // Not traversable relationship type
            Relationship fooCbarRel = fooNode.createRelationshipTo( barNode, RelationshipType.withName( "C" ) );

            // Not traversable end node
            Relationship fooAbazRel = fooNode.createRelationshipTo( bazNode, RelationshipType.withName( "A" ) );

            fooAbarRel.setProperty( "prop1", 1 );
            fooAbarRel.setProperty( "prop2", 3 );
            barAfooRel.setProperty( "prop1", 1 );
            barAfooRel.setProperty( "prop2", 4 );
            fooCbarRel.setProperty( "prop1", 1 );
            fooAbazRel.setProperty( "prop1", 1 );

            fooAbar = fooAbarRel.getId();
            fooBbar = fooBbarRel.getId();
            barAfoo = barAfooRel.getId();
            fooCbar = fooCbarRel.getId();
            fooAbaz = fooAbazRel.getId();

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
            tx.execute( "GRANT READ {prop1} ON GRAPH * RELATIONSHIPS A TO testRole" );
            tx.execute( "GRANT CONSTRAINT MANAGEMENT ON DATABASE * TO testRole" );

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
        assertThat( ids ).containsExactlyInAnyOrder( foo, bar );
    }

    @Test
    void allNodesScanWrappedInScan() throws Throwable
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
        assertThat( ids ).containsExactlyInAnyOrder( foo, bar );
    }

    @Test
    void nodeLabelScan() throws Throwable
    {
        // given
        changeUser( getLoginContext() );
        List<Long> ids = new ArrayList<>();
        try ( NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor( NULL ) )
        {
            // when
            read.nodeLabelScan( barLabel, nodes, IndexOrder.NONE );
            while ( nodes.next() )
            {
                ids.add( nodes.nodeReference() );
            }
        }

        // then
        assertThat( ids ).containsExactlyInAnyOrder( bar );
    }

    @Test
    void nodeLabelScanWrappedInScan() throws Throwable
    {
        // given
        changeUser( getLoginContext() );
        List<Long> ids = new ArrayList<>();
        try ( NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor( NULL ) )
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
        assertThat( ids ).containsExactlyInAnyOrder( bar );
    }

    @Test
    void nodesGetCount() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        // when
        long nodesCount = read.nodesGetCount();

        // then
        assertThat( nodesCount ).isEqualTo( 2L );
    }

    @Test
    void countsForNodeAllLabels() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        // when
        long nodesCount = read.countsForNode( TokenRead.ANY_LABEL );

        // then
        assertThat( nodesCount ).isEqualTo( 2L );
    }

    @Test
    void countsForNodeSpecificLabel() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        // when
        long nodesCount = read.countsForNode( barLabel );

        // then
        assertThat( nodesCount ).isEqualTo( 1L );
    }

    @Test
    void countsForNodeWithoutTxStateAllLabels() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        // when
        long nodesCount = read.countsForNodeWithoutTxState( TokenRead.ANY_LABEL );

        // then
        assertThat( nodesCount ).isEqualTo( 2L );
    }

    @Test
    void countsForNodeWithoutTxStateSpecificLabel() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        // when
        long nodesCount = read.countsForNodeWithoutTxState( barLabel );

        // then
        assertThat( nodesCount ).isEqualTo( 1L );
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
    void singleNodeAllowedLabel() throws Throwable
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
        assertThat( ids ).containsExactlyInAnyOrder( foo );
    }

    @Test
    void singleNodeDeniedLabel() throws Throwable
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
        assertThat( ids ).isEmpty();
    }

    @Test
    void nodeIndexSeek() throws Throwable
    {
        // given
        changeUser( getLoginContext() );
        List<Long> ids = new ArrayList<>();
        IndexDescriptor index = schemaRead.indexGetForName( "barIndex" );
        IndexReadSession indexSession = read.indexReadSession( index );
        try ( NodeValueIndexCursor cursor = cursors.allocateNodeValueIndexCursor( NULL, tx.memoryTracker() ) )
        {
            read.nodeIndexSeek( indexSession, cursor, IndexQueryConstraints.unconstrained(), PropertyIndexQuery.exact( prop1Key, 1 ) );
            while ( cursor.next() )
            {
                ids.add( cursor.nodeReference() );
            }
        }

        // then
        assertThat( ids ).containsExactlyInAnyOrder( bar );
    }

    @Test
    void lockingNodeUniqueIndexSeek() throws Throwable
    {
        // given
        changeUser( getLoginContext() );
        List<Long> ids = new ArrayList<>();
        IndexDescriptor index = schemaRead.indexGetForName( "distinctBarIndex" );
        try ( NodeValueIndexCursor cursor1 = cursors.allocateNodeValueIndexCursor( NULL, tx.memoryTracker() );
              NodeValueIndexCursor cursor2 = cursors.allocateNodeValueIndexCursor( NULL, tx.memoryTracker() ) )
        {
            ids.add( read.lockingNodeUniqueIndexSeek( index, cursor1, PropertyIndexQuery.exact( prop2Key, 3 ) ) );
            ids.add( read.lockingNodeUniqueIndexSeek( index, cursor2, PropertyIndexQuery.exact( prop2Key, 4 ) ) );
        }

        // then
        assertThat( ids ).containsExactlyInAnyOrder( NO_ID, bar );
    }

    @Test
    void nodeIndexScan() throws Throwable
    {
        // given
        changeUser( getLoginContext() );
        List<Long> ids = new ArrayList<>();
        IndexDescriptor index = schemaRead.indexGetForName( "barIndex" );
        IndexReadSession indexSession = read.indexReadSession( index );
        try ( NodeValueIndexCursor cursor = cursors.allocateNodeValueIndexCursor( NULL, tx.memoryTracker() ) )
        {
            read.nodeIndexScan( indexSession, cursor, IndexQueryConstraints.unconstrained() );
            while ( cursor.next() )
            {
                ids.add( cursor.nodeReference() );
            }
        }

        // then
        assertThat( ids ).containsExactlyInAnyOrder( bar );
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
        assertThat( ids ).containsExactlyInAnyOrder( fooAbar, fooBbar, barAfoo );
    }

    @Test
    void allRelationshipsScanWrappedInScan() throws Throwable
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
        assertThat( ids ).containsExactlyInAnyOrder( fooAbar, fooBbar, barAfoo );
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
        assertThat( ids ).containsExactlyInAnyOrder( fooAbar, barAfoo );
    }

    @Test
    void countsForRelationshipWithStartNode() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        // when
        long relCount = read.countsForRelationship( fooLabel, ANY_RELATIONSHIP_TYPE, ANY_LABEL );

        // then
        assertThat( relCount ).isEqualTo( 2L );
    }

    @Test
    void countsForRelationshipWithType() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        // when
        long relCount = read.countsForRelationship( ANY_LABEL, aType, ANY_LABEL );

        // then
        assertThat( relCount ).isEqualTo( 2L );
    }

    @Test
    void countsForRelationshipWithStartNodeAndType() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        // when
        long relCount = read.countsForRelationship( fooLabel, aType, ANY_LABEL );

        // then
        assertThat( relCount ).isEqualTo( 1L );
    }

    @Test
    void countsForRelationshipWithEndNode() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        // when
        long relCount = read.countsForRelationship( ANY_LABEL, ANY_RELATIONSHIP_TYPE, fooLabel );

        // then
        assertThat( relCount ).isEqualTo( 1L );
    }

    @Test
    void relationshipsGetCount() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        // when
        long relCount = read.relationshipsGetCount();

        // then
        assertThat( relCount ).isEqualTo( 3L );
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
    void singleRelationshipAllowed() throws Throwable
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
        assertThat( ids ).containsExactlyInAnyOrder( fooAbar );
    }

    @Test
    void singleRelationshipDeniedEndNode() throws Throwable
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
        assertThat( ids ).isEmpty();
    }

    @Test
    void singleRelationshipNotAllowedType() throws Throwable
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
        assertThat( ids ).isEmpty();
    }

    // Node + relationships
    @Test
    void nodeAllRelationshipsTraversal() throws Throwable
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
        assertThat( ids ).containsExactlyInAnyOrder( fooAbar, fooBbar, barAfoo );
    }

    @Test
    void relationshipsTraversal() throws Throwable
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
        assertThat( ids ).containsExactlyInAnyOrder( fooAbar, fooBbar );
    }

    @Test
    void relationshipIndexScan() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        IndexDescriptor index = schemaRead.indexGetForName( "aIndex" );
        IndexReadSession indexSession = read.indexReadSession( index );

        List<Long> ids = new ArrayList<>();
        try ( RelationshipValueIndexCursor rels = cursors.allocateRelationshipValueIndexCursor( NULL, tx.memoryTracker() ) )
        {
            // when
            read.relationshipIndexScan( indexSession, rels, IndexQueryConstraints.unconstrained() );
            while ( rels.next() )
            {
                ids.add( rels.relationshipReference() );
            }
        }

        // then
        assertThat( ids ).containsExactlyInAnyOrder( fooAbar, barAfoo );
    }

    @Test
    void relationshipIndexScanDeniedType() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        IndexDescriptor index = schemaRead.indexGetForName( "cIndex" );
        IndexReadSession indexSession = read.indexReadSession( index );

        List<Long> ids = new ArrayList<>();
        try ( RelationshipValueIndexCursor rels = cursors.allocateRelationshipValueIndexCursor( NULL, tx.memoryTracker() ) )
        {
            // when
            read.relationshipIndexScan( indexSession, rels, IndexQueryConstraints.unconstrained() );
            while ( rels.next() )
            {
                ids.add( rels.relationshipReference() );
            }
        }

        // then
        assertThat( ids ).isEmpty();
    }

    @Test
    void relationshipIndexScanDeniedProperty() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        IndexDescriptor index = schemaRead.indexGetForName( "ap2Index" );
        IndexReadSession indexSession = read.indexReadSession( index );

        List<Long> ids = new ArrayList<>();
        try ( RelationshipValueIndexCursor rels = cursors.allocateRelationshipValueIndexCursor( NULL, tx.memoryTracker() ) )
        {
            // when
            read.relationshipIndexScan( indexSession, rels, IndexQueryConstraints.unconstrained() );
            while ( rels.next() )
            {
                ids.add( rels.relationshipReference() );
            }
        }

        // then
        // not allowed to read prop2 on rel A
        assertThat( ids ).isEmpty();
    }

    protected LoginContext getLoginContext() throws InvalidAuthTokenException
    {
        return authManager.login( Map.of( "principal", "testUser", "credentials", "abc123".getBytes( StandardCharsets.UTF_8 ), "scheme", "basic" ) );
    }
}
