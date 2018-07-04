/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene;

import com.neo4j.kernel.api.impl.fulltext.FulltextIndexProviderFactory;
import org.junit.Test;

import java.util.Optional;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;

import static org.neo4j.storageengine.api.EntityType.NODE;
import static org.neo4j.storageengine.api.EntityType.RELATIONSHIP;

public class LuceneFulltextIndexTest extends LuceneFulltextTestSupport
{
    private static final String NODE_INDEX_NAME = "nodes";
    private static final String REL_INDEX_NAME = "rels";

    @Test
    public void shouldFindNodeWithString() throws Exception
    {
        IndexReference index;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            SchemaDescriptor descriptor = fulltextAdapter.schemaFor( NODE, new String[0], Optional.empty(), PROP );
            index = tx.schemaWrite().indexCreate( descriptor, Optional.of( FulltextIndexProviderFactory.DESCRIPTOR.name() ),
                    Optional.of( NODE_INDEX_NAME ) );
            tx.success();
        }
        await( index );
        long firstID;
        long secondID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( "Hello. Hello again." );
            secondID = createNodeIndexableByPropertyValue(
                    "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                    "cross between a zebra and any other equine: essentially, a zebra hybrid." );

            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            assertQueryFindsIds( NODE_INDEX_NAME, "hello", firstID );
            assertQueryFindsIds( NODE_INDEX_NAME, "zebra", secondID );
            assertQueryFindsIds( NODE_INDEX_NAME, "zedonk", secondID );
            assertQueryFindsIds( NODE_INDEX_NAME, "cross", secondID );
        }
    }

    @Test
    public void shouldFindNodeWithNumber() throws Exception
    {
        IndexReference index;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            SchemaDescriptor descriptor = fulltextAdapter.schemaFor( NODE, new String[0], Optional.empty(), PROP );
            index = tx.schemaWrite().indexCreate( descriptor, Optional.of( FulltextIndexProviderFactory.DESCRIPTOR.name() ),
                    Optional.of( NODE_INDEX_NAME ) );
            tx.success();
        }
        await( index );

        long firstID;
        long secondID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( 1 );
            secondID = createNodeIndexableByPropertyValue( 234 );

            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            assertQueryFindsIds( NODE_INDEX_NAME, "1", firstID );
            assertQueryFindsIds( NODE_INDEX_NAME, "234", secondID );
        }
    }

    @Test
    public void shouldFindNodeWithBoolean() throws Exception
    {
        IndexReference index;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            SchemaDescriptor descriptor = fulltextAdapter.schemaFor( NODE, new String[0], Optional.empty(), PROP );
            index = tx.schemaWrite().indexCreate( descriptor, Optional.of( FulltextIndexProviderFactory.DESCRIPTOR.name() ),
                    Optional.of( NODE_INDEX_NAME ) );
            tx.success();
        }
        await( index );

        long firstID;
        long secondID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( true );
            secondID = createNodeIndexableByPropertyValue( false );

            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            assertQueryFindsIds( NODE_INDEX_NAME, "true", firstID );
            assertQueryFindsIds( NODE_INDEX_NAME, "false", secondID );
        }
    }

    @Test
    public void shouldFindNodeWithArrays() throws Exception
    {
        IndexReference index;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            SchemaDescriptor descriptor = fulltextAdapter.schemaFor( NODE, new String[0], Optional.empty(), PROP );
            index = tx.schemaWrite().indexCreate( descriptor, Optional.of( FulltextIndexProviderFactory.DESCRIPTOR.name() ),
                    Optional.of( NODE_INDEX_NAME ) );
            tx.success();
        }
        await( index );

        long firstID;
        long secondID;
        long thirdID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( new String[]{"hello", "I", "live", "here"} );
            secondID = createNodeIndexableByPropertyValue( new int[]{1, 27, 48} );
            thirdID = createNodeIndexableByPropertyValue( new int[]{1, 2, 48} );

            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            assertQueryFindsIds( NODE_INDEX_NAME, "live", firstID );
            assertQueryFindsIds( NODE_INDEX_NAME, "27", secondID );
            assertQueryFindsIds( NODE_INDEX_NAME, "1 2", secondID, thirdID );
        }
    }

    @Test
    public void shouldRepresentPropertyChanges() throws Exception
    {
        IndexReference index;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            SchemaDescriptor descriptor = fulltextAdapter.schemaFor( NODE, new String[0], Optional.empty(), PROP );
            index = tx.schemaWrite().indexCreate( descriptor, Optional.of( FulltextIndexProviderFactory.DESCRIPTOR.name() ),
                    Optional.of( NODE_INDEX_NAME ) );
            tx.success();
        }
        await( index );

        long firstID;
        long secondID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( "Hello. Hello again." );
            secondID = createNodeIndexableByPropertyValue(
                    "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                    "cross between a zebra and any other equine: essentially, a zebra hybrid." );

            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            setNodeProp( firstID, "Finally! Potato!" );
            setNodeProp( secondID, "This one is a potato farmer." );

            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            assertQueryFindsNothing( NODE_INDEX_NAME, "hello" );
            assertQueryFindsNothing( NODE_INDEX_NAME, "zebra" );
            assertQueryFindsNothing( NODE_INDEX_NAME, "zedonk" );
            assertQueryFindsNothing( NODE_INDEX_NAME, "cross" );
            assertQueryFindsIds( NODE_INDEX_NAME, "finally", firstID );
            assertQueryFindsIds( NODE_INDEX_NAME, "farmer", secondID );
            assertQueryFindsIds( NODE_INDEX_NAME, "potato", firstID, secondID );
        }
    }

    @Test
    public void shouldNotFindRemovedNodes() throws Exception
    {
        IndexReference index;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            SchemaDescriptor descriptor = fulltextAdapter.schemaFor( NODE, new String[0], Optional.empty(), PROP );
            index = tx.schemaWrite().indexCreate( descriptor, Optional.of( FulltextIndexProviderFactory.DESCRIPTOR.name() ),
                    Optional.of( NODE_INDEX_NAME ) );
            tx.success();
        }
        await( index );

        long firstID;
        long secondID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( "Hello. Hello again." );
            secondID = createNodeIndexableByPropertyValue(
                    "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                    "cross between a zebra and any other equine: essentially, a zebra hybrid." );

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.getNodeById( firstID ).delete();
            db.getNodeById( secondID ).delete();

            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            assertQueryFindsNothing( NODE_INDEX_NAME, "hello" );
            assertQueryFindsNothing( NODE_INDEX_NAME, "zebra" );
            assertQueryFindsNothing( NODE_INDEX_NAME, "zedonk" );
            assertQueryFindsNothing( NODE_INDEX_NAME, "cross" );
        }
    }

    @Test
    public void shouldNotFindRemovedProperties() throws Exception
    {
        IndexReference index;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            SchemaDescriptor descriptor = fulltextAdapter.schemaFor( NODE, new String[0], Optional.empty(), "prop", "prop2" );
            index = tx.schemaWrite().indexCreate( descriptor, Optional.of( FulltextIndexProviderFactory.DESCRIPTOR.name() ),
                    Optional.of( NODE_INDEX_NAME ) );
            tx.success();
        }
        await( index );
        long firstID;
        long secondID;
        long thirdID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( "Hello. Hello again." );
            secondID = createNodeIndexableByPropertyValue(
                    "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                    "cross between a zebra and any other equine: essentially, a zebra hybrid." );
            thirdID = createNodeIndexableByPropertyValue( "Hello. Hello again." );

            setNodeProp( firstID, "zebra" );
            setNodeProp( secondID, "Hello. Hello again." );

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.getNodeById( firstID );
            Node node2 = db.getNodeById( secondID );
            Node node3 = db.getNodeById( thirdID );

            node.setProperty( "prop", "tomtar" );
            node.setProperty( "prop2", "tomtar" );

            node2.setProperty( "prop", "tomtar" );
            node2.setProperty( "prop2", "Hello" );

            node3.removeProperty( "prop" );

            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            assertQueryFindsIds( NODE_INDEX_NAME, "hello", secondID );
            assertQueryFindsNothing( NODE_INDEX_NAME, "zebra" );
            assertQueryFindsNothing( NODE_INDEX_NAME, "zedonk" );
            assertQueryFindsNothing( NODE_INDEX_NAME, "cross" );
        }
    }

    @Test
    public void shouldOnlyIndexIndexedProperties() throws Exception
    {
        IndexReference index;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            SchemaDescriptor descriptor = fulltextAdapter.schemaFor( NODE, new String[0], Optional.empty(), PROP );
            index = tx.schemaWrite().indexCreate( descriptor, Optional.of( FulltextIndexProviderFactory.DESCRIPTOR.name() ),
                    Optional.of( NODE_INDEX_NAME ) );
            tx.success();
        }
        await( index );

        long firstID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( "Hello. Hello again." );
            setNodeProp( firstID, "prop2", "zebra" );

            Node node2 = db.createNode();
            node2.setProperty( "prop2", "zebra" );
            node2.setProperty( "prop3", "hello" );

            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            assertQueryFindsIds( NODE_INDEX_NAME, "hello", firstID );
            assertQueryFindsNothing( NODE_INDEX_NAME, "zebra" );
        }
    }

    @Test
    public void shouldSearchAcrossMultipleProperties() throws Exception
    {
        IndexReference index;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            SchemaDescriptor descriptor = fulltextAdapter.schemaFor( NODE, new String[0], Optional.empty(), "prop", "prop2" );
            index = tx.schemaWrite().indexCreate( descriptor, Optional.of( FulltextIndexProviderFactory.DESCRIPTOR.name() ),
                    Optional.of( NODE_INDEX_NAME ) );
            tx.success();
        }
        await( index );

        long firstID;
        long secondID;
        long thirdID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( "Tomtar tomtar oftsat i tomteutstyrsel." );
            secondID = createNodeIndexableByPropertyValue( "Olof och Hans" );
            setNodeProp( secondID, "prop2", "och karl" );

            Node node3 = db.createNode();
            thirdID = node3.getId();
            node3.setProperty( "prop2", "Tomtar som inte tomtar ser upp till tomtar som tomtar." );

            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            assertQueryFindsIds( NODE_INDEX_NAME, "tomtar Karl", firstID, secondID, thirdID );
        }
    }

    @Test
    public void shouldOrderResultsBasedOnRelevance() throws Exception
    {
        IndexReference index;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            SchemaDescriptor descriptor = fulltextAdapter.schemaFor( NODE, new String[0], Optional.empty(), "first", "last" );
            index = tx.schemaWrite().indexCreate( descriptor, Optional.of( FulltextIndexProviderFactory.DESCRIPTOR.name() ),
                    Optional.of( NODE_INDEX_NAME ) );
            tx.success();
        }
        await( index );
        long firstID;
        long secondID;
        long thirdID;
        long fourthID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = db.createNode().getId();
            secondID = db.createNode().getId();
            thirdID = db.createNode().getId();
            fourthID = db.createNode().getId();
            setNodeProp( firstID, "first", "Full" );
            setNodeProp( firstID, "last", "Hanks" );
            setNodeProp( secondID, "first", "Tom" );
            setNodeProp( secondID, "last", "Hunk" );
            setNodeProp( thirdID, "first", "Tom" );
            setNodeProp( thirdID, "last", "Hanks" );
            setNodeProp( fourthID, "first", "Tom Hanks" );
            setNodeProp( fourthID, "last", "Tom Hanks" );

            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            assertQueryFindsIdsInOrder( NODE_INDEX_NAME, "Tom Hanks", fourthID, thirdID, firstID, secondID );
        }
    }

    @Test
    public void shouldDifferentiateNodesAndRelationships() throws Exception
    {
        SchemaDescriptor nodes = fulltextAdapter.schemaFor( NODE, new String[0], Optional.empty(), PROP );
        SchemaDescriptor rels = fulltextAdapter.schemaFor( RELATIONSHIP, new String[0], Optional.empty(), PROP );
        IndexReference nodesIndex;
        IndexReference relsIndex;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            nodesIndex = tx.schemaWrite().indexCreate( nodes, Optional.of( FulltextIndexProviderFactory.DESCRIPTOR.name() ),
                    Optional.of( NODE_INDEX_NAME ) );
            relsIndex = tx.schemaWrite().indexCreate( rels, Optional.of( FulltextIndexProviderFactory.DESCRIPTOR.name() ),
                    Optional.of( REL_INDEX_NAME ) );
            tx.success();
        }
        await( nodesIndex );
        await( relsIndex );
        long firstNodeID;
        long secondNodeID;
        long firstRelID;
        long secondRelID;
        try ( Transaction tx = db.beginTx() )
        {
            firstNodeID = createNodeIndexableByPropertyValue( "Hello. Hello again." );
            secondNodeID = createNodeIndexableByPropertyValue(
                    "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                    "cross between a zebra and any other equine: essentially, a zebra hybrid." );
            firstRelID = createRelationshipIndexableByPropertyValue( firstNodeID, secondNodeID, "Hello. Hello again." );
            secondRelID = createRelationshipIndexableByPropertyValue( secondNodeID, firstNodeID, "And now, something completely different" );

            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            assertQueryFindsIds( NODE_INDEX_NAME, "hello", firstNodeID );
            assertQueryFindsIds( NODE_INDEX_NAME, "zebra", secondNodeID );
            assertQueryFindsNothing( NODE_INDEX_NAME, "different" );

            assertQueryFindsIds( REL_INDEX_NAME, "hello", firstRelID );
            assertQueryFindsNothing( REL_INDEX_NAME, "zebra" );
            assertQueryFindsIds( REL_INDEX_NAME, "different", secondRelID );
        }
    }

    @Test
    public void shouldNotReturnNonMatches() throws Exception
    {
        SchemaDescriptor nodes = fulltextAdapter.schemaFor( NODE, new String[0], Optional.empty(), PROP );
        SchemaDescriptor rels = fulltextAdapter.schemaFor( RELATIONSHIP, new String[0], Optional.empty(), PROP );
        IndexReference nodesIndex;
        IndexReference relsIndex;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            nodesIndex = tx.schemaWrite().indexCreate( nodes, Optional.of( FulltextIndexProviderFactory.DESCRIPTOR.name() ),
                    Optional.of( NODE_INDEX_NAME ) );
            relsIndex = tx.schemaWrite().indexCreate( rels, Optional.of( FulltextIndexProviderFactory.DESCRIPTOR.name() ),
                    Optional.of( REL_INDEX_NAME ) );
            tx.success();
        }
        await( nodesIndex );
        await( relsIndex );
        try ( Transaction tx = db.beginTx() )
        {
            long firstNode = createNodeIndexableByPropertyValue( "Hello. Hello again." );
            long secondNode = createNodeWithProperty( "prop2",
                    "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                    "cross between a zebra and any other equine: essentially, a zebra hybrid." );
            createRelationshipIndexableByPropertyValue( firstNode, secondNode, "Hello. Hello again." );
            createRelationshipWithProperty( secondNode, firstNode, "prop2",
                    "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                    "cross between a zebra and any other equine: essentially, a zebra hybrid." );

            tx.success();
        }
        try ( Transaction ignore = db.beginTx() )
        {
            assertQueryFindsNothing( NODE_INDEX_NAME, "zebra" );
            assertQueryFindsNothing( REL_INDEX_NAME, "zebra" );
        }
    }

    @Test
    public void shouldPopulateIndexWithExistingNodesAndRelationships() throws Exception
    {
        long firstNodeID;
        long secondNodeID;
        long firstRelID;
        long secondRelID;
        try ( Transaction tx = db.beginTx() )
        {
            // skip a few rel ids, so the ones we work with are different from the node ids, just in case.
            Node node = db.createNode();
            node.createRelationshipTo( node, RELTYPE );
            node.createRelationshipTo( node, RELTYPE );
            node.createRelationshipTo( node, RELTYPE );

            firstNodeID = createNodeIndexableByPropertyValue( "Hello. Hello again." );
            secondNodeID = createNodeIndexableByPropertyValue( "This string is slightly shorter than the zebra one" );
            firstRelID = createRelationshipIndexableByPropertyValue( firstNodeID, secondNodeID, "Goodbye" );
            secondRelID = createRelationshipIndexableByPropertyValue( secondNodeID, firstNodeID, "And now, something completely different" );

            tx.success();
        }

        SchemaDescriptor nodes = fulltextAdapter.schemaFor( NODE, new String[0], Optional.empty(), PROP );
        SchemaDescriptor rels = fulltextAdapter.schemaFor( RELATIONSHIP, new String[0], Optional.empty(), PROP );
        IndexReference nodesIndex;
        IndexReference relsIndex;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            nodesIndex = tx.schemaWrite().indexCreate( nodes, Optional.of( FulltextIndexProviderFactory.DESCRIPTOR.name() ),
                    Optional.of( NODE_INDEX_NAME ) );
            relsIndex = tx.schemaWrite().indexCreate( rels, Optional.of( FulltextIndexProviderFactory.DESCRIPTOR.name() ),
                    Optional.of( REL_INDEX_NAME ) );
            tx.success();
        }
        await( nodesIndex );
        await( relsIndex );
        try ( Transaction ignore = db.beginTx() )
        {
            assertQueryFindsIds( NODE_INDEX_NAME, "hello", firstNodeID );
            assertQueryFindsIds( NODE_INDEX_NAME, "string", secondNodeID );
            assertQueryFindsNothing( NODE_INDEX_NAME, "goodbye" );
            assertQueryFindsNothing( NODE_INDEX_NAME, "different" );

            assertQueryFindsNothing( REL_INDEX_NAME, "hello" );
            assertQueryFindsNothing( REL_INDEX_NAME, "string" );
            assertQueryFindsIds( REL_INDEX_NAME, "goodbye", firstRelID );
            assertQueryFindsIds( REL_INDEX_NAME, "different", secondRelID );
        }
    }

    @Test
    public void shouldBeAbleToUpdateAndQueryAfterIndexChange() throws Exception
    {
        IndexReference index;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            SchemaDescriptor descriptor = fulltextAdapter.schemaFor( NODE, new String[0], Optional.empty(), PROP );
            index = tx.schemaWrite().indexCreate( descriptor, Optional.of( FulltextIndexProviderFactory.DESCRIPTOR.name() ),
                    Optional.of( NODE_INDEX_NAME ) );
            tx.success();
        }
        await( index );

        long firstID;
        long secondID;
        long thirdID;
        long fourthID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( "thing" );

            secondID = db.createNode().getId();
            setNodeProp( secondID, "prop2", "zebra" );

            thirdID = createNodeIndexableByPropertyValue( "zebra" );
            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            assertQueryFindsIds( NODE_INDEX_NAME, "thing zebra", firstID, thirdID );
        }

        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            SchemaDescriptor descriptor = fulltextAdapter.schemaFor( NODE, new String[0], Optional.empty(), "prop2" );
            tx.schemaWrite().indexDrop( index );
            index = tx.schemaWrite().indexCreate( descriptor, Optional.of( FulltextIndexProviderFactory.DESCRIPTOR.name() ),
                    Optional.of( NODE_INDEX_NAME ) );
            tx.success();
        }
        await( index );

        try ( Transaction tx = db.beginTx() )
        {
            setNodeProp( firstID, "prop2", "thing" );

            fourthID = db.createNode().getId();
            setNodeProp( fourthID, "prop2", "zebra" );
            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            assertQueryFindsIds( NODE_INDEX_NAME, "thing zebra", firstID, secondID, fourthID );
        }
    }

    @Test
    public void shouldBeAbleToDropAndReadIndex() throws Exception
    {
        SchemaDescriptor descriptor = fulltextAdapter.schemaFor( NODE, new String[0], Optional.empty(), PROP );
        IndexReference index;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            index = tx.schemaWrite().indexCreate( descriptor, Optional.of( FulltextIndexProviderFactory.DESCRIPTOR.name() ),
                    Optional.of( NODE_INDEX_NAME ) );
            tx.success();
        }
        await( index );

        long firstID;
        long secondID;

        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( "thing" );

            secondID = createNodeIndexableByPropertyValue( "zebra" );
            tx.success();
        }

        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            tx.schemaWrite().indexDrop( index );
            tx.success();
        }
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            index = tx.schemaWrite().indexCreate( descriptor, Optional.of( FulltextIndexProviderFactory.DESCRIPTOR.name() ),
                    Optional.of( NODE_INDEX_NAME ) );
            tx.success();
        }
        await( index );

        try ( Transaction ignore = db.beginTx() )
        {
            assertQueryFindsIds( NODE_INDEX_NAME, "thing zebra", firstID, secondID );
        }
    }
}
