/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.bloom;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.Race;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

public class LuceneFulltextUpdaterTest extends LuceneFulltextTestSupport
{
    @Test
    public void shouldFindNodeWithString() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            provider.createIndex( "nodes", FulltextIndexType.NODES, singleton( "prop" ) );

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
            provider.awaitFlip();
            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", FulltextIndexType.NODES ) )
            {
                assertExactQueryFindsIds( reader, "hello", false, firstID );
                assertExactQueryFindsIds( reader, "zebra", false, secondID );
                assertExactQueryFindsIds( reader, "zedonk", false, secondID );
                assertExactQueryFindsIds( reader, "cross", false, secondID );
            }
        }
    }

    @Test
    public void shouldFindNodeWithNumber() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            provider.createIndex( "nodes", FulltextIndexType.NODES, singleton( "prop" ) );

            long firstID;
            long secondID;
            try ( Transaction tx = db.beginTx() )
            {
                firstID = createNodeIndexableByPropertyValue( 1 );
                secondID = createNodeIndexableByPropertyValue( 234 );

                tx.success();
            }
            provider.awaitFlip();
            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", FulltextIndexType.NODES ) )
            {
                assertExactQueryFindsIds( reader, "1", false, firstID );
                assertExactQueryFindsIds( reader, "234", false, secondID );
            }
        }
    }

    @Test
    public void shouldFindNodeWithBoolean() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            provider.createIndex( "nodes", FulltextIndexType.NODES, singleton( "prop" ) );

            long firstID;
            long secondID;
            try ( Transaction tx = db.beginTx() )
            {
                firstID = createNodeIndexableByPropertyValue( true );
                secondID = createNodeIndexableByPropertyValue( false );

                tx.success();
            }
            provider.awaitFlip();
            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", FulltextIndexType.NODES ) )
            {
                assertExactQueryFindsIds( reader, "true", false, firstID );
                assertExactQueryFindsIds( reader, "false", false, secondID );
            }
        }
    }

    @Test
    public void shouldFindNodeWithArrays() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            provider.createIndex( "nodes", FulltextIndexType.NODES, singleton( "prop" ) );

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
            provider.awaitFlip();

            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", FulltextIndexType.NODES ) )
            {
                assertExactQueryFindsIds( reader, "live", false, firstID );
                assertExactQueryFindsIds( reader, "27", false, secondID );
                assertExactQueryFindsIds( reader, Arrays.asList( "1", "2" ), false, secondID, thirdID );
            }
        }
    }

    @Test
    public void shouldRepresentPropertyChanges() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            provider.createIndex( "nodes", FulltextIndexType.NODES, singleton( "prop" ) );

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
                setNodeProp( firstID, "Hahahaha! potato!" );
                setNodeProp( secondID, "This one is a potato farmer." );

                tx.success();
            }
            provider.awaitFlip();

            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", FulltextIndexType.NODES ) )
            {
                assertExactQueryFindsNothing( reader, "hello" );
                assertExactQueryFindsNothing( reader, "zebra" );
                assertExactQueryFindsNothing( reader, "zedonk" );
                assertExactQueryFindsNothing( reader, "cross" );
                assertExactQueryFindsIds( reader, "hahahaha", false, firstID );
                assertExactQueryFindsIds( reader, "farmer", false, secondID );
                assertExactQueryFindsIds( reader, "potato", false, firstID, secondID );
            }
        }
    }

    @Test
    public void shouldNotFindRemovedNodes() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            provider.createIndex( "nodes", FulltextIndexType.NODES, singleton( "prop" ) );

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
            provider.awaitFlip();
            try ( Transaction tx = db.beginTx() )
            {
                db.getNodeById( firstID ).delete();
                db.getNodeById( secondID ).delete();

                tx.success();
            }
            // We await two flips because the first await might catch an on-going flip, which would race with the delete
            // transaction. Two flips ensures there's no race.
            provider.awaitFlip();
            provider.awaitFlip();
            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", FulltextIndexType.NODES ) )
            {
                assertExactQueryFindsNothing( reader, "hello" );
                assertExactQueryFindsNothing( reader, "zebra" );
                assertExactQueryFindsNothing( reader, "zedonk" );
                assertExactQueryFindsNothing( reader, "cross" );
            }
        }
    }

    @Test
    public void shouldNotFindRemovedProperties() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            provider.createIndex( "nodes", FulltextIndexType.NODES, asSet( "prop", "prop2" ) );

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
            provider.awaitFlip();
            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", FulltextIndexType.NODES ) )
            {
                assertExactQueryFindsIds( reader, "hello", false, secondID );
                assertExactQueryFindsNothing( reader, "zebra" );
                assertExactQueryFindsNothing( reader, "zedonk" );
                assertExactQueryFindsNothing( reader, "cross" );
            }
        }
    }

    private Set<String> asSet( String... prop )
    {
        Set<String> set = new HashSet<>();
        Collections.addAll( set, prop );
        return set;
    }

    @Test
    public void shouldOnlyIndexIndexedProperties() throws Exception
    {
        try ( FulltextProvider provider = createProvider(); )
        {
            provider.createIndex( "nodes", FulltextIndexType.NODES, singleton( "prop" ) );

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
            provider.awaitFlip();

            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", FulltextIndexType.NODES ) )
            {
                assertExactQueryFindsIds( reader, "hello", false, firstID );
                assertExactQueryFindsNothing( reader, "zebra" );
            }
        }
    }

    @Test
    public void shouldSearchAcrossMultipleProperties() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            provider.createIndex( "nodes", FulltextIndexType.NODES, asSet( "prop", "prop2" ) );

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
            provider.awaitFlip();

            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", FulltextIndexType.NODES ) )
            {
                assertExactQueryFindsIds( reader, Arrays.asList( "tomtar", "karl" ), false, firstID, secondID, thirdID );
            }
        }
    }

    @Test
    public void shouldOrderResultsBasedOnRelevance() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            provider.createIndex( "nodes", FulltextIndexType.NODES, asSet( "first", "last" ) );

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
            provider.awaitFlip();
            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", FulltextIndexType.NODES ) )
            {
                assertExactQueryFindsIdsInOrder( reader, Arrays.asList( "Tom", "Hanks" ), false, fourthID, thirdID, firstID, secondID );
            }
        }
    }

    @Test
    public void shouldDifferentiateNodesAndRelationships() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            provider.createIndex( "nodes", FulltextIndexType.NODES, singleton( "prop" ) );
            provider.createIndex( "relationships", FulltextIndexType.RELATIONSHIPS, singleton( "prop" ) );

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
                firstRelID = createRelationshipIndexableByPropertyValue(
                        firstNodeID, secondNodeID, "Hello. Hello again." );
                secondRelID = createRelationshipIndexableByPropertyValue(
                        secondNodeID, firstNodeID, "And now, something completely different" );

                tx.success();
            }
            provider.awaitFlip();
            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", FulltextIndexType.NODES ) )
            {
                assertExactQueryFindsIds( reader, "hello", false, firstNodeID );
                assertExactQueryFindsIds( reader, "zebra", false, secondNodeID );
                assertExactQueryFindsNothing( reader, "different" );
            }
            try ( ReadOnlyFulltext reader = provider.getReader( "relationships", FulltextIndexType.RELATIONSHIPS ) )
            {
                assertExactQueryFindsIds( reader, "hello", false, firstRelID );
                assertExactQueryFindsNothing( reader, "zebra" );
                assertExactQueryFindsIds( reader, "different", false, secondRelID );
            }
        }
    }

    @Test
    public void fuzzyQueryShouldBeFuzzy() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            provider.createIndex( "nodes", FulltextIndexType.NODES, singleton( "prop" ) );

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
            provider.awaitFlip();

            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", FulltextIndexType.NODES ) )
            {
                assertFuzzyQueryFindsIds( reader, "hella", false, firstID );
                assertFuzzyQueryFindsIds( reader, "zebre", false, secondID );
                assertFuzzyQueryFindsIds( reader, "zedink", false, secondID );
                assertFuzzyQueryFindsIds( reader, "cruss", false, secondID );
                assertExactQueryFindsNothing( reader, "hella" );
                assertExactQueryFindsNothing( reader, "zebre" );
                assertExactQueryFindsNothing( reader, "zedink" );
                assertExactQueryFindsNothing( reader, "cruss" );
            }
        }
    }

    @Test
    public void fuzzyQueryShouldReturnExactMatchesFirst() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            provider.createIndex( "nodes", FulltextIndexType.NODES, singleton( "prop" ) );

            long firstID;
            long secondID;
            long thirdID;
            long fourthID;
            try ( Transaction tx = db.beginTx() )
            {
                firstID = createNodeIndexableByPropertyValue( "zibre" );
                secondID = createNodeIndexableByPropertyValue( "zebrae" );
                thirdID = createNodeIndexableByPropertyValue( "zebra" );
                fourthID = createNodeIndexableByPropertyValue( "zibra" );

                tx.success();
            }
            provider.awaitFlip();
            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", FulltextIndexType.NODES ) )
            {
                assertFuzzyQueryFindsIdsInOrder( reader, "zebra", true, thirdID, secondID, fourthID, firstID );
            }
        }
    }

    @Test
    public void shouldNotReturnNonMatches() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            provider.createIndex( "nodes", FulltextIndexType.NODES, singleton( "prop" ) );
            provider.createIndex( "relationships", FulltextIndexType.RELATIONSHIPS, singleton( "prop" ) );

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
            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", FulltextIndexType.NODES ) )
            {
                assertExactQueryFindsNothing( reader, "zebra" );
            }
            try ( ReadOnlyFulltext reader = provider.getReader( "relationships", FulltextIndexType.RELATIONSHIPS ) )
            {
                assertExactQueryFindsNothing( reader, "zebra" );
            }
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
            secondRelID = createRelationshipIndexableByPropertyValue( secondNodeID, firstNodeID,
                    "And now, something completely different" );

            tx.success();
        }

        try ( FulltextProvider provider = createProvider() )
        {
            provider.createIndex( "nodes", FulltextIndexType.NODES, singleton( "prop" ) );
            provider.createIndex( "relationships", FulltextIndexType.RELATIONSHIPS, singleton( "prop" ) );
            provider.awaitPopulation();

            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", FulltextIndexType.NODES ) )
            {
                assertExactQueryFindsIds( reader, "hello", false, firstNodeID );
                assertExactQueryFindsIds( reader, "string", false, secondNodeID );
                assertExactQueryFindsNothing( reader, "goodbye" );
                assertExactQueryFindsNothing( reader, "different" );
            }
            try ( ReadOnlyFulltext reader = provider.getReader( "relationships", FulltextIndexType.RELATIONSHIPS ) )
            {
                assertExactQueryFindsNothing( reader, "hello" );
                assertExactQueryFindsNothing( reader, "string" );
                assertExactQueryFindsIds( reader, "goodbye", false, firstRelID );
                assertExactQueryFindsIds( reader, "different", false, secondRelID );
            }
        }
    }

    @Test
    public void shouldReturnMatchesThatContainLuceneSyntaxCharacters() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            provider.createIndex( "nodes", FulltextIndexType.NODES, singleton( "prop" ) );
            String[] luceneSyntaxElements =
                    {"+", "-", "&&", "||", "!", "(", ")", "{", "}", "[", "]", "^", "\"", "~", "*", "?", ":", "\\"};

            long nodeId;
            try ( Transaction tx = db.beginTx() )
            {
                nodeId = db.createNodeId();
                tx.success();
            }

            for ( String elm : luceneSyntaxElements )
            {
                setNodeProp( nodeId, "Hello" + elm + " How are you " + elm + "today?" );
                provider.awaitFlip();
                try ( ReadOnlyFulltext reader = provider.getReader( "nodes", FulltextIndexType.NODES ) )
                {
                    assertExactQueryFindsIds( reader, "Hello" + elm, false, nodeId );
                    assertExactQueryFindsIds( reader, elm + "today", false, nodeId );
                }
            }
        }
    }

    @Test
    public void exactMatchAllShouldOnlyReturnStuffThatMatchesAll() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            provider.createIndex( "nodes", FulltextIndexType.NODES, asSet( "first", "last" ) );

            long firstID;
            long secondID;
            long thirdID;
            long fourthID;
            long fifthID;
            try ( Transaction tx = db.beginTx() )
            {
                firstID = db.createNode().getId();
                secondID = db.createNode().getId();
                thirdID = db.createNode().getId();
                fourthID = db.createNode().getId();
                fifthID = db.createNode().getId();
                setNodeProp( firstID, "first", "Full" );
                setNodeProp( firstID, "last", "Hanks" );
                setNodeProp( secondID, "first", "Tom" );
                setNodeProp( secondID, "last", "Hunk" );
                setNodeProp( thirdID, "first", "Tom" );
                setNodeProp( thirdID, "last", "Hanks" );
                setNodeProp( fourthID, "first", "Tom Hanks" );
                setNodeProp( fourthID, "last", "Tom Hanks" );
                setNodeProp( fifthID, "last", "Tom Hanks" );
                setNodeProp( fifthID, "first", "Morgan" );

                tx.success();
            }
            provider.awaitFlip();
            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", FulltextIndexType.NODES ) )
            {
                assertExactQueryFindsIds( reader, Arrays.asList( "Tom", "Hanks" ), true, thirdID, fourthID, fifthID );
            }
        }
    }

    @Test
    public void fuzzyMatchAllShouldOnlyReturnStuffThatKindaMatchesAll() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            provider.createIndex( "nodes", FulltextIndexType.NODES, asSet( "first", "last" ) );

            long firstID;
            long secondID;
            long thirdID;
            long fourthID;
            long fifthID;
            try ( Transaction tx = db.beginTx() )
            {
                firstID = db.createNode().getId();
                secondID = db.createNode().getId();
                thirdID = db.createNode().getId();
                fourthID = db.createNode().getId();
                fifthID = db.createNode().getId();
                setNodeProp( firstID, "first", "Christian" );
                setNodeProp( firstID, "last", "Hanks" );
                setNodeProp( secondID, "first", "Tom" );
                setNodeProp( secondID, "last", "Hungarian" );
                setNodeProp( thirdID, "first", "Tom" );
                setNodeProp( thirdID, "last", "Hunk" );
                setNodeProp( fourthID, "first", "Tim" );
                setNodeProp( fourthID, "last", "Hanks" );
                setNodeProp( fifthID, "last", "Tom Hanks" );
                setNodeProp( fifthID, "first", "Morgan" );

                tx.success();
            }
            provider.awaitFlip();
            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", FulltextIndexType.NODES ) )
            {
                assertFuzzyQueryFindsIds( reader, Arrays.asList( "Tom", "Hanks" ), true, thirdID, fourthID, fifthID );
            }
        }
    }

    @Test
    public void shouldBeAbleToUpdateAndQueryAfterIndexChange() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            provider.createIndex( "nodes", FulltextIndexType.NODES, singleton( "prop" ) );

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
            provider.awaitFlip();
            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", FulltextIndexType.NODES ) )
            {
                assertExactQueryFindsIds( reader, Arrays.asList( "thing", "zebra" ), false, firstID, thirdID );
            }

            provider.changeIndexedProperties( "nodes", FulltextIndexType.NODES, singleton( "prop2" ) );
            provider.awaitPopulation();
            try ( Transaction tx = db.beginTx() )
            {
                setNodeProp( firstID, "prop2", "thing" );

                fourthID = db.createNode().getId();
                setNodeProp( fourthID, "prop2", "zebra" );
                tx.success();
            }

            provider.awaitFlip();
            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", FulltextIndexType.NODES ) )
            {
                assertExactQueryFindsIds( reader, Arrays.asList( "thing", "zebra" ), false, firstID, secondID, fourthID );
            }
        }
    }

    @Test
    public void shouldBeAbleToDropAndReaddIndex() throws Exception
    {
        try ( FulltextProviderImpl provider = createProvider() )
        {
            provider.createIndex( "nodes", FulltextIndexType.NODES, singleton( "prop" ) );

            long firstID;
            long secondID;

            try ( Transaction tx = db.beginTx() )
            {
                firstID = createNodeIndexableByPropertyValue( "thing" );

                secondID = createNodeIndexableByPropertyValue( "zebra" );
                tx.success();
            }

            provider.drop( "nodes", FulltextIndexType.NODES );

            provider.createIndex( "nodes", FulltextIndexType.NODES, singleton( "prop" ) );
            provider.awaitPopulation();

            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", FulltextIndexType.NODES ) )
            {
                assertExactQueryFindsIds( reader, Arrays.asList( "thing", "zebra" ), false, firstID, secondID );
            }
        }
    }

    @Test
    public void concurrentUpdatesAndIndexChangesShouldResultInValidState() throws Throwable
    {
        try ( FulltextProvider provider = createProvider() )
        {
            provider.createIndex( "nodes", FulltextIndexType.NODES, singleton( "prop" ) );

            int aliceThreads = 10;
            int bobthreads = 10;
            int nodesCreatedPerThread = 10;
            Race race = new Race();
            Runnable aliceWork = () ->
            {
                for ( int i = 0; i < nodesCreatedPerThread; i++ )
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        createNodeIndexableByPropertyValue( "alice" );
                        tx.success();
                    }
                }
            };
            Runnable changeConfig = () ->
            {
                try
                {
                    provider.changeIndexedProperties( "nodes", FulltextIndexType.NODES, singleton( "otherProp" ) );
                    provider.awaitPopulation();
                }
                catch ( Exception e )
                {
                    throw new AssertionError( e );
                }
            };
            Runnable bobWork = () ->
            {
                for ( int i = 0; i < nodesCreatedPerThread; i++ )
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        createNodeWithProperty( "otherProp", "bob" );
                        tx.success();
                    }
                }
            };
            race.addContestants( aliceThreads, aliceWork );
            race.addContestant( changeConfig );
            race.addContestants( bobthreads, bobWork );
            race.go();
            provider.awaitFlip();
            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", FulltextIndexType.NODES ) )
            {
                ScoreEntityIterator bob = reader.query( singleton( "bob" ), true );
                assertEquals( bobthreads * nodesCreatedPerThread, Iterators.count( bob ) );
                ScoreEntityIterator alice = reader.query( singleton( "alice" ), true );
                assertEquals( 0, Iterators.count( alice ) );
            }
        }
    }
}
