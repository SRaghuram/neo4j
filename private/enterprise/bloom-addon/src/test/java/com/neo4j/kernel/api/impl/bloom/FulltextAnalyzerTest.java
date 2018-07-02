/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.bloom;

import com.neo4j.kernel.api.impl.bloom.surface.BloomKernelExtensionFactory;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.junit.Test;

import org.neo4j.graphdb.Transaction;

import static java.util.Collections.singleton;

public class FulltextAnalyzerTest extends LuceneFulltextTestSupport
{
    private static final String ENGLISH = EnglishAnalyzer.class.getCanonicalName();
    private static final String SWEDISH = SwedishAnalyzer.class.getCanonicalName();

    @Test
    public void shouldBeAbleToSpecifyEnglishAnalyzer() throws Exception
    {
        analyzer = ENGLISH;
        try ( FulltextProvider provider = createProvider() )
        {
            provider.createIndex( BloomKernelExtensionFactory.BLOOM_NODES, FulltextIndexType.NODES, singleton( "prop" ) );

            long id;
            try ( Transaction tx = db.beginTx() )
            {
                createNodeIndexableByPropertyValue( "Hello and hello again, in the end." );
                id = createNodeIndexableByPropertyValue( "En apa och en tomte bodde i ett hus." );

                tx.success();
            }
            provider.awaitFlip();
            try ( ReadOnlyFulltext reader = provider.getReader( BloomKernelExtensionFactory.BLOOM_NODES, FulltextIndexType.NODES ) )
            {
                assertExactQueryFindsNothing( reader, "and" );
                assertExactQueryFindsNothing( reader, "in" );
                assertExactQueryFindsNothing( reader, "the" );
                assertExactQueryFindsIds( reader, "en", false, id );
                assertExactQueryFindsIds( reader, "och", false, id );
                assertExactQueryFindsIds( reader, "ett", false, id );
            }
        }
    }

    @Test
    public void shouldBeAbleToSpecifySwedishAnalyzer() throws Exception
    {
        analyzer = SWEDISH;
        try ( FulltextProvider provider = createProvider() )
        {
            provider.createIndex( BloomKernelExtensionFactory.BLOOM_NODES, FulltextIndexType.NODES, singleton( "prop" ) );

            long id;
            try ( Transaction tx = db.beginTx() )
            {
                id = createNodeIndexableByPropertyValue( "Hello and hello again, in the end." );
                createNodeIndexableByPropertyValue( "En apa och en tomte bodde i ett hus." );

                tx.success();
            }
            provider.awaitFlip();
            try ( ReadOnlyFulltext reader = provider.getReader( BloomKernelExtensionFactory.BLOOM_NODES, FulltextIndexType.NODES ) )
            {
                assertExactQueryFindsIds( reader, "and", false, id );
                assertExactQueryFindsIds( reader, "in", false, id );
                assertExactQueryFindsIds( reader, "the", false, id );
                assertExactQueryFindsNothing( reader, "en" );
                assertExactQueryFindsNothing( reader, "och" );
                assertExactQueryFindsNothing( reader, "ett" );
            }
        }
    }

    @Test
    public void shouldReindexNodesWhenAnalyzerIsChanged() throws Exception
    {
        long firstID;
        long secondID;
        analyzer = ENGLISH;
        try ( FulltextProvider provider = createProvider() )
        {
            provider.createIndex( BloomKernelExtensionFactory.BLOOM_NODES, FulltextIndexType.NODES, singleton( "prop" ) );

            try ( Transaction tx = db.beginTx() )
            {
                firstID = createNodeIndexableByPropertyValue( "Hello and hello again, in the end." );
                secondID = createNodeIndexableByPropertyValue( "En apa och en tomte bodde i ett hus." );

                tx.success();
            }
            provider.awaitFlip();
            try ( ReadOnlyFulltext reader = provider.getReader( BloomKernelExtensionFactory.BLOOM_NODES, FulltextIndexType.NODES ) )
            {

                assertExactQueryFindsNothing( reader, "and" );
                assertExactQueryFindsNothing( reader, "in" );
                assertExactQueryFindsNothing( reader, "the" );
                assertExactQueryFindsIds( reader, "en", false, secondID );
                assertExactQueryFindsIds( reader, "och", false, secondID );
                assertExactQueryFindsIds( reader, "ett", false, secondID );
            }
        }

        analyzer = SWEDISH;
        try ( FulltextProvider provider = createProvider() )
        {
            provider.createIndex( BloomKernelExtensionFactory.BLOOM_NODES, FulltextIndexType.NODES, singleton( "prop" ) );
            provider.awaitPopulation();
            provider.awaitFlip();
            try ( ReadOnlyFulltext reader = provider.getReader( BloomKernelExtensionFactory.BLOOM_NODES, FulltextIndexType.NODES ) )
            {
                assertExactQueryFindsIds( reader, "and",  false, firstID );
                assertExactQueryFindsIds( reader, "in",  false, firstID );
                assertExactQueryFindsIds( reader, "the",  false, firstID );
                assertExactQueryFindsNothing( reader, "en" );
                assertExactQueryFindsNothing( reader, "och" );
                assertExactQueryFindsNothing( reader, "ett" );
            }
        }
    }
}
