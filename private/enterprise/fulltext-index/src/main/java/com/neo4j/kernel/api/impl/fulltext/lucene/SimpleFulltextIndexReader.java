/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TotalHitCountCollector;

import java.io.IOException;

import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector;
import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;
import org.neo4j.kernel.api.impl.schema.reader.IndexReaderCloseException;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.values.storable.Value;

/**
 * Lucene index reader that is able to read/sample a single partition of a partitioned Lucene index.
 *
 * @see PartitionedFulltextIndexReader
 */
class SimpleFulltextIndexReader extends FulltextIndexReader
{
    private final SearcherReference searcherRef;
    private final Analyzer analyzer;
    private final TokenHolder propertyKeyTokenHolder;
    private final String[] properties;

    SimpleFulltextIndexReader( SearcherReference searcherRef, String[] properties, Analyzer analyzer, TokenHolder propertyKeyTokenHolder )
    {
        this.searcherRef = searcherRef;
        this.properties = properties;
        this.analyzer = analyzer;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
    }

    @Override
    public void close()
    {
        try
        {
            searcherRef.close();
        }
        catch ( IOException e )
        {
            throw new IndexReaderCloseException( e );
        }
    }

    @Override
    public ScoreEntityIterator query( String queryString ) throws ParseException
    {
        MultiFieldQueryParser multiFieldQueryParser = new MultiFieldQueryParser( properties, analyzer );
        Query query = multiFieldQueryParser.parse( queryString );
        return indexQuery( query );
    }

    private ScoreEntityIterator indexQuery( Query query )
    {
        try
        {
            DocValuesCollector docValuesCollector = new DocValuesCollector( true );
            getIndexSearcher().search( query, docValuesCollector );
            ValuesIterator sortedValuesIterator =
                    docValuesCollector.getSortedValuesIterator( LuceneFulltextDocumentStructure.FIELD_ENTITY_ID, Sort.RELEVANCE );
            return new ScoreEntityIterator( sortedValuesIterator );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private IndexSearcher getIndexSearcher()
    {
        return searcherRef.getIndexSearcher();
    }

    @Override
    public long countIndexedNodes( long nodeId, int[] propertyKeyIds, Value... propertyValues )
    {
        try
        {
            String[] propertyKeys = new String[propertyKeyIds.length];
            for ( int i = 0; i < propertyKeyIds.length; i++ )
            {
                propertyKeys[i] = propertyKeyTokenHolder.getTokenById( propertyKeyIds[i] ).name();
            }
            Query query = LuceneFulltextDocumentStructure.newCountNodeEntriesQuery( nodeId, propertyKeys, propertyValues );
            TotalHitCountCollector collector = new TotalHitCountCollector();
            getIndexSearcher().search( query, collector );
            return collector.getTotalHits();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
}
