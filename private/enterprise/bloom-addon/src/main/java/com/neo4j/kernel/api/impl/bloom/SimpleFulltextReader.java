/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.bloom;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.schema.reader.IndexReaderCloseException;

import static java.util.stream.Collectors.joining;
import static com.neo4j.kernel.api.impl.bloom.FulltextProvider.FIELD_ENTITY_ID;

/**
 * Lucene index reader that is able to read/sample a single partition of a partitioned Lucene index.
 *
 * @see PartitionedFulltextReader
 */
class SimpleFulltextReader implements ReadOnlyFulltext
{
    private final PartitionSearcher partitionSearcher;
    private final Analyzer analyzer;
    private final String[] properties;

    SimpleFulltextReader( PartitionSearcher partitionSearcher, String[] properties, Analyzer analyzer )
    {
        this.partitionSearcher = partitionSearcher;
        this.properties = properties;
        this.analyzer = analyzer;
    }

    @Override
    public ScoreEntityIterator query( Collection<String> terms, boolean matchAll )
    {
        String query = terms.stream().map( QueryParser::escape ).collect( joining( " " ) );
        return innerQuery( query, matchAll );
    }

    @Override
    public ScoreEntityIterator fuzzyQuery( Collection<String> terms, boolean matchAll )
    {
        String query = terms.stream().map( QueryParser::escape ).collect( joining( "~ ", "", "~" ) );
        return innerQuery( query, matchAll );
    }

    @Override
    public void close()
    {
        try
        {
            partitionSearcher.close();
        }
        catch ( IOException e )
        {
            throw new IndexReaderCloseException( e );
        }
    }

    @Override
    public FulltextIndexConfiguration getConfigurationDocument() throws IOException
    {
        IndexSearcher indexSearcher = getIndexSearcher();
        TopDocs docs = indexSearcher.search( new TermQuery( FulltextIndexConfiguration.TERM ), 1 );
        if ( docs.scoreDocs.length < 1 )
        {
            System.err.println( "No config document found in the index." );
            return null;
        }
        return new FulltextIndexConfiguration( indexSearcher.doc( docs.scoreDocs[0].doc ) );
    }

    private ScoreEntityIterator innerQuery( String queryString, boolean matchAll )
    {
        MultiFieldQueryParser multiFieldQueryParser = new MultiFieldQueryParser( properties, analyzer );
        if ( matchAll )
        {
            multiFieldQueryParser.setDefaultOperator( QueryParser.Operator.AND );
        }
        else
        {
            multiFieldQueryParser.setDefaultOperator( QueryParser.Operator.OR );
        }
        Query query;
        try
        {
            query = multiFieldQueryParser.parse( queryString );
        }
        catch ( ParseException e )
        {
            assert false;
            return ScoreEntityIterator.emptyIterator();
        }
        return indexQuery( query );
    }

    private ScoreEntityIterator indexQuery( Query query )
    {
        try
        {
            DocValuesCollector docValuesCollector = new DocValuesCollector( true );
            getIndexSearcher().search( query, docValuesCollector );
            return new ScoreEntityIterator( docValuesCollector.getSortedValuesIterator( FIELD_ENTITY_ID, Sort.RELEVANCE ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private IndexSearcher getIndexSearcher()
    {
        return partitionSearcher.getIndexSearcher();
    }
}
